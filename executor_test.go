package weave

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestRunSimpleGraph(t *testing.T) {
	g := NewGraph()

	a, err := AddTask(g, "a", func(ctx context.Context, deps DependencyResolver) (int, error) {
		return 1, nil
	})
	if err != nil {
		t.Fatalf("add task a: %v", err)
	}

	b, err := AddTask(g, "b", func(ctx context.Context, deps DependencyResolver) (int, error) {
		av, err := a.Value(deps)
		if err != nil {
			return 0, err
		}
		return av + 2, nil
	}, DependsOn(a))
	if err != nil {
		t.Fatalf("add task b: %v", err)
	}

	c, err := AddTask(g, "c", func(ctx context.Context, deps DependencyResolver) (int, error) {
		av, err := a.Value(deps)
		if err != nil {
			return 0, err
		}
		bv, err := b.Value(deps)
		if err != nil {
			return 0, err
		}
		return av + bv, nil
	}, DependsOn(a, b))
	if err != nil {
		t.Fatalf("add task c: %v", err)
	}

	results, metrics, err := g.Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	cv, err := c.Value(results)
	if err != nil {
		t.Fatalf("result c: %v", err)
	}
	if cv != 4 {
		t.Fatalf("expected c result 4, got %d", cv)
	}

	if metrics.TasksTotal != 3 || metrics.TasksSucceeded != 3 || metrics.TasksFailed != 0 || metrics.TasksSkipped != 0 {
		t.Fatalf("unexpected metrics: %+v", metrics)
	}

	if metrics.MaxConcurrency < 1 {
		t.Fatalf("expected max concurrency >= 1, got %d", metrics.MaxConcurrency)
	}
}

func TestCycleDetection(t *testing.T) {
	g := NewGraph()

	run := func(ctx context.Context, deps DependencyResolver) (int, error) {
		return 0, nil
	}

	// Construct tasks with a circular dependency using internal helpers.
	if _, err := g.addNode("a", wrapTaskFunc(run), taskConfig{
		dependencies: []taskKey{{graphID: g.id, id: "b"}},
	}); err != nil {
		t.Fatalf("add node a: %v", err)
	}
	if _, err := g.addNode("b", wrapTaskFunc(run), taskConfig{
		dependencies: []taskKey{{graphID: g.id, id: "a"}},
	}); err != nil {
		t.Fatalf("add node b: %v", err)
	}

	if err := g.Validate(); !errors.Is(err, ErrCycleDetected) {
		t.Fatalf("expected cycle detected error, got %v", err)
	}

	if _, _, err := g.Run(context.Background()); !errors.Is(err, ErrCycleDetected) {
		t.Fatalf("expected run to fail with cycle error, got %v", err)
	}
}

func TestFailFastStrategy(t *testing.T) {
	g := NewGraph()
	failErr := errors.New("boom")
	trigger := make(chan struct{})

	fail, err := AddTask(g, "fail", func(ctx context.Context, deps DependencyResolver) (int, error) {
		close(trigger)
		return 0, failErr
	})
	if err != nil {
		t.Fatalf("add fail: %v", err)
	}

	dependent, err := AddTask(g, "dependent", func(ctx context.Context, deps DependencyResolver) (int, error) {
		<-trigger
		val, err := fail.Value(deps)
		return val, err
	}, DependsOn(fail))
	if err != nil {
		t.Fatalf("add dependent: %v", err)
	}

	independent, err := AddTask(g, "independent", func(ctx context.Context, deps DependencyResolver) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(50 * time.Millisecond):
			return 42, nil
		}
	})
	if err != nil {
		t.Fatalf("add independent: %v", err)
	}

	results, metrics, err := g.Run(context.Background(), WithErrorStrategy(FailFast))
	if !errors.Is(err, failErr) {
		t.Fatalf("expected fail-fast error %v, got %v", failErr, err)
	}

	if status := results.Status(fail); status != StatusFailed {
		t.Fatalf("expected fail status failed, got %s", status)
	}

	if status := results.Status(dependent); status != StatusSkipped {
		t.Fatalf("expected dependent skipped, got %s", status)
	}

	if status := results.Status(independent); status != StatusFailed {
		t.Fatalf("expected independent failure due to cancellation, got %s", status)
	}

	if metrics.TasksFailed < 1 {
		t.Fatalf("expected at least one failed task, metrics=%+v", metrics)
	}
	if metrics.TasksSkipped != 1 {
		t.Fatalf("expected one skipped task, metrics=%+v", metrics)
	}

	if depErr := results.Error(dependent); depErr == nil || !errors.Is(depErr, ErrDependencyFailed) {
		t.Fatalf("expected dependency failure error, got %v", depErr)
	}
}

func TestContinueOnErrorStrategy(t *testing.T) {
	g := NewGraph()
	failErr := errors.New("oops")

	fail, err := AddTask(g, "fail", func(ctx context.Context, deps DependencyResolver) (int, error) {
		return 0, failErr
	})
	if err != nil {
		t.Fatalf("add fail: %v", err)
	}

	_, err = AddTask(g, "skipped", func(ctx context.Context, deps DependencyResolver) (int, error) {
		return 0, nil
	}, DependsOn(fail))
	if err != nil {
		t.Fatalf("add skipped: %v", err)
	}

	solo, err := AddTask(g, "solo", func(ctx context.Context, deps DependencyResolver) (int, error) {
		return 99, nil
	})
	if err != nil {
		t.Fatalf("add solo: %v", err)
	}

	results, metrics, err := g.Run(context.Background(), WithErrorStrategy(ContinueOnError))
	if !errors.Is(err, failErr) {
		t.Fatalf("expected continue to report failure %v, got %v", failErr, err)
	}

	if status := results.Status(solo); status != StatusSucceeded {
		t.Fatalf("expected solo succeeded, got %s", status)
	}
	if status := results.Status(fail); status != StatusFailed {
		t.Fatalf("expected fail task failed, got %s", status)
	}

	if metrics.TasksSucceeded != 1 || metrics.TasksFailed != 1 || metrics.TasksSkipped != 1 {
		t.Fatalf("unexpected metrics: %+v", metrics)
	}

	value, err := solo.Value(results)
	if err != nil {
		t.Fatalf("solo result: %v", err)
	}
	if value != 99 {
		t.Fatalf("expected solo value 99, got %d", value)
	}
}

func TestHooksInvocation(t *testing.T) {
	g := NewGraph()
	var mu sync.Mutex
	events := make([]string, 0, 8)

	record := func(label string) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, label)
	}

	successTask, err := AddTask(g, "success", func(ctx context.Context, deps DependencyResolver) (int, error) {
		return 7, nil
	})
	if err != nil {
		t.Fatalf("add success: %v", err)
	}

	_, err = AddTask(g, "failure", func(ctx context.Context, deps DependencyResolver) (int, error) {
		return 0, errors.New("fail")
	}, DependsOn(successTask), WithHooks(Hooks{
		OnFailure: func(ctx context.Context, event TaskEvent) {
			record(fmt.Sprintf("failure:onFailure:%s:%s", event.Metadata.Name, event.Metrics.Status))
		},
	}))
	if err != nil {
		t.Fatalf("add failure: %v", err)
	}

	results, _, _ := g.Run(context.Background(), WithGlobalHooks(Hooks{
		OnStart: func(ctx context.Context, event TaskEvent) {
			record(fmt.Sprintf("%s:onStart", event.Metadata.Name))
		},
		OnSuccess: func(ctx context.Context, event TaskEvent) {
			record(fmt.Sprintf("%s:onSuccess:%s", event.Metadata.Name, event.Metrics.Status))
		},
		OnFinish: func(ctx context.Context, event TaskEvent) {
			record(fmt.Sprintf("%s:onFinish:%s", event.Metadata.Name, event.Metrics.Status))
		},
	}))

	if status := results.Status(successTask); status != StatusSucceeded {
		t.Fatalf("expected success task succeeded, got %s", status)
	}

	expected := []string{
		"success:onStart",
		"success:onSuccess:succeeded",
		"success:onFinish:succeeded",
		"failure:onStart",
		"failure:onFailure:failure:failed",
		"failure:onFinish:failed",
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) != len(expected) {
		t.Fatalf("expected %d events, got %d (%v)", len(expected), len(events), events)
	}
	for i, want := range expected {
		if events[i] != want {
			t.Fatalf("event %d: expected %s, got %s (all %v)", i, want, events[i], events)
		}
	}
}

func TestMaxConcurrencyTracking(t *testing.T) {
	g := NewGraph()

	a, err := AddTask(g, "a", func(ctx context.Context, deps DependencyResolver) (int, error) {
		time.Sleep(50 * time.Millisecond)
		return 1, nil
	})
	if err != nil {
		t.Fatalf("add a: %v", err)
	}

	b, err := AddTask(g, "b", func(ctx context.Context, deps DependencyResolver) (int, error) {
		time.Sleep(50 * time.Millisecond)
		return 2, nil
	})
	if err != nil {
		t.Fatalf("add b: %v", err)
	}

	_, err = AddTask(g, "c", func(ctx context.Context, deps DependencyResolver) (int, error) {
		av, err := a.Value(deps)
		if err != nil {
			return 0, err
		}
		bv, err := b.Value(deps)
		if err != nil {
			return 0, err
		}
		return av + bv, nil
	}, DependsOn(a, b))
	if err != nil {
		t.Fatalf("add c: %v", err)
	}

	_, metrics, err := g.Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	if metrics.MaxConcurrency < 2 {
		t.Fatalf("expected max concurrency >= 2, got %d", metrics.MaxConcurrency)
	}
}

func TestRunCancellation(t *testing.T) {
	g := NewGraph()
	started := make(chan struct{})
	var once sync.Once

	slow, err := AddTask(g, "slow", func(ctx context.Context, deps DependencyResolver) (int, error) {
		once.Do(func() { close(started) })
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(200 * time.Millisecond):
			return 1, nil
		}
	})
	if err != nil {
		t.Fatalf("add slow: %v", err)
	}

	dependent, err := AddTask(g, "dependent", func(ctx context.Context, deps DependencyResolver) (int, error) {
		return slow.Value(deps)
	}, DependsOn(slow))
	if err != nil {
		t.Fatalf("add dependent: %v", err)
	}

	exec := g.Start(context.Background())
	<-started
	exec.Cancel()

	results, metrics, err := exec.Await()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation error, got %v", err)
	}

	if status := results.Status(slow); status != StatusFailed {
		t.Fatalf("expected slow task to fail due to cancellation, got %s", status)
	}
	if status := results.Status(dependent); status != StatusSkipped {
		t.Fatalf("expected dependent skipped, got %s", status)
	}
	if metrics.TasksSkipped != 1 {
		t.Fatalf("expected one skipped task, metrics=%+v", metrics)
	}
}

func TestTaskPanicIsCaptured(t *testing.T) {
	g := NewGraph()
	panicTask, err := AddTask(g, "panic", func(ctx context.Context, deps DependencyResolver) (int, error) {
		panic("kaboom")
	})
	if err != nil {
		t.Fatalf("add panic: %v", err)
	}

	results, metrics, err := g.Run(context.Background())
	if err == nil {
		t.Fatal("expected panic to propagate as error")
	}

	var panicErr TaskPanicError
	if !errors.As(err, &panicErr) {
		t.Fatalf("expected TaskPanicError, got %T (%v)", err, err)
	}
	if panicErr.TaskID != "panic" {
		t.Fatalf("expected panic task id, got %s", panicErr.TaskID)
	}

	if status := results.Status(panicTask); status != StatusFailed {
		t.Fatalf("expected panic task failed, got %s", status)
	}
	taskErr := results.Error(panicTask)
	if !errors.As(taskErr, &panicErr) {
		t.Fatalf("expected task error to be TaskPanicError, got %T (%v)", taskErr, taskErr)
	}

	if metrics.TasksFailed != 1 {
		t.Fatalf("expected one failed task, metrics=%+v", metrics)
	}
}

func TestWorkerPoolDispatcherLimitsConcurrency(t *testing.T) {
	g := NewGraph()
	startGate := make(chan struct{})
	releaseGate := make(chan struct{})
	var mu sync.Mutex
	current := 0
	maxObserved := 0

	taskHooks := Hooks{
		OnStart: func(ctx context.Context, event TaskEvent) {
			mu.Lock()
			current++
			if current > maxObserved {
				maxObserved = current
			}
			mu.Unlock()
		},
		OnFinish: func(ctx context.Context, event TaskEvent) {
			mu.Lock()
			current--
			mu.Unlock()
		},
	}

	handles := make([]*Handle[int], 0, 5)
	for i := 0; i < 5; i++ {
		value := i
		name := fmt.Sprintf("task-%d", i)
		handle, err := AddTask(g, name, func(ctx context.Context, deps DependencyResolver) (int, error) {
			<-startGate
			<-releaseGate
			return value, nil
		}, WithHooks(taskHooks))
		if err != nil {
			t.Fatalf("add %s: %v", name, err)
		}
		handles = append(handles, handle)
	}

	exec := g.Start(context.Background(), WithDispatcher(NewWorkerPoolDispatcher(2)))

	time.Sleep(20 * time.Millisecond)
	close(startGate)
	time.Sleep(20 * time.Millisecond)
	close(releaseGate)

	results, metrics, err := exec.Await()
	if err != nil {
		t.Fatalf("await: %v", err)
	}
	for _, handle := range handles {
		if status := results.Status(handle); status != StatusSucceeded {
			t.Fatalf("expected %s success, got %s", handle.ID(), status)
		}
	}

	mu.Lock()
	observed := maxObserved
	mu.Unlock()

	if observed > 2 {
		t.Fatalf("expected observed concurrency <= 2, got %d", observed)
	}
	if metrics.MaxConcurrency > 2 {
		t.Fatalf("expected metrics concurrency <= 2, got %d", metrics.MaxConcurrency)
	}
	if metrics.TasksSucceeded != len(handles) {
		t.Fatalf("expected %d succeeded tasks, metrics=%+v", len(handles), metrics)
	}
}

func TestAddTaskValidationErrors(t *testing.T) {
	g := NewGraph()

	if _, err := AddTask[int](g, "invalid", nil); !errors.Is(err, ErrNilRun) {
		t.Fatalf("expected ErrNilRun, got %v", err)
	}

	if _, err := AddTask[int](g, "", func(ctx context.Context, deps DependencyResolver) (int, error) {
		return 0, nil
	}); !errors.Is(err, ErrEmptyTaskName) {
		t.Fatalf("expected ErrEmptyTaskName, got %v", err)
	}

	if _, err := AddTask(g, "once", func(ctx context.Context, deps DependencyResolver) (int, error) {
		return 1, nil
	}); err != nil {
		t.Fatalf("add once: %v", err)
	}

	if _, err := AddTask(g, "once", func(ctx context.Context, deps DependencyResolver) (int, error) {
		return 2, nil
	}); !errors.Is(err, ErrTaskExists) {
		t.Fatalf("expected ErrTaskExists, got %v", err)
	}
}

func TestMissingDependencyValidation(t *testing.T) {
	g := NewGraph()
	run := func(ctx context.Context, deps DependencyResolver) (int, error) { return 0, nil }

	if _, err := g.addNode("a", wrapTaskFunc(run), taskConfig{
		dependencies: []taskKey{{graphID: g.id, id: "ghost"}},
	}); err != nil {
		t.Fatalf("add node: %v", err)
	}

	if err := g.Validate(); !errors.Is(err, ErrMissingDependency) {
		t.Fatalf("expected missing dependency error, got %v", err)
	}

	if _, _, err := g.Run(context.Background()); !errors.Is(err, ErrMissingDependency) {
		t.Fatalf("expected run error for missing dependency, got %v", err)
	}
}

func TestDependencyResolverGuards(t *testing.T) {
	g := NewGraph()

	required, err := AddTask(g, "required", func(ctx context.Context, deps DependencyResolver) (int, error) {
		return 123, nil
	})
	if err != nil {
		t.Fatalf("add required: %v", err)
	}

	unauthorized, err := AddTask(g, "unauthorized", func(ctx context.Context, deps DependencyResolver) (int, error) {
		_, err := required.Value(deps)
		if err == nil {
			return 0, errors.New("expected dependency error")
		}
		return 0, err
	})
	if err != nil {
		t.Fatalf("add unauthorized: %v", err)
	}

	results, _, err := g.Run(context.Background(), WithErrorStrategy(ContinueOnError))
	if !errors.Is(err, ErrNotDependency) {
		t.Fatalf("expected ErrNotDependency, got %v", err)
	}

	if status := results.Status(required); status != StatusSucceeded {
		t.Fatalf("expected required succeeded, got %s", status)
	}
	if status := results.Status(unauthorized); status != StatusFailed {
		t.Fatalf("expected unauthorized failed, got %s", status)
	}

	taskErr := results.Error(unauthorized)
	if !errors.Is(taskErr, ErrNotDependency) {
		t.Fatalf("expected ErrNotDependency, got %v", taskErr)
	}
}
