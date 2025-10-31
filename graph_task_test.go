package weave

import (
	"context"
	"errors"
	"testing"
)

func TestAddGraphTask_ComposesSubgraph(t *testing.T) {
	sub := NewGraph()
	valueTask, err := AddTask(sub, "produce-values", func(ctx context.Context, deps DependencyResolver) ([]int, error) {
		return []int{1, 2, 3}, nil
	})
	if err != nil {
		t.Fatalf("add sub task: %v", err)
	}

	parent := NewGraph()
	graphHandle, err := AddGraphTask(parent, "subgraph", sub, func(ctx context.Context, results *Results, metrics ExecutionMetrics, runErr error) ([]int, error) {
		if runErr != nil {
			return nil, runErr
		}
		return valueTask.Value(results)
	})
	if err != nil {
		t.Fatalf("add graph task: %v", err)
	}

	sumTask, err := AddTask(parent, "sum", func(ctx context.Context, deps DependencyResolver) (int, error) {
		values, err := graphHandle.Value(deps)
		if err != nil {
			return 0, err
		}
		total := 0
		for _, v := range values {
			total += v
		}
		return total, nil
	}, DependsOn(graphHandle))
	if err != nil {
		t.Fatalf("add sum task: %v", err)
	}

	results, _, err := parent.Run(context.Background())
	if err != nil {
		t.Fatalf("parent run error: %v", err)
	}

	total, err := sumTask.Value(results)
	if err != nil {
		t.Fatalf("sum value error: %v", err)
	}
	if total != 6 {
		t.Fatalf("expected sum 6, got %d", total)
	}
}

func TestAddGraphTask_PropagatesOptionsAndHandlesErrors(t *testing.T) {
	sub := NewGraph()
	_, err := AddTask(sub, "will-fail", func(ctx context.Context, deps DependencyResolver) (string, error) {
		return "", errors.New("boom")
	})
	if err != nil {
		t.Fatalf("add fail task: %v", err)
	}

	parent := NewGraph()

	rootTask, err := AddTask(parent, "prepare", func(ctx context.Context, deps DependencyResolver) (string, error) {
		return "ready", nil
	})
	if err != nil {
		t.Fatalf("add prepare task: %v", err)
	}

	graphHandle, err := AddGraphTask(parent, "subgraph", sub, func(ctx context.Context, results *Results, metrics ExecutionMetrics, runErr error) (string, error) {
		if runErr != nil {
			return "fallback", nil
		}
		t.Errorf("expected runErr to be non-nil")
		return "", nil
	},
		WithGraphTaskOptions(DependsOn(rootTask)),
		WithGraphTaskExecutorOptions(WithErrorStrategy(ContinueOnError)),
	)
	if err != nil {
		t.Fatalf("add graph task: %v", err)
	}

	finalTask, err := AddTask(parent, "final", func(ctx context.Context, deps DependencyResolver) (string, error) {
		value, err := graphHandle.Value(deps)
		if err != nil {
			return "", err
		}
		return value, nil
	}, DependsOn(graphHandle))
	if err != nil {
		t.Fatalf("add final task: %v", err)
	}

	results, _, err := parent.Run(context.Background())
	if err != nil {
		t.Fatalf("parent run error: %v", err)
	}

	output, err := finalTask.Value(results)
	if err != nil {
		t.Fatalf("final value error: %v", err)
	}
	if output != "fallback" {
		t.Fatalf("expected fallback, got %q", output)
	}

	if status := results.Status(rootTask); status != StatusSucceeded {
		t.Fatalf("prepare task should run first and succeed, got %s", status)
	}
}

func TestAddGraphTask_ErrorsOnInvalidInput(t *testing.T) {
	parent := NewGraph()
	_, err := AddGraphTask[string](parent, "invalid", nil, func(ctx context.Context, r *Results, m ExecutionMetrics, runErr error) (string, error) {
		return "", nil
	})
	if !errors.Is(err, ErrNilSubgraph) {
		t.Fatalf("expected ErrNilSubgraph, got %v", err)
	}

	sub := NewGraph()
	_, err = AddGraphTask[string](parent, "missing-result-func", sub, nil)
	if !errors.Is(err, ErrNilGraphTaskResult) {
		t.Fatalf("expected ErrNilGraphTaskResult, got %v", err)
	}

	_, err = AddGraphTask[string](parent, "self", parent, func(ctx context.Context, r *Results, m ExecutionMetrics, runErr error) (string, error) {
		return "", nil
	})
	if !errors.Is(err, ErrRecursiveGraphTask) {
		t.Fatalf("expected ErrRecursiveGraphTask, got %v", err)
	}
}
