package weave

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// now is overridden in tests to provide deterministic timings.
var now = time.Now

// ErrorStrategy controls how the executor handles task failures.
type ErrorStrategy int

const (
	// FailFast cancels execution after the first failure.
	FailFast ErrorStrategy = iota
	// ContinueOnError continues executing independent tasks after failures.
	ContinueOnError
)

// TaskPanicError wraps a panic recovered from a task execution.
type TaskPanicError struct {
	TaskID string
	Value  any
}

func (e TaskPanicError) Error() string {
	return fmt.Sprintf("weave: panic in task %s: %v", e.TaskID, e.Value)
}

// Dispatcher submits work for execution and is responsible for running submitted functions.
type Dispatcher interface {
	Submit(func())
	Stop()
}

// goroutineDispatcher runs every submitted task on its own goroutine.
type goroutineDispatcher struct{}

func (goroutineDispatcher) Submit(fn func()) {
	go fn()
}

func (goroutineDispatcher) Stop() {}

// ExecutorOption configures task execution.
type ExecutorOption func(*executorOptions)

type executorOptions struct {
	strategy   ErrorStrategy
	hooks      Hooks
	dispatcher Dispatcher
}

func defaultExecutorOptions() executorOptions {
	return executorOptions{
		strategy:   FailFast,
		dispatcher: goroutineDispatcher{},
	}
}

// WithErrorStrategy configures how the executor reacts to task failures.
func WithErrorStrategy(strategy ErrorStrategy) ExecutorOption {
	return func(opts *executorOptions) {
		opts.strategy = strategy
	}
}

// WithGlobalHooks registers hooks applied to every task in the graph.
func WithGlobalHooks(h Hooks) ExecutorOption {
	return func(opts *executorOptions) {
		opts.hooks = opts.hooks.Merge(h)
	}
}

// WithDispatcher supplies a custom dispatcher implementation.
func WithDispatcher(dispatcher Dispatcher) ExecutorOption {
	return func(opts *executorOptions) {
		if dispatcher != nil {
			opts.dispatcher = dispatcher
		}
	}
}

// Execution encapsulates an in-flight or completed graph execution.
type Execution struct {
	results *Results
	metrics *ExecutionMetrics

	cancel context.CancelFunc
	done   chan struct{}

	mu  sync.Mutex
	err error
}

// Results returns the shared result store (may be partially populated).
func (e *Execution) Results() *Results {
	return e.results
}

// Metrics returns the current execution metrics snapshot.
func (e *Execution) Metrics() ExecutionMetrics {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.metrics == nil {
		return ExecutionMetrics{}
	}
	copied := *e.metrics
	return copied
}

// Done reports when execution has completed.
func (e *Execution) Done() <-chan struct{} {
	return e.done
}

// Await blocks until execution completes and returns the final results.
func (e *Execution) Await() (*Results, ExecutionMetrics, error) {
	<-e.done
	return e.results, e.Metrics(), e.Err()
}

// Err returns the first error encountered.
func (e *Execution) Err() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.err
}

// Cancel requests cancellation of the execution.
func (e *Execution) Cancel() {
	if e.cancel != nil {
		e.cancel()
	}
}

func (e *Execution) setError(err error) {
	if err == nil {
		return
	}
	e.mu.Lock()
	if e.err == nil {
		e.err = err
	}
	e.mu.Unlock()
}

// ExecutionMetrics aggregates run-level measurements.
type ExecutionMetrics struct {
	StartedAt      time.Time
	CompletedAt    time.Time
	Duration       time.Duration
	MaxConcurrency int
	TasksTotal     int
	TasksSucceeded int
	TasksFailed    int
	TasksSkipped   int
}

// Run executes the graph synchronously and blocks until it finishes.
func (g *Graph) Run(ctx context.Context, opts ...ExecutorOption) (*Results, ExecutionMetrics, error) {
	exec := g.Start(ctx, opts...)
	return exec.Await()
}

// Start begins executing the graph asynchronously.
func (g *Graph) Start(ctx context.Context, opts ...ExecutorOption) *Execution {
	execOpts := defaultExecutorOptions()
	for _, opt := range opts {
		opt(&execOpts)
	}

	exec := &Execution{
		done: make(chan struct{}),
	}

	analysis, err := analyzeGraph(g)
	if err != nil {
		exec.metrics = &ExecutionMetrics{
			StartedAt:   now(),
			CompletedAt: now(),
		}
		exec.setError(err)
		close(exec.done)
		return exec
	}

	results := newResults(analysis.nodes)
	exec.metrics = &ExecutionMetrics{
		StartedAt:  now(),
		TasksTotal: len(analysis.nodes),
	}

	runCtx, cancel := context.WithCancel(ctx)
	exec.cancel = cancel
	exec.results = results

	engine := &executor{
		ctx:        runCtx,
		cancel:     cancel,
		options:    execOpts,
		analysis:   analysis,
		results:    results,
		execution:  exec,
		dispatcher: execOpts.dispatcher,
		metrics:    exec.metrics,
	}

	go engine.run()
	return exec
}

type executor struct {
	ctx        context.Context
	cancel     context.CancelFunc
	options    executorOptions
	analysis   *analysis
	results    *Results
	execution  *Execution
	dispatcher Dispatcher
	metrics    *ExecutionMetrics

	active         atomic.Int64
	maxConcurrency atomic.Int64

	metricsMu sync.Mutex
}

type taskState struct {
	node      *node
	remaining int
	blocked   bool
	blockErr  error
	queued    bool
}

type taskResult struct {
	id     string
	status TaskStatus
	err    error
}

func (ex *executor) run() {
	defer close(ex.execution.done)
	defer ex.dispatcher.Stop()

	states := make(map[string]*taskState, len(ex.analysis.nodes))
	ready := make([]*taskState, 0)

	for id, node := range ex.analysis.nodes {
		state := &taskState{
			node:      node,
			remaining: ex.analysis.indegree[id],
		}
		states[id] = state
		if state.remaining == 0 {
			state.queued = true
			ready = append(ready, state)
		}
	}

	if len(states) == 0 {
		ex.finalize(nil)
		return
	}

	doneCh := make(chan taskResult, len(states))
	var wg sync.WaitGroup
	pending := len(states)

	for pending > 0 {
		for len(ready) > 0 {
			state := ready[0]
			ready = ready[1:]
			if state == nil || state.node == nil {
				continue
			}
			if state.blocked {
				result := ex.skipTask(state, state.blockErr)
				doneCh <- result
				continue
			}
			if err := ex.ctx.Err(); err != nil {
				result := ex.skipTask(state, err)
				doneCh <- result
				continue
			}
			wg.Add(1)
			st := state
			ex.dispatcher.Submit(func() {
				defer wg.Done()
				doneCh <- ex.runTask(st)
			})
		}

		if pending == 0 {
			break
		}

		result := <-doneCh
		pending--
		ex.handleResult(result, states, &ready)
	}

	wg.Wait()
	ex.finalize(ex.ctx.Err())
}

func (ex *executor) finalize(ctxErr error) {
	ex.metricsMu.Lock()
	ex.metrics.CompletedAt = now()
	ex.metrics.Duration = ex.metrics.CompletedAt.Sub(ex.metrics.StartedAt)
	ex.metrics.MaxConcurrency = int(ex.maxConcurrency.Load())
	ex.metricsMu.Unlock()

	if ctxErr != nil && !errors.Is(ex.execution.Err(), ctxErr) {
		ex.execution.setError(ctxErr)
	}
}

func (ex *executor) runTask(state *taskState) taskResult {
	meta := TaskMetadata{
		ID:           state.node.id,
		Name:         state.node.name,
		Dependencies: append([]string(nil), state.node.deps...),
	}
	hooks := ex.options.hooks.Merge(state.node.hooks)

	current := int(ex.active.Add(1))
	defer ex.active.Add(-1)
	ex.updateMaxConcurrency(current)

	startMetrics := ex.results.recordStart(state.node.id, current)
	ex.invokeHook(hooks.OnStart, TaskEvent{
		Metadata: meta,
		Metrics:  startMetrics,
	})

	var (
		value  any
		runErr error
	)

	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				runErr = TaskPanicError{
					TaskID: state.node.id,
					Value:  recovered,
				}
			}
		}()

		value, runErr = state.node.run(ex.ctx, &resolver{
			results: ex.results,
			node:    state.node,
		})
	}()

	var status TaskStatus
	var metrics TaskMetrics
	var result any

	if runErr != nil {
		status = StatusFailed
		metrics = ex.results.recordFailure(state.node.id, runErr, StatusFailed)
		ex.metricsMu.Lock()
		ex.metrics.TasksFailed++
		ex.metricsMu.Unlock()
		ex.invokeHook(hooks.OnFailure, TaskEvent{
			Metadata: meta,
			Metrics:  metrics,
		})
		ex.execution.setError(runErr)
		if ex.options.strategy == FailFast {
			ex.cancel()
		}
	} else {
		status = StatusSucceeded
		metrics = ex.results.recordSuccess(state.node.id, value)
		ex.metricsMu.Lock()
		ex.metrics.TasksSucceeded++
		ex.metricsMu.Unlock()
		ex.invokeHook(hooks.OnSuccess, TaskEvent{
			Metadata: meta,
			Metrics:  metrics,
			Result:   value,
		})
		result = value
	}

	finished := ex.results.recordFinish(state.node.id)
	ex.invokeHook(hooks.OnFinish, TaskEvent{
		Metadata: meta,
		Metrics:  finished,
		Result:   result,
	})

	return taskResult{
		id:     state.node.id,
		status: status,
		err:    runErr,
	}
}

func (ex *executor) skipTask(state *taskState, reason error) taskResult {
	meta := TaskMetadata{
		ID:           state.node.id,
		Name:         state.node.name,
		Dependencies: append([]string(nil), state.node.deps...),
	}
	if reason == nil {
		reason = fmt.Errorf("%w: dependency failure", ErrDependencyFailed)
	}
	state.blocked = true
	state.blockErr = reason
	hooks := ex.options.hooks.Merge(state.node.hooks)
	metrics := ex.results.recordSkip(state.node.id, reason)
	ex.metricsMu.Lock()
	ex.metrics.TasksSkipped++
	ex.metricsMu.Unlock()
	ex.invokeHook(hooks.OnFailure, TaskEvent{
		Metadata: meta,
		Metrics:  metrics,
	})
	finished := ex.results.recordFinish(state.node.id)
	ex.invokeHook(hooks.OnFinish, TaskEvent{
		Metadata: meta,
		Metrics:  finished,
	})
	ex.execution.setError(reason)
	return taskResult{
		id:     state.node.id,
		status: StatusSkipped,
		err:    reason,
	}
}

func (ex *executor) handleResult(result taskResult, states map[string]*taskState, ready *[]*taskState) {
	for _, dependentID := range ex.analysis.dependents[result.id] {
		depState := states[dependentID]
		depState.remaining--
		if depState.remaining < 0 {
			depState.remaining = 0
		}
		switch result.status {
		case StatusFailed:
			if depState.blockErr == nil {
				if result.err != nil {
					depState.blockErr = fmt.Errorf("%w: dependency %s failed: %v", ErrDependencyFailed, result.id, result.err)
				} else {
					depState.blockErr = fmt.Errorf("%w: dependency %s failed", ErrDependencyFailed, result.id)
				}
			}
			depState.blocked = true
		case StatusSkipped:
			if depState.blockErr == nil {
				if result.err != nil {
					depState.blockErr = fmt.Errorf("%w: dependency %s skipped: %v", ErrDependencyFailed, result.id, result.err)
				} else {
					depState.blockErr = fmt.Errorf("%w: dependency %s skipped", ErrDependencyFailed, result.id)
				}
			}
			depState.blocked = true
		}
		if depState.remaining == 0 && !depState.queued {
			depState.queued = true
			*ready = append(*ready, depState)
		}
	}
}

func (ex *executor) invokeHook(hook HookFunc, event TaskEvent) {
	if hook != nil {
		hook(ex.ctx, event)
	}
}

func (ex *executor) updateMaxConcurrency(current int) {
	for {
		max := int(ex.maxConcurrency.Load())
		if current <= max {
			return
		}
		if ex.maxConcurrency.CompareAndSwap(int64(max), int64(current)) {
			ex.metricsMu.Lock()
			if current > ex.metrics.MaxConcurrency {
				ex.metrics.MaxConcurrency = current
			}
			ex.metricsMu.Unlock()
			return
		}
	}
}
