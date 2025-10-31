package weave

import (
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrUnknownTask indicates a handle does not map to a known task.
	ErrUnknownTask = errors.New("weave: unknown task")
	// ErrDependencyNotReady indicates a dependency result was requested before completion.
	ErrDependencyNotReady = errors.New("weave: dependency result not ready")
	// ErrDependencyFailed indicates a dependency finished unsuccessfully.
	ErrDependencyFailed = errors.New("weave: dependency failed")
	// ErrNotDependency indicates a task attempted to access a non-dependency handle.
	ErrNotDependency = errors.New("weave: handle is not a declared dependency of the task")
)

// DependencyResolver exposes dependency results to running tasks.
type DependencyResolver interface {
	Value(TaskReference) (any, error)
}

type resolver struct {
	results *Results
	node    *node
}

func (r *resolver) Value(ref TaskReference) (any, error) {
	if ref == nil {
		return nil, fmt.Errorf("%w: nil handle", ErrUnknownTask)
	}
	key := ref.taskKey()
	if key.graphID != r.node.graphID {
		return nil, ErrForeignDependency
	}
	if _, ok := r.node.depSet[key.id]; !ok {
		return nil, fmt.Errorf("%w: %s depends on %s", ErrNotDependency, r.node.name, key.id)
	}
	return r.results.Value(ref)
}

// Results tracks execution state of tasks.
type Results struct {
	mu       sync.RWMutex
	values   map[string]any
	errors   map[string]error
	statuses map[string]TaskStatus
	metrics  map[string]TaskMetrics
}

func newResults(nodes map[string]*node) *Results {
	statuses := make(map[string]TaskStatus, len(nodes))
	for id := range nodes {
		statuses[id] = StatusPending
	}
	return &Results{
		values:   make(map[string]any, len(nodes)),
		errors:   make(map[string]error, len(nodes)),
		statuses: statuses,
		metrics:  make(map[string]TaskMetrics, len(nodes)),
	}
}

// Value retrieves the raw result for a completed task.
func (r *Results) Value(ref TaskReference) (any, error) {
	if ref == nil {
		return nil, fmt.Errorf("%w: nil handle", ErrUnknownTask)
	}
	key := ref.taskKey()

	r.mu.RLock()
	defer r.mu.RUnlock()

	status, ok := r.statuses[key.id]
	if !ok {
		return nil, ErrUnknownTask
	}
	switch status {
	case StatusSucceeded:
		value, ok := r.values[key.id]
		if !ok {
			return nil, ErrDependencyNotReady
		}
		return value, nil
	case StatusFailed:
		return nil, r.errors[key.id]
	case StatusSkipped:
		return nil, ErrDependencyFailed
	default:
		return nil, ErrDependencyNotReady
	}
}

// Error obtains the error recorded for a task.
func (r *Results) Error(ref TaskReference) error {
	if ref == nil {
		return ErrUnknownTask
	}
	key := ref.taskKey()

	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.errors[key.id]
}

// Status returns the observed status for a task.
func (r *Results) Status(ref TaskReference) TaskStatus {
	if ref == nil {
		return StatusPending
	}
	key := ref.taskKey()

	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.statuses[key.id]
}

// Metrics returns task metrics if available.
func (r *Results) Metrics(ref TaskReference) (TaskMetrics, bool) {
	if ref == nil {
		return TaskMetrics{}, false
	}
	key := ref.taskKey()

	r.mu.RLock()
	defer r.mu.RUnlock()
	metrics, ok := r.metrics[key.id]
	return metrics, ok
}

func (r *Results) recordStart(id string, concurrency int) TaskMetrics {
	r.mu.Lock()
	metrics := r.metrics[id]
	metrics.StartedAt = now()
	metrics.Concurrency = concurrency
	metrics.Status = StatusRunning
	r.metrics[id] = metrics
	r.statuses[id] = StatusRunning
	r.mu.Unlock()
	return metrics
}

func (r *Results) recordSuccess(id string, value any) TaskMetrics {
	r.mu.Lock()
	r.values[id] = value
	metrics := r.metrics[id]
	metrics.CompletedAt = now()
	metrics.Duration = metrics.CompletedAt.Sub(metrics.StartedAt)
	metrics.Status = StatusSucceeded
	r.metrics[id] = metrics
	r.statuses[id] = StatusSucceeded
	r.mu.Unlock()
	return metrics
}

func (r *Results) recordFailure(id string, err error, status TaskStatus) TaskMetrics {
	r.mu.Lock()
	r.errors[id] = err
	metrics := r.metrics[id]
	if metrics.StartedAt.IsZero() {
		metrics.StartedAt = now()
	}
	metrics.CompletedAt = now()
	if metrics.Duration == 0 && !metrics.StartedAt.IsZero() {
		metrics.Duration = metrics.CompletedAt.Sub(metrics.StartedAt)
	}
	metrics.Error = err
	metrics.Status = status
	r.metrics[id] = metrics
	r.statuses[id] = status
	r.mu.Unlock()
	return metrics
}

func (r *Results) recordSkip(id string, err error) TaskMetrics {
	return r.recordFailure(id, err, StatusSkipped)
}

func (r *Results) recordFinish(id string) TaskMetrics {
	r.mu.Lock()
	metrics := r.metrics[id]
	if metrics.CompletedAt.IsZero() {
		metrics.CompletedAt = now()
	}
	if metrics.Duration == 0 && !metrics.StartedAt.IsZero() {
		metrics.Duration = metrics.CompletedAt.Sub(metrics.StartedAt)
	}
	r.metrics[id] = metrics
	r.mu.Unlock()
	return metrics
}
