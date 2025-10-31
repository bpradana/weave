package weave

import (
	"context"
	"time"
)

// TaskStatus captures the lifecycle status of a task.
type TaskStatus string

const (
	StatusPending   TaskStatus = "pending"
	StatusRunning   TaskStatus = "running"
	StatusSucceeded TaskStatus = "succeeded"
	StatusFailed    TaskStatus = "failed"
	StatusSkipped   TaskStatus = "skipped"
)

// TaskMetadata provides identifying information about a task.
type TaskMetadata struct {
	ID           string
	Name         string
	Dependencies []string
}

// TaskMetrics captures execution metrics for a task.
type TaskMetrics struct {
	StartedAt   time.Time
	CompletedAt time.Time
	Duration    time.Duration
	Concurrency int
	Status      TaskStatus
	Error       error
}

// TaskEvent is passed to hook callbacks to describe task progress.
type TaskEvent struct {
	Metadata TaskMetadata
	Metrics  TaskMetrics
	Result   any
}

// HookFunc is invoked for lifecycle notifications.
type HookFunc func(context.Context, TaskEvent)

// Hooks aggregates optional lifecycle callbacks.
type Hooks struct {
	OnStart   HookFunc
	OnSuccess HookFunc
	OnFailure HookFunc
	OnFinish  HookFunc
}

// Merge combines two hook sets, running the receiver first.
func (h Hooks) Merge(other Hooks) Hooks {
	return Hooks{
		OnStart:   chainHooks(h.OnStart, other.OnStart),
		OnSuccess: chainHooks(h.OnSuccess, other.OnSuccess),
		OnFailure: chainHooks(h.OnFailure, other.OnFailure),
		OnFinish:  chainHooks(h.OnFinish, other.OnFinish),
	}
}

func chainHooks(first, second HookFunc) HookFunc {
	switch {
	case first == nil:
		return second
	case second == nil:
		return first
	default:
		return func(ctx context.Context, event TaskEvent) {
			first(ctx, event)
			second(ctx, event)
		}
	}
}
