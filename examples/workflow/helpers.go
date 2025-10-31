package main

import (
	"context"
	"fmt"
	"log"

	"github.com/bpradana/weave"
)

func mustTask[T any](g *weave.Graph, name string, fn weave.TaskFunc[T], opts ...weave.TaskOption) *weave.Handle[T] {
	if fn == nil {
		panic(fmt.Sprintf("task %s: nil function", name))
	}

	wrapped := func(ctx context.Context, deps weave.DependencyResolver) (T, error) {
		log.Printf("[task:start] %s", name)
		value, err := fn(ctx, deps)
		if err != nil {
			log.Printf("[task:fail ] %s err=%v", name, err)
			var zero T
			return zero, err
		}
		log.Printf("[task:done ] %s", name)
		return value, nil
	}

	handle, err := weave.AddTask(g, name, wrapped, opts...)
	if err != nil {
		panic(fmt.Sprintf("add task %s: %v", name, err))
	}
	return handle
}

func mustGraphTask[T any](parent *weave.Graph, name string, subgraph *weave.Graph, fn weave.GraphTaskResultFunc[T], opts ...weave.GraphTaskOption) *weave.Handle[T] {
	if fn == nil {
		panic(fmt.Sprintf("graph task %s: nil result func", name))
	}

	logStart := weave.WithGraphTaskOptions(weave.WithHooks(weave.Hooks{
		OnStart: func(ctx context.Context, event weave.TaskEvent) {
			log.Printf("[task:start] %s", name)
		},
	}))

	wrapped := func(ctx context.Context, results *weave.Results, metrics weave.ExecutionMetrics, runErr error) (T, error) {
		value, err := fn(ctx, results, metrics, runErr)
		switch {
		case err != nil:
			log.Printf("[task:fail ] %s err=%v", name, err)
			var zero T
			return zero, err
		case runErr != nil:
			log.Printf("[task:warn ] %s handled subgraph error: %v", name, runErr)
			return value, nil
		default:
			log.Printf("[task:done ] %s", name)
			return value, nil
		}
	}

	allOpts := append([]weave.GraphTaskOption{logStart}, opts...)
	handle, err := weave.AddGraphTask(parent, name, subgraph, wrapped, allOpts...)
	if err != nil {
		panic(fmt.Sprintf("add graph task %s: %v", name, err))
	}
	return handle
}
