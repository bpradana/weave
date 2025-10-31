package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/bpradana/weave"
)

func main() {
	graph := weave.NewGraph()

	seed := rand.New(rand.NewSource(time.Now().UnixNano()))

	taskHandles := make([]*weave.Handle[int], 0, 8)
	for i := 0; i < 8; i++ {
		index := i
		handle, err := weave.AddTask(graph, fmt.Sprintf("work-%d", index), func(ctx context.Context, deps weave.DependencyResolver) (int, error) {
			// Simulate work with a jittered sleep.
			sleep := time.Duration(25+seed.Intn(50)) * time.Millisecond
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(sleep):
				return index * 2, nil
			}
		})
		if err != nil {
			log.Fatalf("register work-%d: %v", index, err)
		}
		taskHandles = append(taskHandles, handle)
	}

	exec := graph.Start(context.Background(),
		weave.WithDispatcher(weave.NewWorkerPoolDispatcher(3)),
	)

	results, metrics, err := exec.Await()
	if err != nil {
		log.Fatalf("await graph: %v", err)
	}

	for _, handle := range taskHandles {
		value, err := handle.Value(results)
		if err != nil {
			log.Fatalf("value %s: %v", handle.ID(), err)
		}
		fmt.Printf("%s -> %d\n", handle.ID(), value)
	}
	fmt.Printf("Max concurrency observed: %d\n", metrics.MaxConcurrency)
}
