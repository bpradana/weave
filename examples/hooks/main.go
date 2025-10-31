package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/bpradana/weave"
)

func main() {
	graph := weave.NewGraph()

	hooks := weave.Hooks{
		OnStart: func(ctx context.Context, event weave.TaskEvent) {
			log.Printf("starting %s (deps=%v)", event.Metadata.Name, event.Metadata.Dependencies)
		},
		OnSuccess: func(ctx context.Context, event weave.TaskEvent) {
			log.Printf("finished %s in %s", event.Metadata.Name, event.Metrics.Duration)
		},
		OnFailure: func(ctx context.Context, event weave.TaskEvent) {
			log.Printf("failed %s: %v", event.Metadata.Name, event.Metrics.Error)
		},
	}

	fetchData, err := weave.AddTask(graph, "fetch-data", func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		time.Sleep(50 * time.Millisecond)
		return "payload", nil
	})
	if err != nil {
		log.Fatalf("register fetch-data: %v", err)
	}

	processData, err := weave.AddTask(graph, "process-data", func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		payload, err := fetchData.Value(deps)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("processed(%s)", payload), nil
	}, weave.WithHooks(weave.Hooks{
		OnFinish: func(ctx context.Context, event weave.TaskEvent) {
			log.Printf("process-data result: %v", event.Result)
		},
	}), weave.DependsOn(fetchData))
	if err != nil {
		log.Fatalf("register process-data: %v", err)
	}

	results, _, err := graph.Run(context.Background(), weave.WithGlobalHooks(hooks))
	if err != nil {
		log.Fatalf("run graph: %v", err)
	}

	value, err := processData.Value(results)
	if err != nil {
		log.Fatalf("result: %v", err)
	}
	fmt.Println(value)
}
