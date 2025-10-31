package main

import (
	"context"
	"fmt"
	"log"

	"github.com/bpradana/weave"
)

func main() {
	graph := weave.NewGraph()

	userTask, err := weave.AddTask(graph, "load-user", func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		// Pretend to load from storage or network.
		return "Ada Lovelace", nil
	})
	if err != nil {
		log.Fatalf("register load-user: %v", err)
	}

	greetingTask, err := weave.AddTask(graph, "render-greeting", func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		user, err := userTask.Value(deps)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("Hello, %s!", user), nil
	}, weave.DependsOn(userTask))
	if err != nil {
		log.Fatalf("register render-greeting: %v", err)
	}

	results, metrics, err := graph.Run(context.Background())
	if err != nil {
		log.Fatalf("run graph: %v", err)
	}

	greeting, err := greetingTask.Value(results)
	if err != nil {
		log.Fatalf("read greeting: %v", err)
	}

	fmt.Println(greeting)
	fmt.Printf("Succeeded tasks: %d/%d\n", metrics.TasksSucceeded, metrics.TasksTotal)
}
