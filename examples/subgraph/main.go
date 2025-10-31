package main

import (
	"context"
	"fmt"
	"log"

	"github.com/bpradana/weave"
)

func main() {
	// Build a reusable subgraph that loads data and formats a summary.
	ordersGraph := weave.NewGraph()

	userTask, err := weave.AddTask(ordersGraph, "fetch-user", func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		return "Ada Lovelace", nil
	})
	if err != nil {
		log.Fatalf("register fetch-user: %v", err)
	}

	ordersTask, err := weave.AddTask(ordersGraph, "fetch-orders", func(ctx context.Context, deps weave.DependencyResolver) ([]string, error) {
		return []string{"Analytical Engine Manual", "Difference Engine Schematics"}, nil
	})
	if err != nil {
		log.Fatalf("register fetch-orders: %v", err)
	}

	summaryTask, err := weave.AddTask(ordersGraph, "build-summary", func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		user, err := userTask.Value(deps)
		if err != nil {
			return "", err
		}
		orders, err := ordersTask.Value(deps)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s has %d open orders", user, len(orders)), nil
	}, weave.DependsOn(userTask, ordersTask))
	if err != nil {
		log.Fatalf("register build-summary: %v", err)
	}

	parentGraph := weave.NewGraph()

	ordersSubgraph, err := weave.AddGraphTask(parentGraph, "orders-subgraph", ordersGraph, func(ctx context.Context, results *weave.Results, metrics weave.ExecutionMetrics, runErr error) (string, error) {
		if runErr != nil {
			return "", runErr
		}
		summary, err := summaryTask.Value(results)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s (subgraph tasks succeeded: %d)", summary, metrics.TasksSucceeded), nil
	})
	if err != nil {
		log.Fatalf("register orders-subgraph: %v", err)
	}

	notifyTask, err := weave.AddTask(parentGraph, "notify", func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		message, err := ordersSubgraph.Value(deps)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("Sending notification: %s", message), nil
	}, weave.DependsOn(ordersSubgraph))
	if err != nil {
		log.Fatalf("register notify: %v", err)
	}

	results, metrics, err := parentGraph.Run(context.Background())
	if err != nil {
		log.Fatalf("run parent graph: %v", err)
	}

	notification, err := notifyTask.Value(results)
	if err != nil {
		log.Fatalf("read notification: %v", err)
	}

	fmt.Println(notification)
	fmt.Printf("Parent graph tasks succeeded: %d/%d\n", metrics.TasksSucceeded, metrics.TasksTotal)
}
