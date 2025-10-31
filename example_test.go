package weave_test

import (
	"context"
	"fmt"

	"github.com/bpradana/weave"
)

func ExampleGraph_Run() {
	graph := weave.NewGraph()

	fetchName, err := weave.AddTask(graph, "fetch-name", func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		return "Ada", nil
	})
	if err != nil {
		panic(err)
	}

	greet, err := weave.AddTask(graph, "greet", func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		name, err := fetchName.Value(deps)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("Hello, %s!", name), nil
	}, weave.DependsOn(fetchName))
	if err != nil {
		panic(err)
	}

	results, _, err := graph.Run(context.Background())
	if err != nil {
		panic(err)
	}

	message, err := greet.Value(results)
	if err != nil {
		panic(err)
	}
	fmt.Println(message)
	// Output: Hello, Ada!
}
