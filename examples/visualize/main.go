package main

import (
	"bytes"
	"context"
	"fmt"
	"log"

	"github.com/bpradana/weave"
)

func main() {
	graph := weave.NewGraph()

	build, err := weave.AddTask(graph, "build", func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		return "artifact.tar.gz", nil
	})
	if err != nil {
		log.Fatalf("register build: %v", err)
	}

	test, err := weave.AddTask(graph, "test", func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		return build.Value(deps)
	}, weave.DependsOn(build))
	if err != nil {
		log.Fatalf("register test: %v", err)
	}

	_, err = weave.AddTask(graph, "deploy", func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		artifact, err := test.Value(deps)
		if err != nil {
			return "", err
		}
		return "deployed " + artifact, nil
	}, weave.DependsOn(test))
	if err != nil {
		log.Fatalf("register deploy: %v", err)
	}

	var buf bytes.Buffer
	if err := graph.ExportDOT(&buf, weave.DOTWithGraphName("ci_pipeline"), weave.DOTWithRankDir("LR")); err != nil {
		log.Fatalf("export DOT: %v", err)
	}

	fmt.Print(buf.String())
}
