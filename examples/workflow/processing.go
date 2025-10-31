package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/bpradana/weave"
)

type processingGraph struct {
	graph   *weave.Graph
	summary *weave.Handle[processingReport]
}

func buildProcessingGraph(rng *rand.Rand) processingGraph {
	g := weave.NewGraph()

	pipelineConfig := mustTask(g, "load-processing-config", func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		time.Sleep(20 * time.Millisecond)
		return "default-processing", nil
	})

	batchPlan := mustTask(g, "plan-batches", func(ctx context.Context, deps weave.DependencyResolver) ([]string, error) {
		time.Sleep(18 * time.Millisecond)
		return []string{"morning", "afternoon", "evening"}, nil
	}, weave.DependsOn(pipelineConfig))

	video := buildVideoGraph(rng)
	videoHandle := mustGraphTask(g, "video-pipeline", video.graph, video.result,
		weave.WithGraphTaskOptions(weave.DependsOn(batchPlan)),
		weave.WithGraphTaskExecutorOptions(weave.WithDispatcher(weave.NewWorkerPoolDispatcher(4))),
	)

	article := buildArticleGraph(rng)
	articleHandle := mustGraphTask(g, "article-pipeline", article.graph, article.result,
		weave.WithGraphTaskOptions(weave.DependsOn(batchPlan)),
		weave.WithGraphTaskExecutorOptions(weave.WithDispatcher(weave.NewWorkerPoolDispatcher(3))),
	)

	image := buildImageGraph(rng)
	imageHandle := mustGraphTask(g, "image-pipeline", image.graph, image.result,
		weave.WithGraphTaskOptions(weave.DependsOn(batchPlan)),
	)

	qaChecks := mustTask(g, "run-quality-checks", func(ctx context.Context, deps weave.DependencyResolver) ([]string, error) {
		videoBundle, err := videoHandle.Value(deps)
		if err != nil {
			return nil, err
		}
		articleBundle, err := articleHandle.Value(deps)
		if err != nil {
			return nil, err
		}
		imageBundle, err := imageHandle.Value(deps)
		if err != nil {
			return nil, err
		}
		return []string{
			fmt.Sprintf("video-streams:%d", len(videoBundle.StreamsGenerated)),
			fmt.Sprintf("article-topics:%d", len(articleBundle.Topics)),
			fmt.Sprintf("image-variants:%d", len(imageBundle.Sizes)),
		}, nil
	}, weave.DependsOn(videoHandle, articleHandle, imageHandle))

	summary := mustTask(g, "processing-summary", func(ctx context.Context, deps weave.DependencyResolver) (processingReport, error) {
		videoBundle, err := videoHandle.Value(deps)
		if err != nil {
			return processingReport{}, err
		}
		articleBundle, err := articleHandle.Value(deps)
		if err != nil {
			return processingReport{}, err
		}
		imageBundle, err := imageHandle.Value(deps)
		if err != nil {
			return processingReport{}, err
		}
		checks, err := qaChecks.Value(deps)
		if err != nil {
			return processingReport{}, err
		}
		return processingReport{
			Videos:   videoBundle,
			Articles: articleBundle,
			Images:   imageBundle,
			Checks:   checks,
		}, nil
	}, weave.DependsOn(videoHandle, articleHandle, imageHandle, qaChecks))

	return processingGraph{
		graph:   g,
		summary: summary,
	}
}

func (pg processingGraph) result(ctx context.Context, results *weave.Results, metrics weave.ExecutionMetrics, runErr error) (processingReport, error) {
	if runErr != nil {
		return processingReport{}, runErr
	}
	return pg.summary.Value(results)
}
