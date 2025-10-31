package main

import (
	"context"
	"math/rand"
	"sort"
	"time"

	"github.com/bpradana/weave"
)

type articleGraph struct {
	graph  *weave.Graph
	bundle *weave.Handle[articleBundle]
}

func buildArticleGraph(rng *rand.Rand) articleGraph {
	g := weave.NewGraph()

	body := mustTask(g, "fetch-article-bodies", func(ctx context.Context, deps weave.DependencyResolver) ([]string, error) {
		time.Sleep(22 * time.Millisecond)
		return []string{"tech", "finance", "sports"}, nil
	})

	clean := mustTask(g, "clean-markup", func(ctx context.Context, deps weave.DependencyResolver) ([]string, error) {
		items, err := body.Value(deps)
		if err != nil {
			return nil, err
		}
		time.Sleep(18 * time.Millisecond)
		return items, nil
	}, weave.DependsOn(body))

	nlp := buildNLPGraph()
	nlpHandle := mustGraphTask(g, "nlp-enrichment", nlp.graph, nlp.result,
		weave.WithGraphTaskOptions(weave.DependsOn(clean)),
	)

	compliance := buildComplianceGraph()
	complianceHandle := mustGraphTask(g, "compliance-review", compliance.graph, compliance.result,
		weave.WithGraphTaskOptions(weave.DependsOn(nlpHandle)),
	)

	summarise := mustTask(g, "summarise-articles", func(ctx context.Context, deps weave.DependencyResolver) (articleBundle, error) {
		topics, err := nlpHandle.Value(deps)
		if err != nil {
			return articleBundle{}, err
		}
		_, err = complianceHandle.Value(deps)
		if err != nil {
			return articleBundle{}, err
		}
		return articleBundle{
			Stories: rng.Intn(5) + 9,
			Topics:  topics,
			Summary: "all content ready for editorial review",
		}, nil
	}, weave.DependsOn(nlpHandle, complianceHandle))

	return articleGraph{
		graph:  g,
		bundle: summarise,
	}
}

func (ag articleGraph) result(ctx context.Context, results *weave.Results, metrics weave.ExecutionMetrics, runErr error) (articleBundle, error) {
	if runErr != nil {
		return articleBundle{}, runErr
	}
	return ag.bundle.Value(results)
}

type nlpGraph struct {
	graph  *weave.Graph
	topics *weave.Handle[[]string]
}

func buildNLPGraph() nlpGraph {
	g := weave.NewGraph()

	extract := mustTask(g, "extract-entities", func(ctx context.Context, deps weave.DependencyResolver) ([]string, error) {
		time.Sleep(16 * time.Millisecond)
		return []string{"ai", "markets", "sports"}, nil
	})

	categorise := mustTask(g, "categorise-topics", func(ctx context.Context, deps weave.DependencyResolver) ([]string, error) {
		entities, err := extract.Value(deps)
		if err != nil {
			return nil, err
		}
		time.Sleep(14 * time.Millisecond)
		sort.Strings(entities)
		return entities, nil
	}, weave.DependsOn(extract))

	sentiment := mustTask(g, "sentiment-analysis", func(ctx context.Context, deps weave.DependencyResolver) (float64, error) {
		time.Sleep(10 * time.Millisecond)
		return 0.12, nil
	}, weave.DependsOn(categorise))

	result := mustTask(g, "nlp-result", func(ctx context.Context, deps weave.DependencyResolver) ([]string, error) {
		topics, err := categorise.Value(deps)
		if err != nil {
			return nil, err
		}
		_, err = sentiment.Value(deps)
		if err != nil {
			return nil, err
		}
		return topics, nil
	}, weave.DependsOn(categorise, sentiment))

	return nlpGraph{
		graph:  g,
		topics: result,
	}
}

func (ng nlpGraph) result(ctx context.Context, results *weave.Results, metrics weave.ExecutionMetrics, runErr error) ([]string, error) {
	if runErr != nil {
		return nil, runErr
	}
	return ng.topics.Value(results)
}

type complianceGraph struct {
	graph    *weave.Graph
	approval *weave.Handle[bool]
}

func buildComplianceGraph() complianceGraph {
	g := weave.NewGraph()

	policy := mustTask(g, "load-policy", func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		time.Sleep(12 * time.Millisecond)
		return "policy-v3", nil
	})

	lint := mustTask(g, "lint-content", func(ctx context.Context, deps weave.DependencyResolver) ([]string, error) {
		time.Sleep(14 * time.Millisecond)
		return []string{}, nil
	}, weave.DependsOn(policy))

	final := mustTask(g, "compliance-result", func(ctx context.Context, deps weave.DependencyResolver) (bool, error) {
		_, err := lint.Value(deps)
		if err != nil {
			return false, err
		}
		return true, nil
	}, weave.DependsOn(lint))

	return complianceGraph{
		graph:    g,
		approval: final,
	}
}

func (cg complianceGraph) result(ctx context.Context, results *weave.Results, metrics weave.ExecutionMetrics, runErr error) (bool, error) {
	if runErr != nil {
		return false, runErr
	}
	return cg.approval.Value(results)
}
