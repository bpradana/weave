package main

import (
	"context"
	"math/rand"
	"time"

	"github.com/bpradana/weave"
)

type curationGraph struct {
	graph   *weave.Graph
	summary *weave.Handle[curationReport]
}

func buildCurationGraph(rng *rand.Rand) curationGraph {
	g := weave.NewGraph()

	loadCollections := mustTask(g, "load-collections", func(ctx context.Context, deps weave.DependencyResolver) ([]string, error) {
		time.Sleep(18 * time.Millisecond)
		return []string{"daily-brief", "deep-dive", "spotlight"}, nil
	})

	highlightCandidates := mustTask(g, "pick-featured-candidates", func(ctx context.Context, deps weave.DependencyResolver) ([]string, error) {
		time.Sleep(20 * time.Millisecond)
		pool := []string{"Ada Lovelace", "Grace Hopper", "Alan Turing"}
		rng.Shuffle(len(pool), func(i, j int) { pool[i], pool[j] = pool[j], pool[i] })
		return pool, nil
	}, weave.DependsOn(loadCollections))

	tagEnrichment := mustTask(g, "tag-enrichment", func(ctx context.Context, deps weave.DependencyResolver) ([]string, error) {
		time.Sleep(16 * time.Millisecond)
		return []string{"ai", "innovation", "cloud", "security"}, nil
	}, weave.DependsOn(loadCollections))

	validateSets := mustTask(g, "validate-curation", func(ctx context.Context, deps weave.DependencyResolver) ([]string, error) {
		candidates, err := highlightCandidates.Value(deps)
		if err != nil {
			return nil, err
		}
		if len(candidates) < 2 {
			return []string{"insufficient featured items"}, nil
		}
		return []string{}, nil
	}, weave.DependsOn(highlightCandidates))

	summary := mustTask(g, "curation-summary", func(ctx context.Context, deps weave.DependencyResolver) (curationReport, error) {
		collections, err := loadCollections.Value(deps)
		if err != nil {
			return curationReport{}, err
		}
		featured, err := highlightCandidates.Value(deps)
		if err != nil {
			return curationReport{}, err
		}
		tags, err := tagEnrichment.Value(deps)
		if err != nil {
			return curationReport{}, err
		}
		failures, err := validateSets.Value(deps)
		if err != nil {
			return curationReport{}, err
		}
		report := curationReport{
			Collections: len(collections),
			Featured:    featured[:2],
			Tags:        tags,
			Warnings:    failures,
		}
		return report, nil
	}, weave.DependsOn(loadCollections, highlightCandidates, tagEnrichment, validateSets))

	return curationGraph{
		graph:   g,
		summary: summary,
	}
}

func (cg curationGraph) result(ctx context.Context, results *weave.Results, metrics weave.ExecutionMetrics, runErr error) (curationReport, error) {
	if runErr != nil {
		return curationReport{}, runErr
	}
	return cg.summary.Value(results)
}
