package main

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/bpradana/weave"
)

type ingestionGraph struct {
	graph   *weave.Graph
	summary *weave.Handle[ingestionReport]
}

func buildIngestionGraph(rng *rand.Rand) ingestionGraph {
	g := weave.NewGraph()

	loadSources := mustTask(g, "load-source-catalog", func(ctx context.Context, deps weave.DependencyResolver) ([]string, error) {
		time.Sleep(25 * time.Millisecond)
		return []string{"cms", "live-blog", "video-ingest", "partners"}, nil
	})

	fetchOverrides := mustTask(g, "fetch-manual-overrides", func(ctx context.Context, deps weave.DependencyResolver) (map[string]bool, error) {
		time.Sleep(20 * time.Millisecond)
		return map[string]bool{"partners": true}, nil
	})

	fetchSchedule := mustTask(g, "fetch-schedule", func(ctx context.Context, deps weave.DependencyResolver) (map[string]int, error) {
		time.Sleep(30 * time.Millisecond)
		return map[string]int{
			"cms":          42 + rng.Intn(5),
			"live-blog":    6,
			"video-ingest": 12,
			"partners":     18,
		}, nil
	})

	mergedSources := mustTask(g, "merge-schedule-with-overrides", func(ctx context.Context, deps weave.DependencyResolver) (map[string]int, error) {
		catalog, err := loadSources.Value(deps)
		if err != nil {
			return nil, err
		}
		schedule, err := fetchSchedule.Value(deps)
		if err != nil {
			return nil, err
		}
		overrides, err := fetchOverrides.Value(deps)
		if err != nil {
			return nil, err
		}
		result := make(map[string]int, len(catalog))
		for _, source := range catalog {
			count := schedule[source]
			if overrides[source] {
				count += 3
			}
			result[source] = count
		}
		return result, nil
	}, weave.DependsOn(loadSources, fetchSchedule, fetchOverrides))

	normalize := mustTask(g, "normalize-sources", func(ctx context.Context, deps weave.DependencyResolver) (map[string]int, error) {
		merged, err := mergedSources.Value(deps)
		if err != nil {
			return nil, err
		}
		time.Sleep(15 * time.Millisecond)
		return merged, nil
	}, weave.DependsOn(mergedSources))

	validate := mustTask(g, "validate-ingestion", func(ctx context.Context, deps weave.DependencyResolver) ([]string, error) {
		merged, err := normalize.Value(deps)
		if err != nil {
			return nil, err
		}
		warnings := make([]string, 0)
		for source, count := range merged {
			if count == 0 {
				warnings = append(warnings, fmt.Sprintf("no items from %s", source))
			}
		}
		sort.Strings(warnings)
		return warnings, nil
	}, weave.DependsOn(normalize))

	summary := mustTask(g, "ingestion-summary", func(ctx context.Context, deps weave.DependencyResolver) (ingestionReport, error) {
		merged, err := normalize.Value(deps)
		if err != nil {
			return ingestionReport{}, err
		}
		warnings, err := validate.Value(deps)
		if err != nil {
			return ingestionReport{}, err
		}
		totalSources := len(merged)
		totalItems := 0
		for _, count := range merged {
			totalItems += count
		}
		return ingestionReport{
			SourcesDiscovered: totalSources,
			ItemsCollected:    totalItems,
			Warnings:          warnings,
			Normalized:        true,
		}, nil
	}, weave.DependsOn(normalize, validate))

	return ingestionGraph{
		graph:   g,
		summary: summary,
	}
}

func (ig ingestionGraph) result(ctx context.Context, results *weave.Results, metrics weave.ExecutionMetrics, runErr error) (ingestionReport, error) {
	if runErr != nil {
		return ingestionReport{}, runErr
	}
	return ig.summary.Value(results)
}
