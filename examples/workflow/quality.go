package main

import (
	"context"
	"math"

	"github.com/bpradana/weave"
)

type qualityGraph struct {
	graph   *weave.Graph
	summary *weave.Handle[qualityReport]
}

func buildQualityGraph(ing ingestionReport, proc processingReport) qualityGraph {
	g := weave.NewGraph()

	ingestionWarnings := mustTask(g, "ingestion-warnings", func(ctx context.Context, deps weave.DependencyResolver) ([]string, error) {
		return append([]string(nil), ing.Warnings...), nil
	})

	coverage := mustTask(g, "coverage-metrics", func(ctx context.Context, deps weave.DependencyResolver) (float64, error) {
		totalStreams := float64(len(proc.Videos.StreamsGenerated))
		totalCollections := float64(ing.SourcesDiscovered)
		if totalCollections == 0 {
			return 0, nil
		}
		return math.Min(1, totalStreams/totalCollections), nil
	})

	failureScan := mustTask(g, "scan-failures", func(ctx context.Context, deps weave.DependencyResolver) ([]string, error) {
		warnings, err := ingestionWarnings.Value(deps)
		if err != nil {
			return nil, err
		}
		failures := append([]string(nil), warnings...)
		if len(proc.Checks) == 0 {
			failures = append(failures, "missing qa checks")
		}
		return failures, nil
	}, weave.DependsOn(ingestionWarnings))

	summary := mustTask(g, "quality-summary", func(ctx context.Context, deps weave.DependencyResolver) (qualityReport, error) {
		score, err := coverage.Value(deps)
		if err != nil {
			return qualityReport{}, err
		}
		failures, err := failureScan.Value(deps)
		if err != nil {
			return qualityReport{}, err
		}
		adjusted := score
		if len(failures) > 0 {
			adjusted = score * 0.6
		}
		return qualityReport{
			Score:    adjusted,
			Failures: failures,
		}, nil
	}, weave.DependsOn(coverage, failureScan))

	return qualityGraph{graph: g, summary: summary}
}

func (qg qualityGraph) result(ctx context.Context, results *weave.Results, metrics weave.ExecutionMetrics, runErr error) (qualityReport, error) {
	if runErr != nil {
		return qualityReport{}, runErr
	}
	return qg.summary.Value(results)
}
