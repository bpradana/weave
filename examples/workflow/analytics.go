package main

import (
	"context"
	"fmt"

	"github.com/bpradana/weave"
)

type analyticsGraph struct {
	graph   *weave.Graph
	summary *weave.Handle[analyticsReport]
}

func buildAnalyticsGraph(ing ingestionReport, proc processingReport, dist distributionReport, cur curationReport, qual qualityReport, mon monitoringReport) analyticsGraph {
	g := weave.NewGraph()

	aggregate := mustTask(g, "aggregate-metrics", func(ctx context.Context, deps weave.DependencyResolver) (map[string]float64, error) {
		metrics := map[string]float64{
			"sources":     float64(ing.SourcesDiscovered),
			"items":       float64(ing.ItemsCollected),
			"streams":     float64(len(proc.Videos.StreamsGenerated)),
			"collections": float64(cur.Collections),
			"regions":     float64(len(dist.RegionsPublished)),
			"quality":     qual.Score,
		}
		return metrics, nil
	})

	highlights := mustTask(g, "derive-highlights", func(ctx context.Context, deps weave.DependencyResolver) ([]string, error) {
		agg, err := aggregate.Value(deps)
		if err != nil {
			return nil, err
		}
		result := []string{
			fmt.Sprintf("%d collections live", int(agg["collections"])),
			fmt.Sprintf("%d regions reached", int(agg["regions"])),
			fmt.Sprintf("quality score %.2f", agg["quality"]),
		}
		if len(cur.Featured) > 0 {
			result = append(result, "featured: "+cur.Featured[0])
		}
		return result, nil
	}, weave.DependsOn(aggregate))

	risk := mustTask(g, "risk-assessment", func(ctx context.Context, deps weave.DependencyResolver) ([]string, error) {
		issues := append([]string(nil), cur.Warnings...)
		if qual.Score < 0.5 {
			issues = append(issues, "quality score below threshold")
		}
		if len(mon.Incidents) > 0 {
			issues = append(issues, "active incidents: "+mon.Incidents[0])
		}
		return issues, nil
	})

	summary := mustTask(g, "analytics-summary", func(ctx context.Context, deps weave.DependencyResolver) (analyticsReport, error) {
		high, err := highlights.Value(deps)
		if err != nil {
			return analyticsReport{}, err
		}
		riskItems, err := risk.Value(deps)
		if err != nil {
			return analyticsReport{}, err
		}
		agg, err := aggregate.Value(deps)
		if err != nil {
			return analyticsReport{}, err
		}
		score := agg["quality"]
		if len(riskItems) > 0 {
			score *= 0.75
		}
		return analyticsReport{
			Highlights: high,
			Score:      score,
			Warnings:   riskItems,
		}, nil
	}, weave.DependsOn(highlights, risk, aggregate))

	return analyticsGraph{graph: g, summary: summary}
}

func (ag analyticsGraph) result(ctx context.Context, results *weave.Results, metrics weave.ExecutionMetrics, runErr error) (analyticsReport, error) {
	if runErr != nil {
		return analyticsReport{}, runErr
	}
	return ag.summary.Value(results)
}
