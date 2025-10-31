package main

import (
	"context"
	"math/rand"
	"time"

	"github.com/bpradana/weave"
)

type monitoringGraph struct {
	graph   *weave.Graph
	summary *weave.Handle[monitoringReport]
}

func buildMonitoringGraph(rng *rand.Rand) monitoringGraph {
	g := weave.NewGraph()

	collectMetrics := mustTask(g, "collect-metrics", func(ctx context.Context, deps weave.DependencyResolver) ([]string, error) {
		time.Sleep(18 * time.Millisecond)
		return []string{"latency", "error-rate", "throughput"}, nil
	})

	updateDashboards := mustTask(g, "update-dashboards", func(ctx context.Context, deps weave.DependencyResolver) (bool, error) {
		time.Sleep(16 * time.Millisecond)
		return true, nil
	}, weave.DependsOn(collectMetrics))

	incidentReview := mustTask(g, "incident-review", func(ctx context.Context, deps weave.DependencyResolver) ([]string, error) {
		time.Sleep(15 * time.Millisecond)
		if rng.Intn(10) > 7 {
			return []string{"edge-cache spike"}, nil
		}
		return []string{}, nil
	}, weave.DependsOn(updateDashboards))

	summary := mustTask(g, "monitoring-summary", func(ctx context.Context, deps weave.DependencyResolver) (monitoringReport, error) {
		dashReady, err := updateDashboards.Value(deps)
		if err != nil {
			return monitoringReport{}, err
		}
		signals, err := collectMetrics.Value(deps)
		if err != nil {
			return monitoringReport{}, err
		}
		incidents, err := incidentReview.Value(deps)
		if err != nil {
			return monitoringReport{}, err
		}
		return monitoringReport{
			DashboardsReady: dashReady,
			Incidents:       incidents,
			SignalsTracked:  signals,
		}, nil
	}, weave.DependsOn(updateDashboards, collectMetrics, incidentReview))

	return monitoringGraph{
		graph:   g,
		summary: summary,
	}
}

func (mg monitoringGraph) result(ctx context.Context, results *weave.Results, metrics weave.ExecutionMetrics, runErr error) (monitoringReport, error) {
	if runErr != nil {
		return monitoringReport{}, runErr
	}
	return mg.summary.Value(results)
}
