package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/bpradana/weave"
)

func main() {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	workflow := weave.NewGraph()

	configTask := mustTask(workflow, "load-workflow-config", func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		return time.Now().UTC().Format("2006-01-02"), nil
	})

	ingestion := buildIngestionGraph(rng)
	ingestionHandle := mustGraphTask(workflow, "ingestion-stage", ingestion.graph, ingestion.result,
		weave.WithGraphTaskOptions(weave.DependsOn(configTask)),
		weave.WithGraphTaskExecutorOptions(weave.WithDispatcher(weave.NewWorkerPoolDispatcher(4))),
	)

	curation := buildCurationGraph(rng)
	curationHandle := mustGraphTask(workflow, "curation-stage", curation.graph, curation.result,
		weave.WithGraphTaskOptions(weave.DependsOn(ingestionHandle)),
		weave.WithGraphTaskExecutorOptions(weave.WithDispatcher(weave.NewWorkerPoolDispatcher(3))),
	)

	processing := buildProcessingGraph(rng)
	processingHandle := mustGraphTask(workflow, "processing-stage", processing.graph, processing.result,
		weave.WithGraphTaskOptions(weave.DependsOn(ingestionHandle)),
		weave.WithGraphTaskExecutorOptions(weave.WithDispatcher(weave.NewWorkerPoolDispatcher(4))),
	)

	qualityHandle := mustTask(workflow, "quality-stage", func(ctx context.Context, deps weave.DependencyResolver) (qualityReport, error) {
		ing, err := ingestionHandle.Value(deps)
		if err != nil {
			return qualityReport{}, err
		}
		proc, err := processingHandle.Value(deps)
		if err != nil {
			return qualityReport{}, err
		}
		quality := buildQualityGraph(ing, proc)
		res, _, err := quality.graph.Run(ctx)
		if err != nil {
			return qualityReport{}, err
		}
		return quality.summary.Value(res)
	}, weave.DependsOn(ingestionHandle, processingHandle))

	distribution := buildDistributionGraph(rng)
	distributionHandle := mustGraphTask(workflow, "distribution-stage", distribution.graph, distribution.result,
		weave.WithGraphTaskOptions(weave.DependsOn(processingHandle, qualityHandle)),
		weave.WithGraphTaskExecutorOptions(weave.WithDispatcher(weave.NewWorkerPoolDispatcher(3))),
	)

	monitoring := buildMonitoringGraph(rng)
	monitoringHandle := mustGraphTask(workflow, "monitoring-stage", monitoring.graph, monitoring.result,
		weave.WithGraphTaskOptions(weave.DependsOn(distributionHandle, curationHandle)),
	)

	analyticsHandle := mustTask(workflow, "analytics-stage", func(ctx context.Context, deps weave.DependencyResolver) (analyticsReport, error) {
		ing, err := ingestionHandle.Value(deps)
		if err != nil {
			return analyticsReport{}, err
		}
		proc, err := processingHandle.Value(deps)
		if err != nil {
			return analyticsReport{}, err
		}
		dist, err := distributionHandle.Value(deps)
		if err != nil {
			return analyticsReport{}, err
		}
		cur, err := curationHandle.Value(deps)
		if err != nil {
			return analyticsReport{}, err
		}
		qual, err := qualityHandle.Value(deps)
		if err != nil {
			return analyticsReport{}, err
		}
		mon, err := monitoringHandle.Value(deps)
		if err != nil {
			return analyticsReport{}, err
		}
		analytics := buildAnalyticsGraph(ing, proc, dist, cur, qual, mon)
		res, _, err := analytics.graph.Run(ctx)
		if err != nil {
			return analyticsReport{}, err
		}
		return analytics.summary.Value(res)
	}, weave.DependsOn(ingestionHandle, processingHandle, distributionHandle, curationHandle, qualityHandle, monitoringHandle))

	summaryTask := mustTask(workflow, "prepare-summary", func(ctx context.Context, deps weave.DependencyResolver) (workflowSummary, error) {
		day, err := configTask.Value(deps)
		if err != nil {
			return workflowSummary{}, err
		}
		ing, err := ingestionHandle.Value(deps)
		if err != nil {
			return workflowSummary{}, err
		}
		proc, err := processingHandle.Value(deps)
		if err != nil {
			return workflowSummary{}, err
		}
		cur, err := curationHandle.Value(deps)
		if err != nil {
			return workflowSummary{}, err
		}
		qual, err := qualityHandle.Value(deps)
		if err != nil {
			return workflowSummary{}, err
		}
		dist, err := distributionHandle.Value(deps)
		if err != nil {
			return workflowSummary{}, err
		}
		mon, err := monitoringHandle.Value(deps)
		if err != nil {
			return workflowSummary{}, err
		}
		an, err := analyticsHandle.Value(deps)
		if err != nil {
			return workflowSummary{}, err
		}
		return workflowSummary{
			Day:        day,
			Ingestion:  ing,
			Processing: proc,
			Curation:   cur,
			Quality:    qual,
			Delivery:   dist,
			Monitoring: mon,
			Analytics:  an,
		}, nil
	}, weave.DependsOn(configTask, ingestionHandle, processingHandle, curationHandle, qualityHandle, distributionHandle, monitoringHandle, analyticsHandle))

	results, metrics, err := workflow.Run(context.Background(), weave.WithGlobalHooks(weave.Hooks{
		OnStart: func(ctx context.Context, event weave.TaskEvent) {
			log.Printf("[hook:start] %s deps=%v", event.Metadata.Name, event.Metadata.Dependencies)
		},
		OnFinish: func(ctx context.Context, event weave.TaskEvent) {
			log.Printf("[hook:done ] %s status=%s duration=%s", event.Metadata.Name, event.Metrics.Status, event.Metrics.Duration)
		},
	}))
	if err != nil {
		log.Fatalf("workflow run: %v", err)
	}

	final, err := summaryTask.Value(results)
	if err != nil {
		log.Fatalf("summary: %v", err)
	}

	fmt.Println()
	fmt.Printf("=== Daily Workflow Summary (%s) ===\n", final.Day)
	fmt.Printf("Sources: %d, Items: %d, Warnings: %v\n", final.Ingestion.SourcesDiscovered, final.Ingestion.ItemsCollected, final.Ingestion.Warnings)
	fmt.Printf("Videos: %v\n", final.Processing.Videos.StreamsGenerated)
	fmt.Printf("Articles: %d topics=%v\n", final.Processing.Articles.Stories, final.Processing.Articles.Topics)
	fmt.Printf("Images: %d variants=%v\n", final.Processing.Images.Assets, final.Processing.Images.Sizes)
	fmt.Printf("Curation collections: %d featured=%v warnings=%v\n", final.Curation.Collections, final.Curation.Featured, final.Curation.Warnings)
	fmt.Printf("Quality score: %.2f failures=%v\n", final.Quality.Score, final.Quality.Failures)
	fmt.Printf("Distribution regions: %v, alerts: %v\n", final.Delivery.RegionsPublished, final.Delivery.Alerts)
	fmt.Printf("Monitoring dashboards ready: %t incidents=%v\n", final.Monitoring.DashboardsReady, final.Monitoring.Incidents)
	fmt.Printf("Analytics score: %.2f highlights=%v warnings=%v\n", final.Analytics.Score, final.Analytics.Highlights, final.Analytics.Warnings)
	fmt.Printf("\nSucceeded tasks: %d/%d, max concurrency: %d\n", metrics.TasksSucceeded, metrics.TasksTotal, metrics.MaxConcurrency)
}
