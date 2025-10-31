package main

import (
	"context"
	"math/rand"
	"time"

	"github.com/bpradana/weave"
)

type distributionGraph struct {
	graph   *weave.Graph
	summary *weave.Handle[distributionReport]
}

func buildDistributionGraph(rng *rand.Rand) distributionGraph {
	g := weave.NewGraph()

	loadPlan := mustTask(g, "load-distribution-plan", func(ctx context.Context, deps weave.DependencyResolver) ([]string, error) {
		time.Sleep(20 * time.Millisecond)
		return []string{"us-east", "eu-west", "ap-south"}, nil
	})

	prewarm := mustTask(g, "prewarm-cdn", func(ctx context.Context, deps weave.DependencyResolver) (int, error) {
		time.Sleep(25 * time.Millisecond)
		return 24, nil
	}, weave.DependsOn(loadPlan))

	regional := buildRegionalDistributionGraph()
	regionalHandle := mustGraphTask(g, "regional-rollout", regional.graph, regional.result,
		weave.WithGraphTaskOptions(weave.DependsOn(loadPlan)),
	)

	alerts := mustTask(g, "check-alerts", func(ctx context.Context, deps weave.DependencyResolver) ([]string, error) {
		time.Sleep(15 * time.Millisecond)
		return []string{"cdn-cache-hit-rate:98%"}, nil
	}, weave.DependsOn(prewarm))

	summary := mustTask(g, "distribution-summary", func(ctx context.Context, deps weave.DependencyResolver) (distributionReport, error) {
		regions, err := regionalHandle.Value(deps)
		if err != nil {
			return distributionReport{}, err
		}
		pre, err := prewarm.Value(deps)
		if err != nil {
			return distributionReport{}, err
		}
		alertList, err := alerts.Value(deps)
		if err != nil {
			return distributionReport{}, err
		}
		return distributionReport{
			RegionsPublished: regions,
			EdgeWarmups:      pre,
			Alerts:           alertList,
		}, nil
	}, weave.DependsOn(regionalHandle, prewarm, alerts))

	return distributionGraph{
		graph:   g,
		summary: summary,
	}
}

func (dg distributionGraph) result(ctx context.Context, results *weave.Results, metrics weave.ExecutionMetrics, runErr error) (distributionReport, error) {
	if runErr != nil {
		return distributionReport{}, runErr
	}
	return dg.summary.Value(results)
}

type regionalGraph struct {
	graph   *weave.Graph
	regions *weave.Handle[[]string]
}

func buildRegionalDistributionGraph() regionalGraph {
	g := weave.NewGraph()

	us := buildRegionRolloutGraph("us-east")
	usHandle := mustGraphTask(g, "rollout-us-east", us.graph, us.result)

	eu := buildRegionRolloutGraph("eu-west")
	euHandle := mustGraphTask(g, "rollout-eu-west", eu.graph, eu.result,
		weave.WithGraphTaskOptions(weave.DependsOn(usHandle)),
	)

	ap := buildRegionRolloutGraph("ap-south")
	apHandle := mustGraphTask(g, "rollout-ap-south", ap.graph, ap.result,
		weave.WithGraphTaskOptions(weave.DependsOn(euHandle)),
	)

	regions := mustTask(g, "regional-result", func(ctx context.Context, deps weave.DependencyResolver) ([]string, error) {
		usRegions, err := usHandle.Value(deps)
		if err != nil {
			return nil, err
		}
		euRegions, err := euHandle.Value(deps)
		if err != nil {
			return nil, err
		}
		apRegions, err := apHandle.Value(deps)
		if err != nil {
			return nil, err
		}
		return append(append(usRegions, euRegions...), apRegions...), nil
	}, weave.DependsOn(usHandle, euHandle, apHandle))

	return regionalGraph{
		graph:   g,
		regions: regions,
	}
}

func (rg regionalGraph) result(ctx context.Context, results *weave.Results, metrics weave.ExecutionMetrics, runErr error) ([]string, error) {
	if runErr != nil {
		return nil, runErr
	}
	return rg.regions.Value(results)
}

type regionRolloutGraph struct {
	graph   *weave.Graph
	regions *weave.Handle[[]string]
}

func buildRegionRolloutGraph(region string) regionRolloutGraph {
	g := weave.NewGraph()

	stage := mustTask(g, "stage-"+region, func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		time.Sleep(14 * time.Millisecond)
		return region + "-stage", nil
	})

	validate := mustTask(g, "validate-"+region, func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		_, err := stage.Value(deps)
		if err != nil {
			return "", err
		}
		time.Sleep(12 * time.Millisecond)
		return region + "-validated", nil
	}, weave.DependsOn(stage))

	publish := mustTask(g, "publish-"+region, func(ctx context.Context, deps weave.DependencyResolver) ([]string, error) {
		_, err := validate.Value(deps)
		if err != nil {
			return nil, err
		}
		time.Sleep(12 * time.Millisecond)
		return []string{region}, nil
	}, weave.DependsOn(validate))

	return regionRolloutGraph{
		graph:   g,
		regions: publish,
	}
}

func (rg regionRolloutGraph) result(ctx context.Context, results *weave.Results, metrics weave.ExecutionMetrics, runErr error) ([]string, error) {
	if runErr != nil {
		return nil, runErr
	}
	return rg.regions.Value(results)
}
