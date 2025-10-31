package main

import (
	"context"
	"math/rand"
	"time"

	"github.com/bpradana/weave"
)

type imageGraph struct {
	graph  *weave.Graph
	bundle *weave.Handle[imageBundle]
}

func buildImageGraph(rng *rand.Rand) imageGraph {
	g := weave.NewGraph()

	fetch := mustTask(g, "fetch-image-assets", func(ctx context.Context, deps weave.DependencyResolver) ([]string, error) {
		time.Sleep(20 * time.Millisecond)
		return []string{"hero", "thumb", "banner"}, nil
	})

	dedupe := mustTask(g, "dedupe-images", func(ctx context.Context, deps weave.DependencyResolver) ([]string, error) {
		assets, err := fetch.Value(deps)
		if err != nil {
			return nil, err
		}
		time.Sleep(15 * time.Millisecond)
		return assets, nil
	}, weave.DependsOn(fetch))

	variant := buildImageVariantGraph(rng)
	variantHandle := mustGraphTask(g, "image-variants", variant.graph, variant.result,
		weave.WithGraphTaskOptions(weave.DependsOn(dedupe)),
	)

	compress := mustTask(g, "compress-images", func(ctx context.Context, deps weave.DependencyResolver) (int, error) {
		time.Sleep(12 * time.Millisecond)
		return 3, nil
	}, weave.DependsOn(variantHandle))

	publish := mustTask(g, "publish-image-bundle", func(ctx context.Context, deps weave.DependencyResolver) (imageBundle, error) {
		variants, err := variantHandle.Value(deps)
		if err != nil {
			return imageBundle{}, err
		}
		_, err = compress.Value(deps)
		if err != nil {
			return imageBundle{}, err
		}
		return imageBundle{
			Assets: len(variants.Derivatives),
			Sizes:  variants.Derivatives,
		}, nil
	}, weave.DependsOn(variantHandle, compress))

	return imageGraph{
		graph:  g,
		bundle: publish,
	}
}

func (ig imageGraph) result(ctx context.Context, results *weave.Results, metrics weave.ExecutionMetrics, runErr error) (imageBundle, error) {
	if runErr != nil {
		return imageBundle{}, runErr
	}
	return ig.bundle.Value(results)
}

type imageVariantGraph struct {
	graph  *weave.Graph
	bundle *weave.Handle[variantBundle]
}

func buildImageVariantGraph(rng *rand.Rand) imageVariantGraph {
	g := weave.NewGraph()

	small := mustTask(g, "variant-small", func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		time.Sleep(10 * time.Millisecond)
		return "400w", nil
	})

	medium := mustTask(g, "variant-medium", func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		time.Sleep(10 * time.Millisecond)
		return "800w", nil
	}, weave.DependsOn(small))

	large := mustTask(g, "variant-large", func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		time.Sleep(10 * time.Millisecond)
		return "1600w", nil
	}, weave.DependsOn(medium))

	cdn := mustTask(g, "upload-variants", func(ctx context.Context, deps weave.DependencyResolver) (variantBundle, error) {
		l, err := large.Value(deps)
		if err != nil {
			return variantBundle{}, err
		}
		return variantBundle{
			Derivatives: []string{"400w", "800w", l},
		}, nil
	}, weave.DependsOn(large))

	return imageVariantGraph{
		graph:  g,
		bundle: cdn,
	}
}

func (vg imageVariantGraph) result(ctx context.Context, results *weave.Results, metrics weave.ExecutionMetrics, runErr error) (variantBundle, error) {
	if runErr != nil {
		return variantBundle{}, runErr
	}
	return vg.bundle.Value(results)
}
