package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/bpradana/weave"
)

type videoGraph struct {
	graph  *weave.Graph
	bundle *weave.Handle[videoBundle]
}

func buildVideoGraph(rng *rand.Rand) videoGraph {
	g := weave.NewGraph()

	loadManifest := mustTask(g, "load-source-manifest", func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		time.Sleep(20 * time.Millisecond)
		return "s3://bucket/master.mxf", nil
	})

	probeStreams := mustTask(g, "probe-streams", func(ctx context.Context, deps weave.DependencyResolver) (int, error) {
		time.Sleep(25 * time.Millisecond)
		return 3, nil
	}, weave.DependsOn(loadManifest))

	transcode1080 := mustTask(g, "transcode-1080p", func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		time.Sleep(40 * time.Millisecond)
		return "1080p", nil
	}, weave.DependsOn(probeStreams))

	transcode720 := mustTask(g, "transcode-720p", func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		time.Sleep(35 * time.Millisecond)
		return "720p", nil
	}, weave.DependsOn(probeStreams))

	normalizeAudio := mustTask(g, "normalize-audio", func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		time.Sleep(28 * time.Millisecond)
		return "stereo", nil
	}, weave.DependsOn(probeStreams))

	packageStreams := mustTask(g, "package-hls", func(ctx context.Context, deps weave.DependencyResolver) ([]string, error) {
		hd, err := transcode1080.Value(deps)
		if err != nil {
			return nil, err
		}
		sd, err := transcode720.Value(deps)
		if err != nil {
			return nil, err
		}
		audio, err := normalizeAudio.Value(deps)
		if err != nil {
			return nil, err
		}
		time.Sleep(30 * time.Millisecond)
		return []string{hd, sd, audio}, nil
	}, weave.DependsOn(transcode1080, transcode720, normalizeAudio))

	thumbs := buildThumbnailGraph()
	thumbHandle := mustGraphTask(g, "thumbnail-pipeline", thumbs.graph, thumbs.result,
		weave.WithGraphTaskOptions(weave.DependsOn(packageStreams)),
	)

	bundle := mustTask(g, "video-bundle", func(ctx context.Context, deps weave.DependencyResolver) (videoBundle, error) {
		streams, err := packageStreams.Value(deps)
		if err != nil {
			return videoBundle{}, err
		}
		thumbnails, err := thumbHandle.Value(deps)
		if err != nil {
			return videoBundle{}, err
		}
		return videoBundle{
			StreamsGenerated: streams,
			ThumbnailCount:   thumbnails.Count,
			Duration:         90 * time.Minute,
		}, nil
	}, weave.DependsOn(packageStreams, thumbHandle))

	return videoGraph{
		graph:  g,
		bundle: bundle,
	}
}

func (vg videoGraph) result(ctx context.Context, results *weave.Results, metrics weave.ExecutionMetrics, runErr error) (videoBundle, error) {
	if runErr != nil {
		return videoBundle{}, runErr
	}
	return vg.bundle.Value(results)
}

type thumbnailGraph struct {
	graph   *weave.Graph
	summary *weave.Handle[thumbnailSet]
}

func buildThumbnailGraph() thumbnailGraph {
	g := weave.NewGraph()

	keyframes := mustTask(g, "capture-keyframes", func(ctx context.Context, deps weave.DependencyResolver) (int, error) {
		time.Sleep(22 * time.Millisecond)
		return 12, nil
	})

	enhanced := mustTask(g, "enhance-keyframes", func(ctx context.Context, deps weave.DependencyResolver) (int, error) {
		count, err := keyframes.Value(deps)
		if err != nil {
			return 0, err
		}
		time.Sleep(18 * time.Millisecond)
		return count, nil
	}, weave.DependsOn(keyframes))

	variantGraph := buildThumbnailVariantGraph()
	variantHandle := mustGraphTask(g, "thumbnail-variants", variantGraph.graph, variantGraph.result,
		weave.WithGraphTaskOptions(weave.DependsOn(enhanced)),
	)

	spriteGraph := buildSpriteGraph()
	spriteHandle := mustGraphTask(g, "sprite-sheet", spriteGraph.graph, spriteGraph.result,
		weave.WithGraphTaskOptions(weave.DependsOn(variantHandle)),
	)

	summary := mustTask(g, "thumbnail-result", func(ctx context.Context, deps weave.DependencyResolver) (thumbnailSet, error) {
		variants, err := variantHandle.Value(deps)
		if err != nil {
			return thumbnailSet{}, err
		}
		sprite, err := spriteHandle.Value(deps)
		if err != nil {
			return thumbnailSet{}, err
		}
		return thumbnailSet{
			Count:      variants.Generated,
			SpritePath: sprite,
		}, nil
	}, weave.DependsOn(variantHandle, spriteHandle))

	return thumbnailGraph{
		graph:   g,
		summary: summary,
	}
}

func (tg thumbnailGraph) result(ctx context.Context, results *weave.Results, metrics weave.ExecutionMetrics, runErr error) (thumbnailSet, error) {
	if runErr != nil {
		return thumbnailSet{}, runErr
	}
	return tg.summary.Value(results)
}

type variantGraph struct {
	graph   *weave.Graph
	summary *weave.Handle[variantResult]
}

func buildThumbnailVariantGraph() variantGraph {
	g := weave.NewGraph()

	resizeSmall := mustTask(g, "resize-small", func(ctx context.Context, deps weave.DependencyResolver) (int, error) {
		time.Sleep(12 * time.Millisecond)
		return 12, nil
	})

	resizeMedium := mustTask(g, "resize-medium", func(ctx context.Context, deps weave.DependencyResolver) (int, error) {
		time.Sleep(12 * time.Millisecond)
		return 12, nil
	}, weave.DependsOn(resizeSmall))

	resizeLarge := mustTask(g, "resize-large", func(ctx context.Context, deps weave.DependencyResolver) (int, error) {
		time.Sleep(12 * time.Millisecond)
		return 12, nil
	}, weave.DependsOn(resizeMedium))

	publish := mustTask(g, "publish-variants", func(ctx context.Context, deps weave.DependencyResolver) (variantResult, error) {
		l, err := resizeLarge.Value(deps)
		if err != nil {
			return variantResult{}, err
		}
		return variantResult{Generated: l}, nil
	}, weave.DependsOn(resizeLarge))

	return variantGraph{
		graph:   g,
		summary: publish,
	}
}

func (vg variantGraph) result(ctx context.Context, results *weave.Results, metrics weave.ExecutionMetrics, runErr error) (variantResult, error) {
	if runErr != nil {
		return variantResult{}, runErr
	}
	return vg.summary.Value(results)
}

type spriteGraph struct {
	graph *weave.Graph
	path  *weave.Handle[string]
}

func buildSpriteGraph() spriteGraph {
	g := weave.NewGraph()

	collectFrames := mustTask(g, "collect-frames", func(ctx context.Context, deps weave.DependencyResolver) (int, error) {
		time.Sleep(10 * time.Millisecond)
		return 8, nil
	})

	layout := mustTask(g, "layout-sprite", func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		frames, err := collectFrames.Value(deps)
		if err != nil {
			return "", err
		}
		time.Sleep(14 * time.Millisecond)
		return fmt.Sprintf("sprite-%d.png", frames), nil
	}, weave.DependsOn(collectFrames))

	compress := mustTask(g, "compress-sprite", func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		path, err := layout.Value(deps)
		if err != nil {
			return "", err
		}
		time.Sleep(8 * time.Millisecond)
		return "s3://thumbnails/" + path, nil
	}, weave.DependsOn(layout))

	return spriteGraph{
		graph: g,
		path:  compress,
	}
}

func (sg spriteGraph) result(ctx context.Context, results *weave.Results, metrics weave.ExecutionMetrics, runErr error) (string, error) {
	if runErr != nil {
		return "", runErr
	}
	return sg.path.Value(results)
}
