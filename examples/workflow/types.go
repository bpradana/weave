package main

import "time"

type ingestionReport struct {
	SourcesDiscovered int
	ItemsCollected    int
	Warnings          []string
	Normalized        bool
}

type videoBundle struct {
	StreamsGenerated []string
	ThumbnailCount   int
	Duration         time.Duration
}

type thumbnailSet struct {
	Count      int
	SpritePath string
}

type variantResult struct {
	Generated int
}

type articleBundle struct {
	Stories int
	Topics  []string
	Summary string
}

type imageBundle struct {
	Assets int
	Sizes  []string
}

type variantBundle struct {
	Derivatives []string
}

type curationReport struct {
	Collections int
	Featured    []string
	Tags        []string
	Warnings    []string
}

type processingReport struct {
	Videos   videoBundle
	Articles articleBundle
	Images   imageBundle
	Checks   []string
}

type qualityReport struct {
	Score    float64
	Failures []string
}

type distributionReport struct {
	RegionsPublished []string
	EdgeWarmups      int
	Alerts           []string
}

type monitoringReport struct {
	DashboardsReady bool
	Incidents       []string
	SignalsTracked  []string
}

type analyticsReport struct {
	Highlights []string
	Score      float64
	Warnings   []string
}

type workflowSummary struct {
	Day        string
	Ingestion  ingestionReport
	Processing processingReport
	Curation   curationReport
	Quality    qualityReport
	Delivery   distributionReport
	Monitoring monitoringReport
	Analytics  analyticsReport
}
