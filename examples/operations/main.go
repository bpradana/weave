package main

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/bpradana/weave"
)

type reportConfig struct {
	ReportDate time.Time
	Store      string
	FailFast   bool
}

type order struct {
	ID     string
	UserID string
	Total  float64
}

type returnRecord struct {
	OrderID string
	Reason  string
}

type inventoryItem struct {
	SKU      string
	InStock  int
	Reserved int
}

type financials struct {
	Revenue        float64
	Orders         int
	AverageOrder   float64
	ReturnRate     float64
	SevereAnomaly  bool
	AnomalyReasons []string
}

type dashboard struct {
	Headline string
	Widgets  []string
}

type metricCollector struct {
	mu      sync.Mutex
	metrics map[string]weave.TaskMetrics
}

func newMetricCollector() *metricCollector {
	return &metricCollector{
		metrics: make(map[string]weave.TaskMetrics),
	}
}

func (c *metricCollector) Record(event weave.TaskEvent) {
	c.mu.Lock()
	c.metrics[event.Metadata.Name] = event.Metrics
	c.mu.Unlock()
}

func (c *metricCollector) PrintSummary() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.metrics) == 0 {
		fmt.Println("no task metrics captured")
		return
	}
	fmt.Println("\nTask metrics snapshot:")
	names := make([]string, 0, len(c.metrics))
	for name := range c.metrics {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		mt := c.metrics[name]
		fmt.Printf(" - %-20s status=%-9s duration=%-12s concurrency=%d\n",
			name,
			mt.Status,
			mt.Duration,
			mt.Concurrency,
		)
	}
}

func main() {
	graph := weave.NewGraph()
	collector := newMetricCollector()

	globalHooks := weave.Hooks{
		OnStart: func(ctx context.Context, evt weave.TaskEvent) {
			log.Printf("[START] %-20s deps=%v", evt.Metadata.Name, evt.Metadata.Dependencies)
		},
		OnSuccess: func(ctx context.Context, evt weave.TaskEvent) {
			log.Printf("[OK   ] %-20s", evt.Metadata.Name)
		},
		OnFailure: func(ctx context.Context, evt weave.TaskEvent) {
			log.Printf("[FAIL ] %-20s err=%v", evt.Metadata.Name, evt.Metrics.Error)
		},
		OnFinish: func(ctx context.Context, evt weave.TaskEvent) {
			collector.Record(evt)
		},
	}

	configTask, err := weave.AddTask(graph, "load-config", func(ctx context.Context, deps weave.DependencyResolver) (reportConfig, error) {
		time.Sleep(20 * time.Millisecond)
		return reportConfig{
			ReportDate: time.Now().UTC(),
			Store:      "weave-home-goods",
			FailFast:   false,
		}, nil
	})
	fatalOnErr("register load-config", err)

	orderTask, err := weave.AddTask(graph, "fetch-orders", func(ctx context.Context, deps weave.DependencyResolver) ([]order, error) {
		cfg, err := configTask.Value(deps)
		if err != nil {
			return nil, err
		}
		_ = cfg // pretend to use config to scope query
		time.Sleep(65 * time.Millisecond)
		return []order{
			{ID: "o-1001", UserID: "u-1", Total: 420},
			{ID: "o-1002", UserID: "u-2", Total: 180},
			{ID: "o-1003", UserID: "u-3", Total: 980},
			{ID: "o-1004", UserID: "u-1", Total: 220},
			{ID: "o-1005", UserID: "u-4", Total: 140},
		}, nil
	}, weave.DependsOn(configTask))
	fatalOnErr("register fetch-orders", err)

	returnTask, err := weave.AddTask(graph, "fetch-returns", func(ctx context.Context, deps weave.DependencyResolver) ([]returnRecord, error) {
		time.Sleep(45 * time.Millisecond)
		return []returnRecord{
			{OrderID: "o-1002", Reason: "damaged"},
			{OrderID: "o-1005", Reason: "wrong-size"},
		}, nil
	}, weave.DependsOn(configTask))
	fatalOnErr("register fetch-returns", err)

	inventoryTask, err := weave.AddTask(graph, "fetch-inventory", func(ctx context.Context, deps weave.DependencyResolver) ([]inventoryItem, error) {
		time.Sleep(30 * time.Millisecond)
		return []inventoryItem{
			{SKU: "sku-ax1", InStock: 12, Reserved: 5},
			{SKU: "sku-bb1", InStock: 4, Reserved: 1},
			{SKU: "sku-cc3", InStock: 45, Reserved: 18},
		}, nil
	}, weave.DependsOn(configTask))
	fatalOnErr("register fetch-inventory", err)

	cleanOrdersTask, err := weave.AddTask(graph, "clean-orders", func(ctx context.Context, deps weave.DependencyResolver) ([]order, error) {
		orders, err := orderTask.Value(deps)
		if err != nil {
			return nil, err
		}
		time.Sleep(30 * time.Millisecond)
		cleaned := make([]order, len(orders))
		copy(cleaned, orders)
		sort.SliceStable(cleaned, func(i, j int) bool {
			return cleaned[i].Total > cleaned[j].Total
		})
		return cleaned, nil
	}, weave.DependsOn(orderTask))
	fatalOnErr("register clean-orders", err)

	revenueTask, err := weave.AddTask(graph, "reconcile-financials", func(ctx context.Context, deps weave.DependencyResolver) (financials, error) {
		orders, err := cleanOrdersTask.Value(deps)
		if err != nil {
			return financials{}, err
		}
		returns, err := returnTask.Value(deps)
		if err != nil {
			return financials{}, err
		}
		time.Sleep(40 * time.Millisecond)
		var revenue float64
		for _, ord := range orders {
			revenue += ord.Total
		}
		fin := financials{
			Revenue:      revenue,
			Orders:       len(orders),
			AverageOrder: revenue / float64(len(orders)),
			ReturnRate:   float64(len(returns)) / float64(len(orders)),
		}
		return fin, nil
	}, weave.DependsOn(cleanOrdersTask, returnTask))
	fatalOnErr("register reconcile-financials", err)

	returnRateTask, err := weave.AddTask(graph, "compute-return-rate", func(ctx context.Context, deps weave.DependencyResolver) (float64, error) {
		fin, err := revenueTask.Value(deps)
		if err != nil {
			return 0, err
		}
		time.Sleep(25 * time.Millisecond)
		return fin.ReturnRate, nil
	}, weave.DependsOn(revenueTask))
	fatalOnErr("register compute-return-rate", err)

	anomalyTask, err := weave.AddTask(graph, "detect-anomalies", func(ctx context.Context, deps weave.DependencyResolver) (financials, error) {
		fin, err := revenueTask.Value(deps)
		if err != nil {
			return financials{}, err
		}
		returnRate, err := returnRateTask.Value(deps)
		if err != nil {
			return financials{}, err
		}
		time.Sleep(35 * time.Millisecond)
		fin.ReturnRate = returnRate
		if fin.Revenue < 2000 || fin.ReturnRate > 0.25 {
			fin.SevereAnomaly = true
			fin.AnomalyReasons = []string{
				fmt.Sprintf("revenue is low: $%.2f", fin.Revenue),
				fmt.Sprintf("return rate high: %.0f%%", fin.ReturnRate*100),
			}
			return fin, fmt.Errorf("detected severe anomaly: %v", fin.AnomalyReasons)
		}
		return fin, nil
	}, weave.DependsOn(revenueTask, returnRateTask))
	fatalOnErr("register detect-anomalies", err)

	notifyTask, err := weave.AddTask(graph, "notify-ops", func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		fin, err := anomalyTask.Value(deps)
		if err != nil {
			return "", err
		}
		time.Sleep(20 * time.Millisecond)
		return fmt.Sprintf("notified ops about anomalies: %+v", fin.AnomalyReasons), nil
	}, weave.DependsOn(anomalyTask))
	fatalOnErr("register notify-ops", err)

	stockTask, err := weave.AddTask(graph, "stock-alerts", func(ctx context.Context, deps weave.DependencyResolver) ([]string, error) {
		items, err := inventoryTask.Value(deps)
		if err != nil {
			return nil, err
		}
		time.Sleep(30 * time.Millisecond)
		alerts := make([]string, 0)
		for _, item := range items {
			if item.InStock-item.Reserved < 5 {
				alerts = append(alerts, fmt.Sprintf("%s low stock (%d available)", item.SKU, item.InStock-item.Reserved))
			}
		}
		return alerts, nil
	}, weave.DependsOn(inventoryTask))
	fatalOnErr("register stock-alerts", err)

	dashboardTask, err := weave.AddTask(graph, "generate-dashboard", func(ctx context.Context, deps weave.DependencyResolver) (dashboard, error) {
		cfg, err := configTask.Value(deps)
		if err != nil {
			return dashboard{}, err
		}
		fin, err := revenueTask.Value(deps)
		if err != nil {
			return dashboard{}, err
		}
		returnRate, err := returnRateTask.Value(deps)
		if err != nil {
			return dashboard{}, err
		}
		alerts, err := stockTask.Value(deps)
		if err != nil {
			return dashboard{}, err
		}
		time.Sleep(50 * time.Millisecond)
		return dashboard{
			Headline: fmt.Sprintf("Daily Ops – %s", cfg.ReportDate.Format("2006-01-02")),
			Widgets: []string{
				fmt.Sprintf("Revenue: $%.2f", fin.Revenue),
				fmt.Sprintf("Orders: %d", fin.Orders),
				fmt.Sprintf("Average order: $%.2f", fin.AverageOrder),
				fmt.Sprintf("Return rate: %.1f%%", returnRate*100),
				fmt.Sprintf("Stock alerts: %d", len(alerts)),
			},
		}, nil
	}, weave.DependsOn(configTask, revenueTask, returnRateTask, stockTask))
	fatalOnErr("register generate-dashboard", err)

	publishTask, err := weave.AddTask(graph, "publish-dashboard", func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		dash, err := dashboardTask.Value(deps)
		if err != nil {
			return "", err
		}
		time.Sleep(30 * time.Millisecond)
		return fmt.Sprintf("dashboard published: %s (%d widgets)", dash.Headline, len(dash.Widgets)), nil
	}, weave.DependsOn(dashboardTask))
	fatalOnErr("register publish-dashboard", err)

	cacheTask, err := weave.AddTask(graph, "refresh-cache", func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		_, err := publishTask.Value(deps)
		if err != nil {
			return "", err
		}
		time.Sleep(15 * time.Millisecond)
		return "dashboard cache refreshed", nil
	}, weave.DependsOn(publishTask))
	fatalOnErr("register refresh-cache", err)

	indexTask, err := weave.AddTask(graph, "sync-search-index", func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		_, err := publishTask.Value(deps)
		if err != nil {
			return "", err
		}
		time.Sleep(25 * time.Millisecond)
		return "search index updated for dashboard", nil
	}, weave.DependsOn(publishTask))
	fatalOnErr("register sync-search-index", err)

	archiveTask, err := weave.AddTask(graph, "archive-datasets", func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		orders, err := cleanOrdersTask.Value(deps)
		if err != nil {
			return "", err
		}
		returns, err := returnTask.Value(deps)
		if err != nil {
			return "", err
		}
		time.Sleep(30 * time.Millisecond)
		return fmt.Sprintf("archived %d orders and %d returns", len(orders), len(returns)), nil
	}, weave.DependsOn(cleanOrdersTask, returnTask))
	fatalOnErr("register archive-datasets", err)

	metricsTask, err := weave.AddTask(graph, "publish-metrics", func(ctx context.Context, deps weave.DependencyResolver) (map[string]float64, error) {
		fin, err := revenueTask.Value(deps)
		if err != nil {
			return nil, err
		}
		returnRate, err := returnRateTask.Value(deps)
		if err != nil {
			return nil, err
		}
		time.Sleep(20 * time.Millisecond)
		return map[string]float64{
			"revenue":      fin.Revenue,
			"orders":       float64(fin.Orders),
			"avg_order":    fin.AverageOrder,
			"return_rate":  returnRate,
			"anomaly_flag": boolToFloat(fin.SevereAnomaly),
		}, nil
	}, weave.DependsOn(revenueTask, returnRateTask))
	fatalOnErr("register publish-metrics", err)

	exec := graph.Start(context.Background(),
		weave.WithDispatcher(weave.NewWorkerPoolDispatcher(4)),
		weave.WithErrorStrategy(weave.ContinueOnError),
		weave.WithGlobalHooks(globalHooks),
	)

	results, execMetrics, err := exec.Await()
	if err != nil {
		log.Printf("execution finished with error: %v", err)
	}

	publishValue, publishErr := publishTask.Value(results)
	printResult("publish-dashboard", publishValue, publishErr)

	cacheValue, cacheErr := cacheTask.Value(results)
	printResult("refresh-cache", cacheValue, cacheErr)

	indexValue, indexErr := indexTask.Value(results)
	printResult("sync-search-index", indexValue, indexErr)

	archiveValue, archiveErr := archiveTask.Value(results)
	printResult("archive-datasets", archiveValue, archiveErr)

	metricsValue, metricsErr := metricsTask.Value(results)
	printResult("publish-metrics", metricsValue, metricsErr)

	status := results.Status(notifyTask)
	log.Printf("notify-ops status: %s", status)
	switch status {
	case weave.StatusSucceeded:
		notifyValue, notifyErr := notifyTask.Value(results)
		printResult("notify-ops", notifyValue, notifyErr)
	case weave.StatusSkipped:
		log.Printf("notify-ops skipped because anomalies task failed")
	}

	collector.PrintSummary()

	fmt.Printf("\nExecution metrics: succeeded=%d failed=%d skipped=%d total=%d maxConcurrency=%d duration=%s\n",
		execMetrics.TasksSucceeded,
		execMetrics.TasksFailed,
		execMetrics.TasksSkipped,
		execMetrics.TasksTotal,
		execMetrics.MaxConcurrency,
		execMetrics.Duration.Round(time.Millisecond),
	)
}

func printResult[T any](label string, value T, err error) {
	if err != nil {
		log.Printf("%s error: %v", label, err)
		return
	}
	log.Printf("%s result: %v", label, value)
}

func fatalOnErr(context string, err error) {
	if err != nil {
		log.Fatalf("%s: %v", context, err)
	}
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
