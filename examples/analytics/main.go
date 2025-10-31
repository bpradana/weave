package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/bpradana/weave"
)

type config struct {
	Region string
	Since  time.Time
}

type user struct {
	ID   string
	Name string
}

type order struct {
	UserID string
	Total  float64
}

func main() {
	graph := weave.NewGraph()

	logger := weave.Hooks{
		OnStart: func(ctx context.Context, event weave.TaskEvent) {
			log.Printf("[START] %-18s deps=%v", event.Metadata.Name, event.Metadata.Dependencies)
		},
		OnFinish: func(ctx context.Context, event weave.TaskEvent) {
			log.Printf("[DONE ] %-18s status=%s duration=%s", event.Metadata.Name, event.Metrics.Status, event.Metrics.Duration)
		},
	}

	configTask, err := weave.AddTask(graph, "load-config", func(ctx context.Context, deps weave.DependencyResolver) (config, error) {
		return config{
			Region: "us-east-1",
			Since:  time.Now().Add(-30 * 24 * time.Hour),
		}, nil
	})
	if err != nil {
		log.Fatalf("register load-config: %v", err)
	}

	userTask, err := weave.AddTask(graph, "fetch-users", func(ctx context.Context, deps weave.DependencyResolver) ([]user, error) {
		cfg, err := configTask.Value(deps)
		if err != nil {
			return nil, err
		}
		time.Sleep(80 * time.Millisecond) // simulate network call
		return mockUsers(cfg.Region), nil
	}, weave.DependsOn(configTask))
	if err != nil {
		log.Fatalf("register fetch-users: %v", err)
	}

	orderTask, err := weave.AddTask(graph, "fetch-orders", func(ctx context.Context, deps weave.DependencyResolver) ([]order, error) {
		cfg, err := configTask.Value(deps)
		if err != nil {
			return nil, err
		}
		time.Sleep(120 * time.Millisecond)
		return mockOrders(cfg), nil
	}, weave.DependsOn(configTask))
	if err != nil {
		log.Fatalf("register fetch-orders: %v", err)
	}

	revenueTask, err := weave.AddTask(graph, "total-revenue", func(ctx context.Context, deps weave.DependencyResolver) (float64, error) {
		orders, err := orderTask.Value(deps)
		if err != nil {
			return 0, err
		}
		var total float64
		for _, ord := range orders {
			total += ord.Total
		}
		return total, nil
	}, weave.DependsOn(orderTask))
	if err != nil {
		log.Fatalf("register total-revenue: %v", err)
	}

	topCustomerTask, err := weave.AddTask(graph, "top-customers", func(ctx context.Context, deps weave.DependencyResolver) ([]string, error) {
		users, err := userTask.Value(deps)
		if err != nil {
			return nil, err
		}
		orders, err := orderTask.Value(deps)
		if err != nil {
			return nil, err
		}
		return computeTopCustomers(users, orders, 3), nil
	}, weave.DependsOn(userTask, orderTask))
	if err != nil {
		log.Fatalf("register top-customers: %v", err)
	}

	reportTask, err := weave.AddTask(graph, "compile-report", func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		cfg, err := configTask.Value(deps)
		if err != nil {
			return "", err
		}
		totalRevenue, err := revenueTask.Value(deps)
		if err != nil {
			return "", err
		}
		topCustomers, err := topCustomerTask.Value(deps)
		if err != nil {
			return "", err
		}

		return fmt.Sprintf(
			"Region: %s\nSince: %s\nRevenue: $%.2f\nTop Customers: %s\n",
			cfg.Region,
			cfg.Since.Format("2006-01-02"),
			totalRevenue,
			strings.Join(topCustomers, ", "),
		), nil
	}, weave.DependsOn(configTask, revenueTask, topCustomerTask))
	if err != nil {
		log.Fatalf("register compile-report: %v", err)
	}

	exec := graph.Start(context.Background(),
		weave.WithDispatcher(weave.NewWorkerPoolDispatcher(4)),
		weave.WithGlobalHooks(logger),
	)

	results, metrics, err := exec.Await()
	if err != nil {
		log.Fatalf("await graph: %v", err)
	}

	report, err := reportTask.Value(results)
	if err != nil {
		log.Fatalf("report value: %v", err)
	}

	fmt.Println()
	fmt.Println("=== Regional Revenue Report ===")
	fmt.Println(report)
	fmt.Printf("Max concurrency observed: %d\n", metrics.MaxConcurrency)
}

func mockUsers(region string) []user {
	users := []user{
		{ID: "u-001", Name: "Ada Lovelace"},
		{ID: "u-002", Name: "Alan Turing"},
		{ID: "u-003", Name: "Grace Hopper"},
		{ID: "u-004", Name: "Katherine Johnson"},
	}
	if region == "us-west-2" {
		return users[:3]
	}
	return users
}

func mockOrders(cfg config) []order {
	rng := rand.New(rand.NewSource(cfg.Since.Unix()))
	ids := []string{"u-001", "u-002", "u-003", "u-004"}
	var orders []order
	for i := 0; i < 12; i++ {
		orders = append(orders, order{
			UserID: ids[rng.Intn(len(ids))],
			Total:  25 + rng.Float64()*200,
		})
	}
	return orders
}

func computeTopCustomers(users []user, orders []order, limit int) []string {
	userLookup := make(map[string]string, len(users))
	for _, u := range users {
		userLookup[u.ID] = u.Name
	}

	counts := make(map[string]int)
	for _, ord := range orders {
		counts[ord.UserID]++
	}

	type entry struct {
		UserID string
		Count  int
	}

	ranking := make([]entry, 0, len(counts))
	for id, count := range counts {
		ranking = append(ranking, entry{UserID: id, Count: count})
	}
	sort.Slice(ranking, func(i, j int) bool {
		if ranking[i].Count == ranking[j].Count {
			return ranking[i].UserID < ranking[j].UserID
		}
		return ranking[i].Count > ranking[j].Count
	})

	top := make([]string, 0, limit)
	for _, entry := range ranking {
		name := userLookup[entry.UserID]
		if name == "" {
			continue
		}
		top = append(top, fmt.Sprintf("%s (%d orders)", name, entry.Count))
		if len(top) >= limit {
			break
		}
	}
	return top
}
