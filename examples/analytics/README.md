# Analytics Example

This scenario mimics a regional analytics job with multiple data sources and post-processing steps. It combines typed task results, hooks for logging, and a worker-pool dispatcher to orchestrate a richer DAG.

## How it works
- `load-config` seeds the pipeline with region and time range.
- `fetch-users` and `fetch-orders` depend on the config and simulate remote fetches.
- `total-revenue` aggregates the order totals, and `top-customers` correlates users with orders.
- `compile-report` gathers all previous outputs to produce a formatted report, while hooks log lifecycle events.

## Run it

```shell
go run .
```

## Task graph

```mermaid
flowchart LR
    load_config["load-config"]
    fetch_users["fetch-users"]
    fetch_orders["fetch-orders"]
    total_revenue["total-revenue"]
    top_customers["top-customers"]
    compile_report["compile-report"]

    load_config --> fetch_users
    load_config --> fetch_orders
    fetch_orders --> total_revenue
    fetch_users --> top_customers
    fetch_orders --> top_customers
    load_config --> compile_report
    total_revenue --> compile_report
    top_customers --> compile_report
```
