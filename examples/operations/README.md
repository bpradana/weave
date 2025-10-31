# Operations Example

This end-to-end scenario models a daily operations report for an online retailer. It pulls transactional data, reconciles revenue, evaluates inventory, generates dashboards, and reacts to anomalies. The graph uses custom hooks, rich metrics, a worker-pool dispatcher, and the `ContinueOnError` strategy so that downstream tasks keep running even when anomaly detection fails.

## Highlights
- **Global hooks** log every lifecycle event and feed a custom metric collector.
- **Worker pool dispatcher** (`weave.NewWorkerPoolDispatcher(4)`) keeps concurrency bounded while still parallelising independent branches.
- **Error strategy** (`weave.ContinueOnError`) lets publishing and archiving steps finish even if anomaly detection triggers a failure.
- **Metrics reporting** prints per-task durations, statuses, and aggregate execution stats at the end.

## Run it

```shell
go run .
```

## Task graph

```mermaid
flowchart LR
    load_config["load-config"]
    fetch_orders["fetch-orders"]
    fetch_returns["fetch-returns"]
    fetch_inventory["fetch-inventory"]
    clean_orders["clean-orders"]
    reconcile_financials["reconcile-financials"]
    compute_return_rate["compute-return-rate"]
    detect_anomalies["detect-anomalies"]
    notify_ops["notify-ops"]
    stock_alerts["stock-alerts"]
    generate_dashboard["generate-dashboard"]
    publish_dashboard["publish-dashboard"]
    refresh_cache["refresh-cache"]
    sync_search_index["sync-search-index"]
    archive_datasets["archive-datasets"]
    publish_metrics["publish-metrics"]

    load_config --> fetch_orders --> clean_orders --> reconcile_financials --> compute_return_rate --> detect_anomalies --> notify_ops
    load_config --> fetch_returns --> reconcile_financials
    load_config --> fetch_inventory --> stock_alerts --> generate_dashboard
    load_config --> generate_dashboard
    reconcile_financials --> generate_dashboard
    compute_return_rate --> generate_dashboard
    generate_dashboard --> publish_dashboard --> refresh_cache
    publish_dashboard --> sync_search_index
    clean_orders --> archive_datasets
    fetch_returns --> archive_datasets
    reconcile_financials --> publish_metrics
    compute_return_rate --> publish_metrics
```
