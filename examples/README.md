# Examples

This directory contains self-contained programs that demonstrate different features of `weave`, from the minimal task graph to observability and custom dispatchers. Each example can be run directly with `go run .` inside its folder.

- `basic` – minimal two-node graph that produces a greeting and prints execution metrics.
- `hooks` – shows global and per-task hooks that emit structured logs.
- `workerpool` – executes many independent tasks on a bounded worker pool dispatcher.
- `visualize` – exports the graph in Graphviz DOT format for documentation.
- `analytics` – orchestrates a richer DAG with custom structs, hooks, and worker pools.
