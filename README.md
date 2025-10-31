# weave

Declarative, type-safe orchestration for dependent Go tasks.

`weave` lets you describe a graph of tasks as plain Go functions, specify their dependencies, and execute them concurrently with deterministic ordering, cancellation, error handling, hooks, and metrics.

## Features

- **Typed tasks** – each task returns a concrete Go type, accessed through strongly typed handles.
- **Dependency-aware scheduling** – the executor builds a DAG, validates it for missing edges or cycles, and runs tasks as soon as their prerequisites succeed.
- **Context propagation** – cancellation or timeouts on the root `context.Context` cascade through the entire graph.
- **Configurable error strategies** – choose fail-fast or continue-on-error semantics; dependants of failed tasks are skipped automatically.
- **Observability hooks** – register `OnStart`, `OnSuccess`, `OnFailure`, and `OnFinish` callbacks globally or per task.
- **Timing metrics** – collect per-task timings and aggregate execution metrics (durations, failure counts, max concurrency).
- **Pluggable dispatchers** – swap the default goroutine-per-task dispatcher for custom backends such as worker pools or rate limiters.

## Installation

```shell
go get github.com/bpradana/weave
```

## Quick start

```go
package main

import (
	"context"
	"fmt"

	"github.com/bpradana/weave"
)

func main() {
	graph := weave.NewGraph()

	userTask, _ := weave.AddTask(graph, "load-user", func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		return "Ada", nil
	})

	greetingTask, _ := weave.AddTask(graph, "greeting", func(ctx context.Context, deps weave.DependencyResolver) (string, error) {
		user, err := userTask.Value(deps)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("Hello, %s!", user), nil
	}, weave.DependsOn(userTask))

	results, metrics, err := graph.Run(context.Background())
	if err != nil {
		panic(err)
	}

	greeting, _ := greetingTask.Value(results)
	fmt.Println(greeting)                 // Hello, Ada!
	fmt.Println(metrics.TasksSucceeded)   // 2
}
```

## Error handling

Select how the executor reacts to failures:

- `weave.FailFast` (default) cancels outstanding work as soon as any task fails.
- `weave.ContinueOnError` keeps running independent tasks while skipping dependants of failed ones.

```go
results, _, err := graph.Run(ctx, weave.WithErrorStrategy(weave.ContinueOnError))
```

Each task's status and error are available through the shared `*weave.Results`.

## Hooks & observability

Attach hooks globally or per task to emit metrics, logs, or traces.

```go
logger := weave.Hooks{
	OnStart: func(ctx context.Context, evt weave.TaskEvent) {
		log.Printf("starting %s", evt.Metadata.Name)
	},
	OnFailure: func(ctx context.Context, evt weave.TaskEvent) {
		log.Printf("task %s failed: %v", evt.Metadata.Name, evt.Metrics.Error)
	},
}

graph.Run(ctx, weave.WithGlobalHooks(logger))
```

`TaskEvent` includes metadata, timing information, concurrency level, and (on success) the task result.

## Custom dispatchers

Control concurrency by supplying a custom `Dispatcher`. The package ships with a worker-pool dispatcher:

```go
exec := graph.Start(ctx,
	weave.WithDispatcher(weave.NewWorkerPoolDispatcher(4)),
)

results, metrics, err := exec.Await()
```

Implement the `Dispatcher` interface to integrate with existing goroutine pools, job queues, or distributed executors.

## Graph visualisation

Use `Graph.ExportDOT` to generate Graphviz DOT output for diagrams or tooling:

```go
var buf bytes.Buffer
if err := graph.ExportDOT(&buf,
	weave.DOTWithGraphName("pipeline"),
	weave.DOTWithRankDir("LR"),
); err != nil {
	log.Fatal(err)
}
fmt.Println(buf.String())
```

Run `dot -Tpng pipeline.dot -o pipeline.png` to render the file. See `examples/visualize` for a complete snippet.

## Testing

The project includes a comprehensive test suite covering dependency validation, failure and cancellation behaviour, hooks, metrics, panic recovery, and custom dispatchers. Run it with:

```shell
go test ./...
```

## License

MIT © [Bintang Pradana](https://github.com/bpradana)
