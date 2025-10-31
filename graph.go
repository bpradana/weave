package weave

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

var graphCounter atomic.Uint64

// Graph defines a collection of tasks with dependencies and shared execution context.
type Graph struct {
	mu    sync.Mutex
	id    string
	nodes map[string]*node
}

// NewGraph constructs an empty task graph.
func NewGraph() *Graph {
	return &Graph{
		id:    fmt.Sprintf("graph-%d", graphCounter.Add(1)),
		nodes: make(map[string]*node),
	}
}

// TaskFunc defines the signature for a task implementation.
type TaskFunc[T any] func(ctx context.Context, deps DependencyResolver) (T, error)

type taskKey struct {
	graphID string
	id      string
}

// Handle references a task producing a value of type T.
type Handle[T any] struct {
	key taskKey
}

func (h *Handle[T]) taskKey() taskKey {
	return h.key
}

// ID returns the stable identifier of the task.
func (h *Handle[T]) ID() string {
	return h.key.id
}

// GraphID returns the graph identifier the handle belongs to.
func (h *Handle[T]) GraphID() string {
	return h.key.graphID
}

// Value retrieves the task's typed result from a dependency resolver.
func (h *Handle[T]) Value(res DependencyResolver) (T, error) {
	var zero T
	if res == nil {
		return zero, fmt.Errorf("weave: nil resolver for task %s", h.key.id)
	}
	value, err := res.Value(h)
	if err != nil {
		return zero, err
	}
	casted, ok := value.(T)
	if !ok {
		return zero, fmt.Errorf("weave: type assertion failed for task %s", h.key.id)
	}
	return casted, nil
}

// TaskReference marks types that can be used as dependencies.
type TaskReference interface {
	taskKey() taskKey
}

type taskConfig struct {
	dependencies []taskKey
	hooks        Hooks
}

// TaskOption configures task definition.
type TaskOption func(*taskConfig)

// DependsOn declares task dependencies.
func DependsOn(refs ...TaskReference) TaskOption {
	return func(cfg *taskConfig) {
		for _, ref := range refs {
			cfg.dependencies = append(cfg.dependencies, ref.taskKey())
		}
	}
}

// WithHooks attaches lifecycle hooks to a single task.
func WithHooks(h Hooks) TaskOption {
	return func(cfg *taskConfig) {
		cfg.hooks = cfg.hooks.Merge(h)
	}
}

var (
	// ErrTaskExists indicates a task name collision within the same graph.
	ErrTaskExists = errors.New("weave: task already exists")
	// ErrEmptyTaskName indicates a task was defined without a name.
	ErrEmptyTaskName = errors.New("weave: task name must not be empty")
	// ErrNilRun indicates a task was defined without an implementation.
	ErrNilRun = errors.New("weave: task run function must not be nil")
	// ErrForeignDependency signals a dependency from a different graph was supplied.
	ErrForeignDependency = errors.New("weave: dependency belongs to another graph")
	// ErrNilSubgraph indicates a nil subgraph was supplied to AddGraphTask.
	ErrNilSubgraph = errors.New("weave: subgraph must not be nil")
	// ErrNilGraphTaskResult indicates a nil result mapper was supplied to AddGraphTask.
	ErrNilGraphTaskResult = errors.New("weave: graph task result func must not be nil")
	// ErrRecursiveGraphTask indicates the graph was asked to embed itself as a task.
	ErrRecursiveGraphTask = errors.New("weave: cannot embed graph within itself")
)

// AddTask registers a new task within the graph with the provided name, implementation, and options.
func AddTask[T any](g *Graph, name string, run TaskFunc[T], opts ...TaskOption) (*Handle[T], error) {
	if g == nil {
		return nil, errors.New("weave: nil graph")
	}
	if run == nil {
		return nil, ErrNilRun
	}
	if name == "" {
		return nil, ErrEmptyTaskName
	}

	cfg := taskConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}

	key, err := g.addNode(name, wrapTaskFunc(run), cfg)
	if err != nil {
		return nil, err
	}
	return &Handle[T]{key: key}, nil
}

// GraphTaskResultFunc converts the outcome of running a subgraph into the parent task's result.
type GraphTaskResultFunc[T any] func(context.Context, *Results, ExecutionMetrics, error) (T, error)

// GraphTaskOption configures a graph task wrapper.
type GraphTaskOption func(*graphTaskConfig)

type graphTaskConfig struct {
	taskOpts     []TaskOption
	executorOpts []ExecutorOption
}

// WithGraphTaskOptions appends regular task options (e.g. dependencies or hooks) to the embedded task.
func WithGraphTaskOptions(opts ...TaskOption) GraphTaskOption {
	return func(cfg *graphTaskConfig) {
		cfg.taskOpts = append(cfg.taskOpts, opts...)
	}
}

// WithGraphTaskExecutorOptions forwards executor options to the embedded graph when it runs.
func WithGraphTaskExecutorOptions(opts ...ExecutorOption) GraphTaskOption {
	return func(cfg *graphTaskConfig) {
		cfg.executorOpts = append(cfg.executorOpts, opts...)
	}
}

// AddGraphTask wraps a fully-defined graph as a task within another graph so it can be composed or reused.
// The subgraph executes with the provided context when this task runs, and the result mapper translates its
// output into the parent task's typed result.
func AddGraphTask[T any](parent *Graph, name string, subgraph *Graph, result GraphTaskResultFunc[T], opts ...GraphTaskOption) (*Handle[T], error) {
	if parent == nil {
		return nil, errors.New("weave: nil parent graph")
	}
	if subgraph == nil {
		return nil, ErrNilSubgraph
	}
	if parent == subgraph {
		return nil, ErrRecursiveGraphTask
	}
	if result == nil {
		return nil, ErrNilGraphTaskResult
	}

	cfg := graphTaskConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}

	run := func(ctx context.Context, deps DependencyResolver) (T, error) {
		results, metrics, err := subgraph.Run(ctx, cfg.executorOpts...)
		return result(ctx, results, metrics, err)
	}

	return AddTask(parent, name, run, cfg.taskOpts...)
}

func (g *Graph) addNode(name string, run runFunc, cfg taskConfig) (taskKey, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, exists := g.nodes[name]; exists {
		return taskKey{}, fmt.Errorf("%w: %s", ErrTaskExists, name)
	}

	depSet := make(map[string]struct{}, len(cfg.dependencies))
	deps := make([]string, 0, len(cfg.dependencies))
	for _, dep := range cfg.dependencies {
		if dep.graphID != g.id {
			return taskKey{}, fmt.Errorf("%w (%s)", ErrForeignDependency, dep.id)
		}
		if _, seen := depSet[dep.id]; seen {
			continue
		}
		depSet[dep.id] = struct{}{}
		deps = append(deps, dep.id)
	}

	node := &node{
		id:      name,
		name:    name,
		deps:    deps,
		depSet:  depSet,
		run:     run,
		hooks:   cfg.hooks,
		graphID: g.id,
	}

	g.nodes[name] = node
	return taskKey{graphID: g.id, id: name}, nil
}

func (g *Graph) snapshot() map[string]*node {
	g.mu.Lock()
	defer g.mu.Unlock()
	clone := make(map[string]*node, len(g.nodes))
	for id, n := range g.nodes {
		clone[id] = n
	}
	return clone
}

// Validate ensures the graph forms a directed acyclic graph with all dependencies satisfied.
func (g *Graph) Validate() error {
	_, err := analyzeGraph(g)
	return err
}

type runFunc func(context.Context, *resolver) (any, error)

type node struct {
	id      string
	name    string
	deps    []string
	depSet  map[string]struct{}
	run     runFunc
	hooks   Hooks
	graphID string
}

func wrapTaskFunc[T any](run TaskFunc[T]) runFunc {
	return func(ctx context.Context, r *resolver) (any, error) {
		val, err := run(ctx, r)
		if err != nil {
			var zero T
			return zero, err
		}
		return any(val), nil
	}
}
