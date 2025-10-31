// Package weave provides declarative orchestration for typed, dependency-aware
// tasks. Graphs of tasks execute concurrently while honouring dependency order,
// propagating cancellation via context.Context, collecting metrics, and
// emitting lifecycle hooks for observability.
package weave
