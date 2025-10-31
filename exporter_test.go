package weave

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestExportDOT(t *testing.T) {
	g := NewGraph()

	alpha, err := AddTask(g, "alpha", func(ctx context.Context, deps DependencyResolver) (int, error) {
		return 1, nil
	})
	if err != nil {
		t.Fatalf("add alpha: %v", err)
	}

	beta, err := AddTask(g, "beta", func(ctx context.Context, deps DependencyResolver) (int, error) {
		return alpha.Value(deps)
	}, DependsOn(alpha))
	if err != nil {
		t.Fatalf("add beta: %v", err)
	}

	_, err = AddTask(g, "gamma", func(ctx context.Context, deps DependencyResolver) (int, error) {
		a, err := alpha.Value(deps)
		if err != nil {
			return 0, err
		}
		b, err := beta.Value(deps)
		if err != nil {
			return 0, err
		}
		return a + b, nil
	}, DependsOn(alpha, beta))
	if err != nil {
		t.Fatalf("add gamma: %v", err)
	}

	var buf bytes.Buffer
	if err := g.ExportDOT(&buf, DOTWithGraphName("pipeline"), DOTWithRankDir("TB")); err != nil {
		t.Fatalf("export DOT: %v", err)
	}

	want := `digraph "pipeline" {
    rankdir=TB;
    "alpha";
    "beta";
    "gamma";
    "alpha" -> "beta";
    "alpha" -> "gamma";
    "beta" -> "gamma";
}
`
	if got := buf.String(); got != want {
		t.Fatalf("unexpected DOT output:\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}

func TestExportDOTMissingWriter(t *testing.T) {
	g := NewGraph()
	if _, err := AddTask(g, "noop", func(ctx context.Context, deps DependencyResolver) (int, error) {
		return 0, nil
	}); err != nil {
		t.Fatalf("add task: %v", err)
	}

	err := g.ExportDOT(nil)
	if !errors.Is(err, ErrNilWriter) {
		t.Fatalf("expected ErrNilWriter, got %v", err)
	}
}
