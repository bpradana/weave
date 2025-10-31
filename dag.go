package weave

import (
	"errors"
	"fmt"
)

var (
	// ErrCycleDetected indicates the dependency graph contains a cycle.
	ErrCycleDetected = errors.New("weave: cycle detected")
	// ErrMissingDependency indicates a task references an unknown dependency.
	ErrMissingDependency = errors.New("weave: missing dependency")
)

type analysis struct {
	nodes      map[string]*node
	indegree   map[string]int
	dependents map[string][]string
	order      []*node
}

func analyzeGraph(g *Graph) (*analysis, error) {
	nodes := g.snapshot()
	indegree := make(map[string]int, len(nodes))
	dependents := make(map[string][]string, len(nodes))

	for id := range nodes {
		indegree[id] = 0
	}

	for id, n := range nodes {
		for _, depID := range n.deps {
			dep, ok := nodes[depID]
			if !ok {
				return nil, fmt.Errorf("%w: %s depends on %s", ErrMissingDependency, id, depID)
			}
			if dep.graphID != n.graphID {
				return nil, ErrForeignDependency
			}
			dependents[dep.id] = append(dependents[dep.id], id)
			indegree[id]++
		}
	}

	order := make([]*node, 0, len(nodes))
	queue := make([]*node, 0, len(nodes))
	remaining := make(map[string]int, len(indegree))

	for id, deg := range indegree {
		remaining[id] = deg
		if deg == 0 {
			queue = append(queue, nodes[id])
		}
	}

	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		order = append(order, n)

		for _, depID := range dependents[n.id] {
			remaining[depID]--
			if remaining[depID] == 0 {
				queue = append(queue, nodes[depID])
			}
		}
	}

	if len(order) != len(nodes) {
		return nil, ErrCycleDetected
	}

	return &analysis{
		nodes:      nodes,
		indegree:   indegree,
		dependents: dependents,
		order:      order,
	}, nil
}
