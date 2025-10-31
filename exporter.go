package weave

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
)

// ErrNilWriter indicates that a nil writer was provided to an exporter.
var ErrNilWriter = errors.New("weave: nil writer")

// DOTOption configures the behaviour of ExportDOT.
type DOTOption func(*dotConfig)

type dotConfig struct {
	graphName string
	rankDir   string
}

func defaultDOTConfig(g *Graph) dotConfig {
	name := g.id
	if name == "" {
		name = "weave"
	}
	return dotConfig{
		graphName: name,
		rankDir:   "LR",
	}
}

// DOTWithGraphName overrides the DOT graph identifier.
func DOTWithGraphName(name string) DOTOption {
	return func(cfg *dotConfig) {
		if name != "" {
			cfg.graphName = name
		}
	}
}

// DOTWithRankDir sets the rank direction (e.g. "LR", "TB") for the exported DOT graph.
func DOTWithRankDir(rankDir string) DOTOption {
	return func(cfg *dotConfig) {
		if rankDir != "" {
			cfg.rankDir = rankDir
		}
	}
}

// ExportDOT renders the graph in Graphviz DOT format.
func (g *Graph) ExportDOT(w io.Writer, opts ...DOTOption) error {
	if w == nil {
		return ErrNilWriter
	}

	analysis, err := analyzeGraph(g)
	if err != nil {
		return err
	}

	cfg := defaultDOTConfig(g)
	for _, opt := range opts {
		opt(&cfg)
	}

	names := make([]string, 0, len(analysis.nodes))
	for id := range analysis.nodes {
		names = append(names, id)
	}
	sort.Strings(names)

	if _, err := fmt.Fprintf(w, "digraph %s {\n", dotQuoteIdentifier(cfg.graphName)); err != nil {
		return err
	}
	if cfg.rankDir != "" {
		if _, err := fmt.Fprintf(w, "    rankdir=%s;\n", cfg.rankDir); err != nil {
			return err
		}
	}

	for _, name := range names {
		if _, err := fmt.Fprintf(w, "    %s;\n", dotQuoteIdentifier(name)); err != nil {
			return err
		}
	}

	for _, name := range names {
		node := analysis.nodes[name]
		if node == nil {
			continue
		}
		if len(node.deps) == 0 {
			continue
		}
		deps := append([]string(nil), node.deps...)
		sort.Strings(deps)
		for _, dep := range deps {
			if _, err := fmt.Fprintf(w, "    %s -> %s;\n", dotQuoteIdentifier(dep), dotQuoteIdentifier(name)); err != nil {
				return err
			}
		}
	}

	_, err = io.WriteString(w, "}\n")
	return err
}

func dotQuoteIdentifier(name string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range name {
		switch r {
		case '\\', '"':
			b.WriteByte('\\')
			b.WriteRune(r)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}
