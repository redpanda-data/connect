// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package connectconverter

import (
	"strings"

	"gopkg.in/yaml.v3"
)

// Convert turns a Kafka Connect connector config (REST-wrapped or flat JSON)
// into an equivalent Redpanda Connect pipeline YAML.
func Convert(input []byte) (*Result, error) {
	cfg, err := parse(input)
	if err != nil {
		return nil, err
	}

	ctx := newMapCtx(cfg)

	comp, err := lookupConnector(cfg.Class).Map(cfg, ctx)
	if err != nil {
		return nil, err
	}

	var procs []*yaml.Node
	procs = append(procs, mapConverters(ctx)...)
	procs = append(procs, mapSMTs(ctx)...)

	unmapped := ctx.Unmapped()
	for _, k := range unmapped {
		ctx.Warn(k, "unmapped field")
	}

	root := assemble(cfg, comp, procs, unmapped)
	out, err := render(root)
	if err != nil {
		return nil, err
	}
	return &Result{YAML: out, Warnings: ctx.Warnings()}, nil
}

// mapConverters maps the value converter to deserialization processors. The key
// converter's presence is consumed silently (rarely needs a pipeline step).
func mapConverters(ctx *MapCtx) []*yaml.Node {
	if cls, ok := ctx.String(KeyConverter.Prefix()); ok {
		_ = cls // consumed; no processor emitted for key conversion in v1
	}
	cls, ok := ctx.String(ValueConverter.Prefix())
	if !ok {
		return nil
	}
	m, found := lookupConverter(cls)
	if !found {
		ctx.Warn(ValueConverter.Prefix(), "unsupported value converter "+cls)
		return nil
	}
	nodes, err := m.Map(ValueConverter, ctx)
	if err != nil {
		ctx.Warn(ValueConverter.Prefix(), err.Error())
		return nil
	}
	return nodes
}

// mapSMTs parses the transforms list in declared order and maps each SMT to
// processor nodes.
func mapSMTs(ctx *MapCtx) []*yaml.Node {
	list, ok := ctx.String("transforms")
	if !ok || strings.TrimSpace(list) == "" {
		return nil
	}
	ctx.consume("transforms")

	var out []*yaml.Node
	for alias := range strings.SplitSeq(list, ",") {
		alias = strings.TrimSpace(alias)
		if alias == "" {
			continue
		}
		prefix := "transforms." + alias + "."
		typ, _ := ctx.String(prefix + "type")

		// Collect this SMT's sub-properties, stripping the prefix.
		props := map[string]any{}
		for k, v := range ctx.cfg.Props {
			if strings.HasPrefix(k, prefix) && k != prefix+"type" {
				ctx.consume(k)
				props[strings.TrimPrefix(k, prefix)] = v
			}
		}

		smt := SMTConfig{Alias: alias, Type: typ, Props: props}
		m, found := lookupSMT(typ)
		if !found {
			ctx.Warn(prefix+"type", "unsupported SMT "+typ)
			s := scalar("root = this")
			s.LineComment = "TODO: unsupported SMT " + typ + " — map manually"
			out = append(out, component("mapping", s))
			continue
		}
		nodes, err := m.Map(smt, ctx)
		if err != nil {
			ctx.Warn(prefix, err.Error())
			continue
		}
		out = append(out, nodes...)
	}
	return out
}
