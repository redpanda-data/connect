// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package connectconverter

import (
	"fmt"
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

	if comp.Output != nil && comp.Input == nil {
		if _, known := connectorMappers[cfg.Class]; known {
			if in := sinkInputFromTopics(cfg, ctx); in != nil {
				comp.Input = in
			}
		}
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
//
// If the connector mapper already consumed the value.converter key (e.g.
// Debezium CDC connectors call consumeDebeziumCommon which marks it consumed),
// we skip converter processing entirely — CDC inputs deliver structured rows
// directly from the WAL/binlog and do not read Avro/JSON-encoded Kafka bytes.
func mapConverters(ctx *MapCtx) []*yaml.Node {
	if ctx.consumed[ValueConverter.Prefix()] {
		// Connector already handled (or explicitly discarded) the value converter.
		return nil
	}
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

// filterSMTType is the only KC built-in SMT that handles KC predicates
// internally (via filterSMT.Map). Generic gating skips this type to avoid
// double-wrapping.
const filterSMTType = "org.apache.kafka.connect.transforms.Filter"

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

		// Generic predicate gating: if this non-Filter SMT carries a predicate,
		// wrap its processors in a switch so they only execute for matching records.
		if typ != filterSMTType {
			nodes = applyPredicateGating(smt, nodes, ctx)
		}

		out = append(out, nodes...)
	}
	return out
}

// applyPredicateGating checks whether smt.Props["predicate"] is set. If it is,
// it wraps nodes in a benthos switch processor:
//
//	switch:
//	  - check: <predExpr>     # or "!(<predExpr>)" when negate=true
//	    processors:
//	      - <node...>
//
// If the predicate class is unknown, nodes are returned ungated and a warning is
// emitted. If no predicate is set, nodes are returned unchanged.
func applyPredicateGating(smt SMTConfig, nodes []*yaml.Node, ctx *MapCtx) []*yaml.Node {
	predName, _ := smt.Props["predicate"].(string)
	predName = strings.TrimSpace(predName)
	if predName == "" {
		return nodes
	}

	// Consume all predicates.* keys so they don't surface as unmapped fields.
	// (The Filter SMT does this itself; for other SMTs we do it here.)
	consumeAllPredicateKeys(ctx)

	negateStr, _ := smt.Props["negate"].(string)
	negate := strings.EqualFold(strings.TrimSpace(negateStr), "true")

	predExpr, ok := resolvePredicateExpr(predName, ctx)
	if !ok {
		// Unknown predicate class — leave nodes ungated, warn.
		ctx.Warn(smt.Alias, fmt.Sprintf("unsupported predicate class for predicate %q — SMT applied unconditionally; map predicate manually", predName))
		return nodes
	}

	checkExpr := predExpr
	if negate {
		checkExpr = fmt.Sprintf("!(%s)", predExpr)
	}

	// Build: switch: [{check: <expr>, processors: [<nodes...>]}]
	caseNode := mapping()
	kv(caseNode, "check", scalar(checkExpr))
	procsNode := seq(nodes...)
	kv(caseNode, "processors", procsNode)

	switchBody := seq(caseNode)
	switchProc := component("switch", switchBody)

	return []*yaml.Node{switchProc}
}
