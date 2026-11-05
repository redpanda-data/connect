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

func init() {
	// SetSchemaMetadata
	registerSMT("org.apache.kafka.connect.transforms.SetSchemaMetadata$Value", setSchemaMetadataSMT{})
	registerSMT("org.apache.kafka.connect.transforms.SetSchemaMetadata$Key", setSchemaMetadataSMT{})

	// Flatten
	registerSMT("org.apache.kafka.connect.transforms.Flatten$Value", flattenSMT{})
	registerSMT("org.apache.kafka.connect.transforms.Flatten$Key", flattenSMT{})

	// Confluent ReplaceField aliases (delegate to existing replaceFieldSMT)
	registerSMT("io.confluent.connect.transforms.ReplaceField$Value", replaceFieldSMT{})
	registerSMT("io.confluent.connect.transforms.ReplaceField$Key", replaceFieldSMT{})

	// GzipDecompress
	registerSMT("io.confluent.connect.transforms.GzipDecompress$Value", gzipDecompressSMT{})
	registerSMT("io.confluent.connect.transforms.GzipDecompress$Key", gzipDecompressSMT{})

	// Drop
	registerSMT("io.confluent.connect.transforms.Drop$Value", dropSMT{})
	registerSMT("io.confluent.connect.transforms.Drop$Key", dropSMT{})

	// TombstoneHandler (no $Key/$Value suffix)
	registerSMT("io.confluent.connect.transforms.TombstoneHandler", tombstoneHandlerSMT{})

	// FromXml
	registerSMT("io.confluent.connect.cloud.transforms.xml.FromXml$Value", fromXmlSMT{})
}

// setSchemaMetadataSMT maps SetSchemaMetadata$Value/$Key → no-op.
// RPCN is schemaless; schema handling is done by schema_registry_encode/decode components.
type setSchemaMetadataSMT struct{}

func (setSchemaMetadataSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	ctx.Warn(smt.Alias, "SetSchemaMetadata has no RPCN equivalent — schemas are handled by schema_registry_encode/decode; no processor emitted.")
	return []*yaml.Node{}, nil
}

// flattenSMT maps Flatten$Value/$Key → best-effort passthrough with TODO.
// Bloblang has no built-in recursive object flatten with key-joining.
type flattenSMT struct{}

func (flattenSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	delim, _ := smt.Props["delimiter"].(string)
	if delim == "" {
		delim = "."
	}
	expr := scalar("root = this")
	expr.LineComment = fmt.Sprintf(
		`TODO: Flatten nested fields using delimiter %q — Bloblang has no built-in object flatten; map the nested fields manually (e.g. root.a_b = this.a.b)`,
		delim,
	)
	ctx.Warn(smt.Alias, fmt.Sprintf(
		`Flatten is best-effort: Bloblang has no built-in object flatten with delimiter %q; emitted a passthrough stub — map nested fields manually`,
		delim,
	))
	annotateKeyVariant(smt, expr, ctx)
	return []*yaml.Node{mappingProc(expr)}, nil
}

// gzipDecompressSMT maps GzipDecompress$Value/$Key → root = this.decompress("gzip")
type gzipDecompressSMT struct{}

func (gzipDecompressSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	expr := scalar(`root = this.decompress("gzip")`)
	annotateKeyVariant(smt, expr, ctx)
	return []*yaml.Node{mappingProc(expr)}, nil
}

// dropSMT maps Drop$Value → root = null (tombstone),
//
//	Drop$Key → root = this with KEY TODO comment.
type dropSMT struct{}

func (dropSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	if strings.HasSuffix(smt.Type, "$Key") {
		expr := scalar("root = this")
		expr.LineComment = "TODO: Drop$Key should clear the message KEY — set the key field on your output instead (RPCN does not modify keys via a mapping processor)"
		ctx.Warn(smt.Alias, "Drop$Key: clearing the message KEY has no direct RPCN mapping-processor equivalent — set the key field on your output; this SMT targets the message KEY — review manually")
		return []*yaml.Node{mappingProc(expr)}, nil
	}
	expr := scalar("root = null")
	expr.LineComment = "TODO: Drop sets the value to null (tombstone) — verify this is intended"
	return []*yaml.Node{mappingProc(expr)}, nil
}

// tombstoneHandlerSMT maps TombstoneHandler (no $Key/$Value).
// behavior: drop|ignore|warn|fail
type tombstoneHandlerSMT struct{}

func (tombstoneHandlerSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	behavior, _ := smt.Props["behavior"].(string)
	behavior = strings.TrimSpace(strings.ToLower(behavior))

	switch behavior {
	case "drop":
		expr := scalar("root = if this == null { deleted() }")
		return []*yaml.Node{mappingProc(expr)}, nil

	case "fail":
		expr := scalar("root = this")
		expr.LineComment = "TODO: behavior=fail (abort on tombstone) has no RPCN equivalent — handle explicitly"
		ctx.Warn(smt.Alias, "TombstoneHandler behavior=fail has no RPCN equivalent — handle tombstones explicitly in your pipeline")
		return []*yaml.Node{mappingProc(expr)}, nil

	case "warn":
		expr := scalar("root = this")
		expr.LineComment = "TombstoneHandler behavior=warn: RPCN does not log a warning for tombstones; passthrough emitted"
		return []*yaml.Node{mappingProc(expr)}, nil

	case "ignore":
		expr := scalar("root = this")
		expr.LineComment = "TombstoneHandler behavior=ignore: passthrough emitted"
		return []*yaml.Node{mappingProc(expr)}, nil

	default:
		// empty/unknown → passthrough
		expr := scalar("root = this")
		if behavior == "" {
			expr.LineComment = "TombstoneHandler: no behavior specified; passthrough emitted"
		} else {
			expr.LineComment = fmt.Sprintf("TombstoneHandler behavior=%s: passthrough emitted", behavior)
		}
		return []*yaml.Node{mappingProc(expr)}, nil
	}
}

// fromXmlSMT maps FromXml$Value → root = this.parse_xml()
type fromXmlSMT struct{}

func (fromXmlSMT) Map(_ SMTConfig, _ *MapCtx) ([]*yaml.Node, error) {
	expr := scalar("root = this.parse_xml()")
	return []*yaml.Node{mappingProc(expr)}, nil
}
