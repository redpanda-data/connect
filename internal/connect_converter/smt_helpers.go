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

// mappingProc wraps a Bloblang expression as a mapping processor node.
func mappingProc(expr *yaml.Node) *yaml.Node {
	return component("mapping", expr)
}

// annotateKeyVariant adds a TODO comment and warning when the SMT is the $Key
// class variant, since the generated Bloblang operates on the value document.
func annotateKeyVariant(smt SMTConfig, expr *yaml.Node, ctx *MapCtx) {
	if !strings.HasSuffix(smt.Type, "$Key") {
		return
	}
	if expr.LineComment == "" {
		expr.LineComment = "TODO: this SMT targets the message KEY — review manually"
	} else {
		expr.LineComment += "; TODO: targets the message KEY — review manually"
	}
	ctx.Warn(smt.Alias, "this SMT targets the message KEY; review — RPCN sets keys via the output key field / meta key, not the value document")
}
