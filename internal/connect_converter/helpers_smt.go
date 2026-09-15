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

// isSafeIdentifier reports whether s is a valid Bloblang bare identifier
// (letters, digits, underscores only; must not start with a digit).
func isSafeIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
			// always ok
		case r >= '0' && r <= '9':
			if i == 0 {
				return false // cannot start with digit
			}
		default:
			return false
		}
	}
	return true
}

// fieldPath returns a parse-safe Bloblang path for base.name, where base is
// "this" or "root". It splits name on "." into segments; each segment that is
// not a safe identifier is quoted with %q so hyphens, leading digits, and
// other special characters never break Bloblang parsing.
//
// Examples:
//
//	fieldPath("root", "event-type") → `root."event-type"`
//	fieldPath("this", "payload")    → `this.payload`
//	fieldPath("root", "a.b-c")      → `root.a."b-c"`
func fieldPath(base, name string) string {
	var sb strings.Builder
	sb.WriteString(base)
	for seg := range strings.SplitSeq(name, ".") {
		sb.WriteByte('.')
		if isSafeIdentifier(seg) {
			sb.WriteString(seg)
		} else {
			fmt.Fprintf(&sb, "%q", seg)
		}
	}
	return sb.String()
}

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
