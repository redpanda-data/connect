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
	registerSMT("org.apache.kafka.connect.transforms.InsertHeader", insertHeaderSMT{})
	registerSMT("org.apache.kafka.connect.transforms.DropHeaders", dropHeadersSMT{})
	registerSMT("org.apache.kafka.connect.transforms.HeaderFrom$Value", headerFromSMT{})
	registerSMT("org.apache.kafka.connect.transforms.HeaderFrom$Key", headerFromSMT{})
	registerSMT("io.debezium.transforms.HeaderToValue", headerToValueSMT{})
}

// insertHeaderSMT maps InsertHeader → meta <header> = "<value.literal>"
type insertHeaderSMT struct{}

func (insertHeaderSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	header, _ := smt.Props["header"].(string)
	value, _ := smt.Props["value.literal"].(string)

	if strings.TrimSpace(header) == "" {
		expr := scalar("root = this")
		expr.LineComment = "TODO: InsertHeader has no 'header' property — map manually"
		ctx.Warn(smt.Alias, "InsertHeader is missing the 'header' property; emitted a passthrough stub")
		return []*yaml.Node{mappingProc(expr)}, nil
	}

	expr := scalar(fmt.Sprintf("meta %s = %q", metaKey(header), value))
	return []*yaml.Node{mappingProc(expr)}, nil
}

// dropHeadersSMT maps DropHeaders → meta <h> = deleted() for each header in CSV.
type dropHeadersSMT struct{}

func (dropHeadersSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	headers, _ := smt.Props["headers"].(string)

	if strings.TrimSpace(headers) == "" {
		expr := scalar("root = this")
		expr.LineComment = "TODO: DropHeaders has no 'headers' property — map manually"
		ctx.Warn(smt.Alias, "DropHeaders is missing the 'headers' property; emitted a passthrough stub")
		return []*yaml.Node{mappingProc(expr)}, nil
	}

	var lines []string
	for h := range strings.SplitSeq(headers, ",") {
		h = strings.TrimSpace(h)
		if h != "" {
			lines = append(lines, fmt.Sprintf("meta %s = deleted()", metaKey(h)))
		}
	}

	if len(lines) == 0 {
		expr := scalar("root = this")
		expr.LineComment = "TODO: DropHeaders has no parseable header names — map manually"
		ctx.Warn(smt.Alias, "DropHeaders has no parseable header names; emitted a passthrough stub")
		return []*yaml.Node{mappingProc(expr)}, nil
	}

	expr := scalar(strings.Join(lines, "\n"))
	return []*yaml.Node{mappingProc(expr)}, nil
}

// headerFromSMT maps HeaderFrom$Value / HeaderFrom$Key.
// For each (field[i], header[i]) pair: meta <header> = this.<field>
// If operation==move, also appends: root.<field> = deleted()
type headerFromSMT struct{}

func (headerFromSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	fieldsStr, _ := smt.Props["fields"].(string)
	headersStr, _ := smt.Props["headers"].(string)
	operation, _ := smt.Props["operation"].(string)

	fields := splitCSV(fieldsStr)
	headers := splitCSV(headersStr)

	if len(fields) == 0 || len(headers) == 0 {
		expr := scalar("root = this")
		expr.LineComment = "TODO: HeaderFrom has no 'fields' or 'headers' property — map manually"
		ctx.Warn(smt.Alias, "HeaderFrom is missing 'fields' or 'headers' property; emitted a passthrough stub")
		annotateKeyVariant(smt, expr, ctx)
		return []*yaml.Node{mappingProc(expr)}, nil
	}

	count := min(len(fields), len(headers))
	mismatch := len(fields) != len(headers)

	var lines []string
	for i := range count {
		field := fields[i]
		header := headers[i]
		lines = append(lines, fmt.Sprintf("meta %s = %s", metaKey(header), fieldPath("this", field)))
		if operation == "move" {
			lines = append(lines, fieldPath("root", field)+" = deleted()")
		}
	}

	expr := scalar(strings.Join(lines, "\n"))
	if mismatch {
		expr.LineComment = fmt.Sprintf("TODO: fields/headers length mismatch (%d vs %d) — mapped common prefix only", len(fields), len(headers))
		ctx.Warn(smt.Alias, fmt.Sprintf("HeaderFrom fields/headers length mismatch (%d vs %d); mapped common prefix only", len(fields), len(headers)))
	}

	annotateKeyVariant(smt, expr, ctx)
	return []*yaml.Node{mappingProc(expr)}, nil
}

// headerToValueSMT maps io.debezium.transforms.HeaderToValue.
// Inverse of HeaderFrom: for each (header[i], field[i]) pair:
// root.<field> = metadata("<header>"). If operation==move, also meta <header> = deleted().
type headerToValueSMT struct{}

func (headerToValueSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	headersStr, _ := smt.Props["headers"].(string)
	fieldsStr, _ := smt.Props["fields"].(string)
	operation, _ := smt.Props["operation"].(string)

	headers := splitCSV(headersStr)
	fields := splitCSV(fieldsStr)

	if len(headers) == 0 || len(fields) == 0 {
		expr := scalar("root = this")
		expr.LineComment = "TODO: HeaderToValue has no 'headers' or 'fields' property — map manually"
		ctx.Warn(smt.Alias, "HeaderToValue is missing 'headers' or 'fields' property; emitted a passthrough stub")
		return []*yaml.Node{mappingProc(expr)}, nil
	}

	count := min(len(headers), len(fields))
	mismatch := len(headers) != len(fields)

	var lines []string
	for i := range count {
		header := headers[i]
		field := fields[i]
		lines = append(lines, fmt.Sprintf("%s = metadata(%q)", fieldPath("root", field), header))
		if operation == "move" {
			lines = append(lines, fmt.Sprintf("meta %s = deleted()", metaKey(header)))
		}
	}

	expr := scalar(strings.Join(lines, "\n"))
	if mismatch {
		expr.LineComment = fmt.Sprintf("TODO: headers/fields length mismatch (%d vs %d) — mapped common prefix only", len(headers), len(fields))
		ctx.Warn(smt.Alias, fmt.Sprintf("HeaderToValue headers/fields length mismatch (%d vs %d); mapped common prefix only", len(headers), len(fields)))
	}

	return []*yaml.Node{mappingProc(expr)}, nil
}

// splitCSV splits a comma-separated string and trims whitespace from each element.
// Returns nil if the input is empty.
func splitCSV(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	var out []string
	for part := range strings.SplitSeq(s, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

// metaKey returns a Bloblang meta key reference, quoting it when it contains
// characters that would be misinterpreted (e.g. hyphens parsed as minus).
// Safe identifiers match [a-zA-Z_][a-zA-Z0-9_]*.
func metaKey(name string) string {
	if isSafeIdentifier(name) {
		return name
	}
	return fmt.Sprintf("%q", name)
}
