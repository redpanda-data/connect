// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package salesforce

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// buildSOQL composes a SOQL query string from its parts. Args are interpolated
// into `where` `?` placeholders with SOQL literal escaping. Empty prefix /
// where / suffix are omitted.
func buildSOQL(object string, columns []string, where, prefix, suffix string, args []any) (string, error) {
	whereSubstituted, err := substituteSOQLPlaceholders(where, args)
	if err != nil {
		return "", err
	}

	var sb strings.Builder
	if p := strings.TrimSpace(prefix); p != "" {
		sb.WriteString(p)
		sb.WriteByte(' ')
	}
	sb.WriteString("SELECT ")
	sb.WriteString(strings.Join(columns, ", "))
	sb.WriteString(" FROM ")
	sb.WriteString(object)
	if w := strings.TrimSpace(whereSubstituted); w != "" {
		sb.WriteString(" WHERE ")
		sb.WriteString(w)
	}
	if suf := strings.TrimSpace(suffix); suf != "" {
		sb.WriteByte(' ')
		sb.WriteString(suf)
	}
	return sb.String(), nil
}

// substituteSOQLPlaceholders walks `where` and replaces unquoted `?` characters
// with SOQL-escaped representations of args, in order. ? characters inside SOQL
// string literals (single quotes) are left alone. Mismatched arg/placeholder
// counts are an error so callers can fail fast at startup.
func substituteSOQLPlaceholders(where string, args []any) (string, error) {
	if where == "" {
		if len(args) != 0 {
			return "", fmt.Errorf("got %d args but no ? placeholders in where", len(args))
		}
		return "", nil
	}

	var sb strings.Builder
	sb.Grow(len(where))
	inSingleQuote := false
	argIdx := 0

	for i := 0; i < len(where); i++ {
		c := where[i]
		if c == '\\' && i+1 < len(where) {
			sb.WriteByte(c)
			sb.WriteByte(where[i+1])
			i++
			continue
		}
		if c == '\'' {
			inSingleQuote = !inSingleQuote
			sb.WriteByte(c)
			continue
		}
		if c == '?' && !inSingleQuote {
			if argIdx >= len(args) {
				return "", fmt.Errorf("more ? placeholders in where than args (have %d args)", len(args))
			}
			esc, err := soqlEscape(args[argIdx])
			if err != nil {
				return "", fmt.Errorf("arg %d: %w", argIdx, err)
			}
			sb.WriteString(esc)
			argIdx++
			continue
		}
		sb.WriteByte(c)
	}

	if argIdx != len(args) {
		return "", fmt.Errorf("got %d args but only %d ? placeholders in where", len(args), argIdx)
	}
	return sb.String(), nil
}

// soqlEscape converts a Bloblang result value to its SOQL literal form.
// Strings are single-quoted with embedded ' and \ escaped. time.Time values
// are rendered ISO-8601 in UTC (no quotes — SOQL date/datetime literals are
// unquoted). Numbers and booleans pass through. nil → null.
func soqlEscape(v any) (string, error) {
	switch x := v.(type) {
	case nil:
		return "null", nil
	case string:
		return soqlQuoteString(x), nil
	case bool:
		return strconv.FormatBool(x), nil
	case int:
		return strconv.FormatInt(int64(x), 10), nil
	case int32:
		return strconv.FormatInt(int64(x), 10), nil
	case int64:
		return strconv.FormatInt(x, 10), nil
	case uint:
		return strconv.FormatUint(uint64(x), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(x), 10), nil
	case uint64:
		return strconv.FormatUint(x, 10), nil
	case float32:
		return strconv.FormatFloat(float64(x), 'g', -1, 32), nil
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64), nil
	case json.Number:
		return string(x), nil
	case time.Time:
		return x.UTC().Format("2006-01-02T15:04:05Z"), nil
	case []byte:
		return soqlQuoteString(string(x)), nil
	default:
		return "", fmt.Errorf("unsupported SOQL arg type %T", v)
	}
}

// soqlQuoteString wraps s in single quotes and escapes embedded ' and \.
func soqlQuoteString(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `'`, `\'`)
	return "'" + r.Replace(s) + "'"
}
