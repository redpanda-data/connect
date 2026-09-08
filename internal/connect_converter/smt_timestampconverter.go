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
	registerSMT("org.apache.kafka.connect.transforms.TimestampConverter$Value", timestampConverterSMT{})
	registerSMT("org.apache.kafka.connect.transforms.TimestampConverter$Key", timestampConverterSMT{})
}

type timestampConverterSMT struct{}

// jodaToGoLayout translates a Joda/SimpleDateFormat pattern to a Go time
// layout string. Only the common tokens are covered; unrecognised characters
// are passed through unchanged.
func jodaToGoLayout(joda string) string {
	var sb strings.Builder
	i := 0
	for i < len(joda) {
		ch := joda[i]

		// Quoted literals: 'T' or any 'literal' → strip quotes and emit as-is.
		if ch == '\'' {
			i++
			for i < len(joda) && joda[i] != '\'' {
				sb.WriteByte(joda[i])
				i++
			}
			if i < len(joda) {
				i++ // closing quote
			}
			continue
		}

		// Multi-char tokens — check longest first.
		switch {
		case strings.HasPrefix(joda[i:], "yyyy"):
			sb.WriteString("2006")
			i += 4
		case strings.HasPrefix(joda[i:], "yy"):
			sb.WriteString("06")
			i += 2
		case strings.HasPrefix(joda[i:], "SSS"):
			sb.WriteString("000")
			i += 3
		case strings.HasPrefix(joda[i:], "XXX"), strings.HasPrefix(joda[i:], "xxx"):
			sb.WriteString("Z07:00")
			i += 3
		case strings.HasPrefix(joda[i:], "MM"):
			sb.WriteString("01")
			i += 2
		case strings.HasPrefix(joda[i:], "dd"):
			sb.WriteString("02")
			i += 2
		case strings.HasPrefix(joda[i:], "HH"):
			sb.WriteString("15")
			i += 2
		case strings.HasPrefix(joda[i:], "hh"):
			sb.WriteString("03")
			i += 2
		case strings.HasPrefix(joda[i:], "mm"):
			sb.WriteString("04")
			i += 2
		case strings.HasPrefix(joda[i:], "ss"):
			sb.WriteString("05")
			i += 2
		case ch == 'M':
			sb.WriteByte('1')
			i++
		case ch == 'd':
			sb.WriteByte('2')
			i++
		case ch == 'a':
			sb.WriteString("PM")
			i++
		case ch == 'z' || ch == 'Z':
			sb.WriteString("Z07:00")
			i++
		default:
			sb.WriteByte(ch)
			i++
		}
	}
	return sb.String()
}

func (timestampConverterSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	field, _ := smt.Props["field"].(string)
	targetType, _ := smt.Props["target.type"].(string)
	format, _ := smt.Props["format"].(string)

	var expr *yaml.Node
	switch {
	case field == "":
		expr = scalar("root = this")
		expr.LineComment = "TODO: TimestampConverter without a 'field' — map manually"
		ctx.Warn(smt.Alias, "TimestampConverter is missing the 'field' property; emitted a passthrough stub")
	case targetType == "unix":
		expr = scalar(fmt.Sprintf("%s = %s.ts_unix()", fieldPath("root", field), fieldPath("this", field)))
	case targetType == "string":
		if format != "" {
			goLayout := jodaToGoLayout(format)
			expr = scalar(fmt.Sprintf(`%s = %s.ts_format(%q)`, fieldPath("root", field), fieldPath("this", field), goLayout))
		} else {
			expr = scalar(fmt.Sprintf(`%s = %s.ts_format("2006-01-02T15:04:05Z07:00")`, fieldPath("root", field), fieldPath("this", field)))
			expr.LineComment = "TODO: set the target format to match the SMT's 'format' property"
			ctx.Warn(smt.Alias, "TimestampConverter target.type=string: review the emitted ts_format layout against the SMT's 'format'")
		}
	case targetType == "Timestamp" || targetType == "Date" || targetType == "Time":
		if format != "" {
			goLayout := jodaToGoLayout(format)
			expr = scalar(fmt.Sprintf(`%s = %s.ts_parse(%q)`, fieldPath("root", field), fieldPath("this", field), goLayout))
		} else {
			expr = scalar(fmt.Sprintf(`%s = %s.ts_parse("2006-01-02T15:04:05Z07:00")`, fieldPath("root", field), fieldPath("this", field)))
			expr.LineComment = "TODO: set the input layout to parse target.type=" + targetType
			ctx.Warn(smt.Alias, "TimestampConverter target.type="+targetType+": review the emitted ts_parse layout against your timestamp format")
		}
	default:
		expr = scalar("root = this")
		expr.LineComment = "TODO: TimestampConverter with unsupported target.type=" + targetType + " — map manually"
		ctx.Warn(smt.Alias, "TimestampConverter has an unrecognised target.type="+targetType+"; emitted a passthrough stub")
	}
	annotateKeyVariant(smt, expr, ctx)
	return []*yaml.Node{mappingProc(expr)}, nil
}
