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
	registerSMT("org.apache.kafka.connect.transforms.TimestampRouter", timestampRouterSMT{})
	registerSMT("io.confluent.connect.transforms.MessageTimestampRouter", messageTimestampRouterSMT{})
	registerSMT("io.confluent.connect.transforms.ExtractTopic$Value", extractTopicSMT{})
	registerSMT("io.confluent.connect.transforms.ExtractTopic$Key", extractTopicSMT{})
	registerSMT("io.confluent.connect.transforms.ExtractTopic$Header", extractTopicSMT{})
	registerSMT("io.confluent.connect.cloud.transforms.TopicRegexRouter", regexRouterSMT{})
	registerSMT("io.debezium.transforms.ByLogicalTableRouter", byLogicalTableRouterSMT{})
}

// buildTopicFormatExpr builds a Bloblang expression for meta kafka_topic
// from a KC topic.format string (placeholders: ${topic} and ${timestamp})
// and a Joda timestamp.format pattern.
// Example: "${topic}-${timestamp}" + "yyyyMMdd"
// → `meta kafka_topic = @kafka_topic + "-" + metadata("kafka_timestamp_unix").number().ts_format("20060102")`
func buildTopicFormatExpr(topicFormat, tsLayout string) string {
	// Split on ${topic} and ${timestamp} placeholders by tokenising the format
	// string manually so we can preserve literal characters between them.
	var parts []string
	rem := topicFormat
	for len(rem) > 0 {
		topicIdx := strings.Index(rem, "${topic}")
		tsIdx := strings.Index(rem, "${timestamp}")

		// No more placeholders → rest is a literal
		if topicIdx == -1 && tsIdx == -1 {
			if rem != "" {
				parts = append(parts, fmt.Sprintf("%q", rem))
			}
			break
		}

		// Pick whichever placeholder comes first
		if topicIdx != -1 && (tsIdx == -1 || topicIdx < tsIdx) {
			// literal prefix before ${topic}
			if topicIdx > 0 {
				parts = append(parts, fmt.Sprintf("%q", rem[:topicIdx]))
			}
			parts = append(parts, "@kafka_topic")
			rem = rem[topicIdx+len("${topic}"):]
		} else {
			// literal prefix before ${timestamp}
			if tsIdx > 0 {
				parts = append(parts, fmt.Sprintf("%q", rem[:tsIdx]))
			}
			parts = append(parts, fmt.Sprintf(`metadata("kafka_timestamp_unix").number().ts_format(%q)`, tsLayout))
			rem = rem[tsIdx+len("${timestamp}"):]
		}
	}

	if len(parts) == 0 {
		// Degenerate empty format — just keep the current topic
		return "meta kafka_topic = @kafka_topic"
	}
	return "meta kafka_topic = " + strings.Join(parts, " + ")
}

// ---------------------------------------------------------------------------
// 1. TimestampRouter
// ---------------------------------------------------------------------------

type timestampRouterSMT struct{}

func (timestampRouterSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	topicFormat, _ := smt.Props["topic.format"].(string)
	if topicFormat == "" {
		topicFormat = "${topic}-${timestamp}"
	}
	tsFormat, _ := smt.Props["timestamp.format"].(string)
	if tsFormat == "" {
		tsFormat = "yyyyMMdd"
	}
	tsLayout := jodaToGoLayout(tsFormat)
	expr := buildTopicFormatExpr(topicFormat, tsLayout)
	ctx.Warn(smt.Alias, "TimestampRouter rewrites the topic name; verify the rewrite applies to your connector's direction (sink output path vs source topic)")
	return []*yaml.Node{mappingProc(scalar(expr))}, nil
}

// ---------------------------------------------------------------------------
// 2. MessageTimestampRouter (Confluent variant)
// ---------------------------------------------------------------------------

type messageTimestampRouterSMT struct{}

func (messageTimestampRouterSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	topicFormat, _ := smt.Props["topic.format"].(string)
	if topicFormat == "" {
		topicFormat = "${topic}-${timestamp}"
	}
	tsFormat, _ := smt.Props["timestamp.format"].(string)
	if tsFormat == "" {
		tsFormat = "yyyyMMdd"
	}
	tsLayout := jodaToGoLayout(tsFormat)
	expr := buildTopicFormatExpr(topicFormat, tsLayout)

	node := scalar(expr)

	// Handle optional message.timestamp.keys (CSV of fields)
	if keys, _ := smt.Props["message.timestamp.keys"].(string); keys != "" {
		node.LineComment = fmt.Sprintf("TODO: pull timestamp from field(s) %s instead of the record timestamp", keys)
		ctx.Warn(smt.Alias, fmt.Sprintf("MessageTimestampRouter: message.timestamp.keys=%s — emitted routing uses record timestamp (kafka_timestamp_unix); update to read from field(s) %s", keys, keys))
	}

	ctx.Warn(smt.Alias, "MessageTimestampRouter rewrites the topic name; verify the rewrite applies to your connector's direction (sink output path vs source topic)")
	return []*yaml.Node{mappingProc(node)}, nil
}

// ---------------------------------------------------------------------------
// 3. ExtractTopic ($Value / $Key / $Header)
// ---------------------------------------------------------------------------

type extractTopicSMT struct{}

func (extractTopicSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	field, _ := smt.Props["field"].(string)
	// skip.missing.or.null is acknowledged; we consume it below so it doesn't
	// show up as unmapped, but we can't replicate the behaviour in Bloblang here.
	_ = smt.Props["skip.missing.or.null"]

	var expr *yaml.Node
	switch {
	case strings.HasSuffix(smt.Type, "$Header"):
		// Header variant: use metadata()
		if field == "" {
			e := scalar("meta kafka_topic = metadata(\"\")")
			e.LineComment = "TODO: ExtractTopic$Header requires a 'field' property — set the header name"
			ctx.Warn(smt.Alias, "ExtractTopic$Header is missing the 'field' property; emitted a stub")
			expr = e
		} else {
			expr = scalar(fmt.Sprintf(`meta kafka_topic = metadata(%q)`, field))
		}
	case strings.HasSuffix(smt.Type, "$Key"):
		// Key variant: treat same as value but attach the $Key caveat
		if field == "" {
			expr = scalar("meta kafka_topic = this.string()")
		} else {
			expr = scalar(fmt.Sprintf("meta kafka_topic = %s.string()", fieldPath("this", field)))
		}
		annotateKeyVariant(smt, expr, ctx)
	default:
		// $Value (default) variant
		if field == "" {
			expr = scalar("meta kafka_topic = this.string()")
		} else {
			expr = scalar(fmt.Sprintf("meta kafka_topic = %s.string()", fieldPath("this", field)))
		}
	}

	ctx.Warn(smt.Alias, "ExtractTopic rewrites the topic name; verify the rewrite applies to your connector's direction (sink output path vs source topic)")
	return []*yaml.Node{mappingProc(expr)}, nil
}

// ---------------------------------------------------------------------------
// 5. ByLogicalTableRouter (Debezium)
// ---------------------------------------------------------------------------

type byLogicalTableRouterSMT struct{}

func (byLogicalTableRouterSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	regex, _ := smt.Props["topic.regex"].(string)
	replacement, _ := smt.Props["topic.replacement"].(string)

	expr := fmt.Sprintf(`meta kafka_topic = @kafka_topic.re_replace_all(%q, %q)`, regex, replacement)
	node := scalar(expr)

	// Detect key.field.* or key.enforce.uniqueness — not translatable.
	var hasKeyFields bool
	for k := range smt.Props {
		if strings.HasPrefix(k, "key.field.") || k == "key.enforce.uniqueness" {
			hasKeyFields = true
			break
		}
	}
	if hasKeyFields {
		node.LineComment = "TODO: key-field rewriting (adding source topic as a key field) is not translated — configure manually"
		ctx.Warn(smt.Alias, "ByLogicalTableRouter: key-field rewriting (key.field.* / key.enforce.uniqueness) is not translated; configure manually")
	}

	ctx.Warn(smt.Alias, "ByLogicalTableRouter rewrites the topic name; verify the rewrite applies to your connector's direction (sink output path vs source topic)")
	return []*yaml.Node{mappingProc(node)}, nil
}
