// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package plan

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/redpanda-data/connect/v4/benchmarking/aws/catalog"
)

// DeletedKeys are removed at any depth of the input, output and buffer
// subtrees (never inside processors): they carry identity (endpoints,
// credentials, names) rather than performance.
var DeletedKeys = []string{
	"dsn", "url", "urls", "addresses", "seed_brokers", "tls", "sasl", "auth",
	"credentials", "credentials_json", "password", "user", "username",
	"private_key", "private_key_file", "private_key_pass", "account", "role",
	"database", "schema", "tables", "table", "topics", "topic", "slot_name",
	"bucket", "path", "project", "dataset", "namespace", "catalog",
}

// ExtraDeletedKeys are further identity keys that component specs accept,
// dropped on top of DeletedKeys.
var ExtraDeletedKeys = []string{
	"consumer_group", "client_id", "endpoint", "region", "headers",
	"host", "hosts", "brokers", "job_project", "table_location", "warehouse",
	"stage", "snowpipe", "cloud", "init_statement", "channel_prefix",
	"channel_name", "checkpoint_key", "checkpoint_cache_key",
}

// DeletedObjects are whole config objects removed from input, output and
// buffer subtrees, whatever they contain. Removing "aws" also switches off
// IAM auth, which the bench replaces with a plain DSN.
var DeletedObjects = []string{
	"aws", "oauth", "oauth2", "basic_auth", "jwt", "sasl", "tls", "credentials",
}

// secretKey matches any key that plausibly holds a credential or identity, in
// the input, output and buffer subtrees. It is the structural backstop behind
// the explicit lists, so a component this package has never seen cannot leak.
var secretKey = regexp.MustCompile(
	"(?i)(secret|token|passw|credential|private|cert|auth|api_?key|access_?key|connection_string|account_key)")

// PureProcessors is the allowlist of processors that make no external calls.
// They are kept verbatim; every other processor (including any a new release
// adds) is replaced with a no-op mapping. Containers (branch, switch, try,
// workflow, ...) are recursed and keep their structure.
var PureProcessors = []string{
	"mapping", "mutation", "bloblang", "jq", "jmespath", "javascript", "awk",
	"branch", "switch", "try", "catch", "while", "for_each", "parallel",
	"processors", "workflow", "group_by", "group_by_value", "split", "archive",
	"unarchive", "compress", "decompress", "json_schema", "parquet_encode",
	"parquet_decode", "avro", "protobuf", "msgpack", "xml", "grok", "log",
	"metric", "sleep", "bounds_check", "select_parts", "insert_part", "noop",
	"dedupe", "benchmark", "parse_log", "sync_response",
}

// BenchCache is the label of the in-memory cache resource that replaces every
// customer cache reference (CDC checkpoints, dedupe).
const BenchCache = "bench_cache"

// listProcessors hold their processor list directly as the component value.
var listProcessors = []string{"try", "catch", "for_each", "parallel", "processors"}

// wrapperInputs combine child inputs; the plan is classified by the children.
var wrapperInputs = []string{"broker", "sequence", "read_until"}

// topLevelKeys are the only top-level config sections that survive: resources,
// http, logger and the like can all carry customer identity.
var topLevelKeys = []string{"input", "pipeline", "output", "buffer"}

// Template is what the bench sets on a swapped component: Fields are written
// into the component's own config, Top into the top level of the pipeline.
type Template struct {
	Fields map[string]any
	Top    map[string]any
}

// SourceTemplates sets the bench's own values on the customer input, per
// source kind. The argument is the customer's input component name.
var SourceTemplates = map[SourceKind]func(component string) Template{
	SourcePostgres: func(string) Template {
		return Template{Fields: map[string]any{
			"dsn":       "${POSTGRES_DSN}",
			"tls":       map[string]any{"skip_cert_verify": true}, // RDS CA is not in the runner image
			"schema":    "public",
			"tables":    []any{"orders"},
			"slot_name": "bench_slot",
		}}
	},
	SourceMySQL: func(string) Template {
		return Template{Fields: map[string]any{
			"dsn":              "${MYSQL_DSN}",
			"tls":              map[string]any{"skip_cert_verify": true},
			"tables":           []any{"orders"},
			"checkpoint_cache": BenchCache,
			"checkpoint_key":   "mysql_bench",
		}}
	},
	// Note: setting consumer_group on a franz/redpanda input that had none
	// changes its semantics (group-managed offsets instead of direct
	// consumption), so the bench always runs in a group, as the soak does.
	SourceRedpanda: func(component string) Template {
		brokers := []any{"${BENCH_REDPANDA_BROKERS}"}
		f := map[string]any{
			"topics":         []any{"${BENCH_TOPIC}"},
			"consumer_group": "${BENCH_CONSUMER_GROUP}",
		}
		return brokerTemplate(component, brokers, f)
	},
}

// SinkTemplates sets the bench's own values on the customer output, per sink
// kind. The argument is the customer's output component name. Later tasks may
// adjust entries (for example the Iceberg template).
var SinkTemplates = map[SinkKind]func(component string) Template{
	SinkRedpanda: func(component string) Template {
		return brokerTemplate(component, []any{"${BENCH_REDPANDA_BROKERS}"},
			map[string]any{"topic": "${BENCH_DST_TOPIC}"})
	},
	SinkS3: func(string) Template {
		return Template{Fields: map[string]any{
			"bucket": "${BENCH_S3_BUCKET}",
			"path":   "${BENCH_S3_PREFIX}${!counter()}.json",
			"region": "${BENCH_AWS_REGION}",
		}}
	},
	SinkIceberg: func(string) Template {
		return Template{Fields: map[string]any{
			"catalog": map[string]any{
				"url":       "${BENCH_ICEBERG_CATALOG_URL}",
				"warehouse": "${BENCH_ICEBERG_WAREHOUSE}",
				"auth": map[string]any{"aws_sigv4": map[string]any{
					"region":  "${BENCH_AWS_REGION}",
					"service": "glue",
				}},
			},
			"namespace": "${BENCH_ICEBERG_NAMESPACE}",
			"table":     "${BENCH_ICEBERG_TABLE}",
			"storage": map[string]any{"aws_s3": map[string]any{
				"bucket": "${BENCH_ICEBERG_BUCKET}",
				"region": "${BENCH_AWS_REGION}",
			}},
		}}
	},
	SinkSnowflake: func(component string) Template {
		f := map[string]any{
			"account":     "${BENCH_SNOWFLAKE_ACCOUNT}",
			"user":        "${BENCH_SNOWFLAKE_USER}",
			"role":        "${BENCH_SNOWFLAKE_ROLE}",
			"database":    "${BENCH_SNOWFLAKE_DATABASE}",
			"schema":      "${BENCH_SNOWFLAKE_SCHEMA}",
			"private_key": "${BENCH_SNOWFLAKE_PRIVATE_KEY}",
		}
		if component == "snowflake_put" {
			f["warehouse"] = "${BENCH_SNOWFLAKE_WAREHOUSE}"
			f["stage"] = "@${BENCH_SNOWFLAKE_STAGE}"
		} else {
			f["table"] = "${BENCH_SNOWFLAKE_TABLE}"
		}
		return Template{Fields: f}
	},
	SinkBigQuery: func(string) Template {
		// credentials_json is left unset: the runner uses its ambient identity.
		return Template{Fields: map[string]any{
			"project": "${BENCH_BQ_PROJECT}",
			"dataset": "${BENCH_BQ_DATASET}",
			"table":   "${BENCH_BQ_TABLE}",
		}}
	},
}

// brokerTemplate adapts the broker address to each Kafka-flavoured component:
// kafka takes addresses, redpanda_common takes a top-level redpanda block.
func brokerTemplate(component string, brokers []any, fields map[string]any) Template {
	t := Template{Fields: fields}
	switch component {
	case "kafka":
		fields["addresses"] = brokers
	case "redpanda_common":
		t.Top = map[string]any{"redpanda": map[string]any{"seed_brokers": brokers}}
	default:
		fields["seed_brokers"] = brokers
	}
	return t
}

// SwapIdentity returns a copy of a customer pipeline with identity replaced by
// bench placeholders, non-pure processors stubbed and snapshots turned off,
// plus the caveats that those changes imply. The input is not modified.
func SwapIdentity(pipeline map[string]any, src SourceKind, sink SinkKind) (map[string]any, []string, error) {
	out := map[string]any{}
	for _, k := range topLevelKeys {
		if v, ok := pipeline[k]; ok {
			out[k] = deepCopy(v)
		}
	}
	inMap, _, err := component(out, "input")
	if err != nil {
		return nil, nil, err
	}
	outMap, outName, err := component(out, "output")
	if err != nil {
		return nil, nil, err
	}

	var caveats []string
	st := &swapState{}
	if stubProcessors(out, st) {
		caveats = append(caveats, CaveatExternalStubbed)
	}
	if src == SourceApproximated {
		stripStrings(inMap)
	}
	for _, k := range topLevelKeys {
		if k != "pipeline" && out[k] != nil {
			scrub(out[k])
		}
	}

	err = inputLeaves(inMap, func(h map[string]any, name string) error {
		if strings.HasSuffix(name, "_cdc") {
			if cfg, ok := h[name].(map[string]any); ok && cfg["stream_snapshot"] == true {
				cfg["stream_snapshot"] = false
				caveats = append(caveats, CaveatSnapshotNotMeasure)
			}
		}
		kind, real := RealSources[name]
		if !real || src == SourceApproximated {
			return nil
		}
		return applyTemplate(out, h, name, SourceTemplates[kind](name))
	})
	if err != nil {
		return nil, nil, err
	}

	if sink == SinkDrop {
		delete(outMap, outName)
		outMap["drop"] = map[string]any{}
	} else if tf, ok := SinkTemplates[sink]; ok {
		if err := applyTemplate(out, outMap, outName, tf(outName)); err != nil {
			return nil, nil, err
		}
	}

	if swapCheckpointCaches(inMap) || st.benchCache {
		out["cache_resources"] = []any{map[string]any{"label": BenchCache, "memory": map[string]any{}}}
	}
	return out, caveats, nil
}

type swapState struct{ benchCache bool }

// inputLeaves calls fn for every non-wrapper input under holder, recursing
// through broker, sequence and read_until.
func inputLeaves(holder map[string]any, fn func(h map[string]any, name string) error) error {
	name := ""
	for k := range holder {
		if k != "processors" && k != "label" {
			name = k
			break
		}
	}
	if name == "" {
		return errors.New("input has no component")
	}
	if !slices.Contains(wrapperInputs, name) {
		return fn(holder, name)
	}
	cfg, ok := holder[name].(map[string]any)
	if !ok {
		return nil
	}
	var children []any
	if l, ok := cfg["inputs"].([]any); ok {
		children = l
	} else if c, ok := cfg["input"].(map[string]any); ok {
		children = []any{c}
	}
	for _, c := range children {
		cm, ok := c.(map[string]any)
		if !ok {
			return fmt.Errorf("%s child input must be a map", name)
		}
		if err := inputLeaves(cm, fn); err != nil {
			return err
		}
	}
	return nil
}

// swapCheckpointCaches points every checkpoint_cache reference at the bench
// in-memory cache and reports whether it found any.
func swapCheckpointCaches(v any) bool {
	found := false
	switch t := v.(type) {
	case map[string]any:
		for k, child := range t {
			if k == "processors" {
				continue
			}
			if _, isStr := child.(string); isStr && k == "checkpoint_cache" {
				t[k] = BenchCache
				found = true
				continue
			}
			if swapCheckpointCaches(child) {
				found = true
			}
		}
	case []any:
		for _, child := range t {
			if swapCheckpointCaches(child) {
				found = true
			}
		}
	}
	return found
}

func applyTemplate(root, holder map[string]any, name string, t Template) error {
	cfg, ok := holder[name].(map[string]any)
	if !ok {
		if holder[name] != nil {
			return fmt.Errorf("%s config must be a map", name)
		}
		cfg = map[string]any{}
		holder[name] = cfg
	}
	for k, v := range t.Fields {
		cfg[k] = deepCopy(v)
	}
	for k, v := range t.Top {
		root[k] = deepCopy(v)
	}
	return nil
}

// component returns the section map and the name of its single component.
func component(root map[string]any, section string) (map[string]any, string, error) {
	m, ok := root[section].(map[string]any)
	if !ok {
		return nil, "", fmt.Errorf("pipeline %s must be a map", section)
	}
	for k := range m {
		if k != "processors" && k != "label" {
			return m, k, nil
		}
	}
	return nil, "", errors.New("pipeline " + section + " has no component")
}

// stubProcessors walks v and replaces every processor not on PureProcessors
// found in a processor list with a no-op mapping, keeping the surrounding
// structure. It reports whether any were replaced.
func stubProcessors(v any, st *swapState) bool {
	stubbed := false
	switch t := v.(type) {
	case map[string]any:
		for k, child := range t {
			if list, ok := child.([]any); ok && k == "processors" {
				if stubList(list, st) {
					stubbed = true
				}
				continue
			}
			if stubProcessors(child, st) {
				stubbed = true
			}
		}
	case []any:
		for _, child := range t {
			if stubProcessors(child, st) {
				stubbed = true
			}
		}
	}
	return stubbed
}

func stubList(list []any, st *swapState) bool {
	stubbed := false
	for i, item := range list {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		name := processorName(m)
		if name == "" {
			continue
		}
		if !slices.Contains(PureProcessors, name) {
			list[i] = map[string]any{"mapping": "root = this"}
			stubbed = true
			continue
		}
		if name == "dedupe" {
			if cfg, ok := m[name].(map[string]any); ok {
				cfg["cache"] = BenchCache
				st.benchCache = true
			}
		}
		if sub, ok := m[name].([]any); ok && slices.Contains(listProcessors, name) {
			if stubList(sub, st) {
				stubbed = true
			}
			continue
		}
		if stubProcessors(m, st) {
			stubbed = true
		}
	}
	return stubbed
}

func processorName(m map[string]any) string {
	for k := range m {
		if k != "label" {
			return k
		}
	}
	return ""
}

// scrub removes identity and secrets from an input, output or buffer subtree.
// Processor lists are skipped: pure processors are kept verbatim.
func scrub(v any) {
	switch t := v.(type) {
	case map[string]any:
		for k, child := range t {
			if k == "processors" {
				continue
			}
			if slices.Contains(DeletedObjects, k) || slices.Contains(DeletedKeys, k) ||
				slices.Contains(ExtraDeletedKeys, k) || secretKey.MatchString(k) {
				delete(t, k)
				continue
			}
			scrub(child)
		}
	case []any:
		for _, child := range t {
			scrub(child)
		}
	}
}

// stripStrings drops every string value and string list except the keys that
// are performance knobs, leaving only numbers, booleans and structure. Used
// for approximated inputs, whose config the bench does not run as written.
// Processor lists are left for the stub and delete passes.
func stripStrings(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, child := range t {
			if k == "processors" {
				continue
			}
			switch c := child.(type) {
			case string:
				if !slices.Contains(catalog.KeepStringKeys, k) {
					delete(t, k)
				}
			case []any:
				if slices.ContainsFunc(c, func(e any) bool { _, ok := e.(string); return ok }) {
					delete(t, k)
					continue
				}
				t[k] = stripStrings(c)
			default:
				t[k] = stripStrings(child)
			}
		}
	case []any:
		for i, child := range t {
			t[i] = stripStrings(child)
		}
	}
	return v
}

func deepCopy(v any) any {
	switch t := v.(type) {
	case map[string]any:
		m := make(map[string]any, len(t))
		for k, child := range t {
			m[k] = deepCopy(child)
		}
		return m
	case []any:
		l := make([]any, len(t))
		for i, child := range t {
			l[i] = deepCopy(child)
		}
		return l
	default:
		return v
	}
}
