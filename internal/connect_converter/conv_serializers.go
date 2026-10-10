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

func init() {
	registerConverter("io.confluent.connect.avro.AvroConverter", schemaRegistryConverter{})
	registerConverter("io.confluent.connect.protobuf.ProtobufConverter", schemaRegistryConverter{})
	registerConverter("io.confluent.connect.json.JsonSchemaConverter", schemaRegistryConverter{})
	registerConverter("org.apache.kafka.connect.json.JsonConverter", jsonConverter{})
	registerConverter("org.apache.kafka.connect.storage.StringConverter", noopConverter{})
	// ByteArrayConverter is a raw byte passthrough (the MirrorMaker2 default and
	// common for raw object-store sinks). No decode step is needed.
	registerConverter("org.apache.kafka.connect.converters.ByteArrayConverter", noopConverter{})
	// Snowflake-bundled converters.
	registerConverter("com.snowflake.kafka.connector.records.SnowflakeJsonConverter", noopConverter{})
	registerConverter("com.snowflake.kafka.connector.records.SnowflakeAvroConverter", schemaRegistryConverter{})
}

// schemaRegistryConverter emits a schema_registry_decode processor.
type schemaRegistryConverter struct{}

func (schemaRegistryConverter) Map(role ConverterRole, ctx *MapCtx) ([]*yaml.Node, error) {
	body := mapping()
	urlKey := role.Prefix() + ".schema.registry.url"
	if v, ok := ctx.String(urlKey); ok {
		kv(body, "url", scalar(v))
	} else {
		stub := scalar("http://localhost:8081")
		stub.LineComment = "TODO: set the schema registry URL"
		kv(body, "url", stub)
	}
	return []*yaml.Node{component("schema_registry_decode", body)}, nil
}

// noopConverter emits no processor (data is already structured/text).
type noopConverter struct{}

func (noopConverter) Map(_ ConverterRole, _ *MapCtx) ([]*yaml.Node, error) {
	return nil, nil
}

// jsonConverter handles org.apache.kafka.connect.json.JsonConverter.
//
// Redpanda Connect auto-parses JSON message bytes whenever a processor or output
// accesses them structurally, so `schemas.enable=false` needs no decode step.
// With `schemas.enable=true` (the Kafka Connect default) each record is wrapped
// in Connect's `{"schema":..., "payload":...}` envelope, so the real record must
// be unwrapped from `.payload`.
type jsonConverter struct{}

func (jsonConverter) Map(role ConverterRole, ctx *MapCtx) ([]*yaml.Node, error) {
	v, ok := ctx.String(role.Prefix() + ".schemas.enable")
	val := strings.TrimSpace(v)
	switch {
	case ok && strings.EqualFold(val, "true"):
		s := scalar("root = this.payload")
		s.LineComment = "TODO: JsonConverter schemas.enable=true wraps records in a {schema,payload} envelope — unwrapping payload"
		return []*yaml.Node{component("mapping", s)}, nil
	case !ok:
		// schemas.enable defaults to true in Kafka Connect; flag the ambiguity
		// rather than silently (un)wrapping.
		ctx.Warn(role.Prefix(), "JsonConverter schemas.enable defaults to true — if records carry the {schema,payload} envelope, add `root = this.payload` to unwrap")
		return nil, nil
	default:
		// schemas.enable=false → plain JSON, auto-parsed on structured access.
		return nil, nil
	}
}
