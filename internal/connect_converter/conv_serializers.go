// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package connectconverter

import "gopkg.in/yaml.v3"

func init() {
	registerConverter("io.confluent.connect.avro.AvroConverter", schemaRegistryConverter{})
	registerConverter("io.confluent.connect.protobuf.ProtobufConverter", schemaRegistryConverter{})
	registerConverter("org.apache.kafka.connect.json.JsonConverter", noopConverter{})
	registerConverter("org.apache.kafka.connect.storage.StringConverter", noopConverter{})
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
