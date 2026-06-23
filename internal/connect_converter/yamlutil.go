// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package connectconverter

import "gopkg.in/yaml.v3"

func scalar(val string) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: val}
}

func mapping(pairs ...*yaml.Node) *yaml.Node {
	return &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map", Content: pairs}
}

func seq(items ...*yaml.Node) *yaml.Node {
	return &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq", Content: items}
}

// kv appends a key/value pair to a mapping node.
func kv(m *yaml.Node, key string, val *yaml.Node) {
	m.Content = append(m.Content, scalar(key), val)
}

// component wraps a body map as a single-key map {name: body}, the shape RPCN
// uses for an input/output/processor.
func component(name string, body *yaml.Node) *yaml.Node {
	return mapping(scalar(name), body)
}

// topicObjectPath returns a path scalar using the source topic and a
// timestamp/UUID for uniqueness, with a review TODO comment. Used by S3/GCS
// sink connectors that derive the object path from the topic name.
func topicObjectPath() *yaml.Node {
	path := scalar(`${! @kafka_topic }/${! timestamp_unix() }-${! uuid_v4() }.json`)
	path.LineComment = "TODO: review object path/partitioning"
	return path
}
