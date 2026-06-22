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
	registerConnector("org.apache.kafka.connect.mirror.MirrorSourceConnector", mirrorSourceConnector{})
}

type mirrorSourceConnector struct{}

func (mirrorSourceConnector) Map(cfg ConnectConfig, ctx *MapCtx) (Component, error) {
	in := mapping()
	if v, ok := ctx.String("source.cluster.bootstrap.servers"); ok {
		kv(in, "seed_brokers", seq(scalarsFromCSV(v)...))
	} else {
		stub := scalar("")
		stub.LineComment = "TODO: set source cluster brokers"
		kv(in, "seed_brokers", seq(stub))
	}
	if v, ok := ctx.String("topics"); ok {
		kv(in, "topics", seq(scalarsFromCSV(v)...))
	} else {
		stub := scalar("")
		stub.LineComment = "TODO: set topics to mirror"
		kv(in, "topics", seq(stub))
	}
	cg := scalar(cfg.Name + "-rpcn")
	cg.LineComment = "TODO: confirm consumer group name"
	kv(in, "consumer_group", cg)

	out := mapping()
	if v, ok := ctx.String("target.cluster.bootstrap.servers"); ok {
		kv(out, "seed_brokers", seq(scalarsFromCSV(v)...))
	} else {
		stub := scalar("")
		stub.LineComment = "TODO: set target cluster brokers"
		kv(out, "seed_brokers", seq(stub))
	}
	kv(out, "topic", scalar(`${! @kafka_topic }`))

	return Component{
		Input:  component("kafka_franz", in),
		Output: component("kafka_franz", out),
	}, nil
}

// scalarsFromCSV splits a comma-separated Kafka Connect value into scalar nodes.
func scalarsFromCSV(v string) []*yaml.Node {
	parts := strings.Split(v, ",")
	out := make([]*yaml.Node, 0, len(parts))
	for _, p := range parts {
		out = append(out, scalar(strings.TrimSpace(p)))
	}
	return out
}
