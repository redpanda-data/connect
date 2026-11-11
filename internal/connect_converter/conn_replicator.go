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
)

func init() {
	// Confluent Replicator: a Kafka-topic→Kafka-topic source connector. Like
	// MirrorMaker2 it maps to a redpanda input (source cluster) + redpanda
	// output (destination cluster).
	registerConnector("io.confluent.connect.replicator.ReplicatorSourceConnector", replicatorSourceConnector{})

	// MirrorMaker2's auxiliary connectors carry no data — they sync consumer
	// offsets / emit heartbeats. There is no RPCN data-pipeline equivalent;
	// recognize them so they get clear guidance instead of the generic
	// unsupported-connector stub.
	registerConnector("org.apache.kafka.connect.mirror.MirrorCheckpointConnector", mirrorAuxConnector{kind: "MirrorCheckpoint (consumer-group offset sync)"})
	registerConnector("org.apache.kafka.connect.mirror.MirrorHeartbeatConnector", mirrorAuxConnector{kind: "MirrorHeartbeat (liveness heartbeats)"})
}

type replicatorSourceConnector struct{}

func (replicatorSourceConnector) Map(cfg ConnectConfig, ctx *MapCtx) (Component, error) {
	in := mapping()
	if v, ok := ctx.String("src.kafka.bootstrap.servers"); ok {
		kv(in, "seed_brokers", seq(scalarsFromCSV(v)...))
	} else {
		stub := scalar("")
		stub.LineComment = "TODO: set source cluster brokers"
		kv(in, "seed_brokers", seq(stub))
	}

	// Topic selection: topic.whitelist (literal CSV) or topic.regex (pattern).
	if v, ok := ctx.String("topic.whitelist"); ok && strings.TrimSpace(v) != "" {
		kv(in, "topics", seq(scalarsFromCSV(v)...))
	} else if v, ok := ctx.String("topic.regex"); ok && strings.TrimSpace(v) != "" {
		kv(in, "topics", seq(scalar(strings.TrimSpace(v))))
		kv(in, "regexp_topics", boolScalar(true))
	} else {
		stub := scalar("")
		stub.LineComment = "TODO: set topics to replicate (topic.whitelist or topic.regex)"
		kv(in, "topics", seq(stub))
	}

	cg := scalar(cfg.Name + "-rpcn")
	if v, ok := ctx.String("src.consumer.group.id"); ok && v != "" {
		cg = scalar(v)
	} else {
		cg.LineComment = "TODO: confirm consumer group name"
	}
	kv(in, "consumer_group", cg)

	if proto, ok := ctx.String("src.kafka.security.protocol"); ok {
		addSecurityTODOs(in, proto, "source")
	}

	out := mapping()
	if v, ok := ctx.String("dest.kafka.bootstrap.servers"); ok {
		kv(out, "seed_brokers", seq(scalarsFromCSV(v)...))
	} else {
		stub := scalar("")
		stub.LineComment = "TODO: set destination cluster brokers"
		kv(out, "seed_brokers", seq(stub))
	}

	// topic.rename.format uses ${topic}; RPCN interpolates @kafka_topic.
	rename, _ := ctx.String("topic.rename.format")
	topicNode := scalar(replicatorRenameTopic(rename))
	if rename != "" && rename != "${topic}" {
		topicNode.LineComment = "TODO: from topic.rename.format=" + rename + " — verify destination topic naming"
	}
	kv(out, "topic", topicNode)

	if proto, ok := ctx.String("dest.kafka.security.protocol"); ok {
		addSecurityTODOs(out, proto, "destination")
	}

	// topic.blacklist can't be expressed in the redpanda input topic set.
	if v, ok := ctx.String("topic.blacklist"); ok && v != "" {
		ctx.Warn("topic.blacklist", "redpanda input has no exclude list — topics matching ["+v+"] will be replicated; add a filter processor to drop them")
	}

	// Replicator copies record bytes between clusters; the converters describe
	// Kafka serialization and must NOT become a schema_registry_decode (that
	// would corrupt a byte-for-byte copy).
	consumeConverterKeys(ctx)
	consumeIgnored(ctx, "src.key.converter", "src.value.converter", "header.converter")

	// Source/destination security and Replicator plumbing — values are secrets
	// not present in the JSON, or have no RPCN equivalent.
	consumePrefix(ctx, "src.kafka.")
	consumePrefix(ctx, "dest.kafka.")
	consumePrefix(ctx, "src.consumer.")
	consumePrefix(ctx, "offset.translator.")
	consumeIgnored(ctx,
		"topic.poll.interval.ms",
		"topic.auto.create",
		"topic.preserve.partitions",
		"topic.config.sync",
		"topic.timestamp.type",
		"dest.topic.replication.factor",
		"offset.timestamps.commit",
		"provenance.header.enable",
		"confluent.license",
	)

	return Component{
		Input:  component("redpanda", in),
		Output: component("redpanda", out),
	}, nil
}

// replicatorRenameTopic converts a Replicator topic.rename.format (which uses
// the ${topic} placeholder) into a redpanda output topic interpolation.
func replicatorRenameTopic(format string) string {
	if strings.TrimSpace(format) == "" {
		return "${! @kafka_topic }"
	}
	return strings.ReplaceAll(format, "${topic}", "${! @kafka_topic }")
}

// mirrorAuxConnector handles MirrorMaker2's non-data connectors (Checkpoint,
// Heartbeat). They have no RPCN data-pipeline equivalent, so emit a drop output
// with an explanatory TODO + warning rather than a misleading mapping.
type mirrorAuxConnector struct {
	kind string
}

func (m mirrorAuxConnector) Map(_ ConnectConfig, ctx *MapCtx) (Component, error) {
	ctx.Warn("connector.class", m.kind+" is an operational MirrorMaker2 connector (no record data) — RPCN has no equivalent; offset sync/heartbeats are handled outside the data pipeline")
	body := mapping()
	body.LineComment = "TODO: " + m.kind + " has no RPCN data-pipeline equivalent — remove or handle operationally"
	return Component{Output: component("drop", body)}, nil
}
