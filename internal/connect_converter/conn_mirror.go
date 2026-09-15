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
	// MM2 treats `topics` as Java regex patterns. Enable regexp matching so
	// patterns like "orders.*" work as expected in RPCN.
	kv(in, "regexp_topics", boolScalar(true))
	cg := scalar(cfg.Name + "-rpcn")
	cg.LineComment = "TODO: confirm consumer group name"
	kv(in, "consumer_group", cg)

	// Security: source cluster TLS/SASL are not extractable from connector JSON
	// (secrets are not stored there). Emit TODO stubs when security protocol is set.
	if proto, ok := ctx.String("source.cluster.security.protocol"); ok {
		addSecurityTODOs(in, proto, "source")
	}
	// Consume SASL/SSL source keys — values cannot be mapped (secrets absent).
	consumeIgnored(ctx,
		"source.cluster.sasl.mechanism",
		"source.cluster.sasl.jaas.config",
		"source.cluster.sasl.username",
		"source.cluster.sasl.password",
		"source.cluster.ssl.truststore.location",
		"source.cluster.ssl.truststore.password",
		"source.cluster.ssl.keystore.location",
		"source.cluster.ssl.keystore.password",
		"source.cluster.ssl.key.password",
	)

	out := mapping()
	if v, ok := ctx.String("target.cluster.bootstrap.servers"); ok {
		kv(out, "seed_brokers", seq(scalarsFromCSV(v)...))
	} else {
		stub := scalar("")
		stub.LineComment = "TODO: set target cluster brokers"
		kv(out, "seed_brokers", seq(stub))
	}
	// Target topic naming follows the MM2 replication policy. DefaultReplicationPolicy
	// renames mirrored topics to "<source.alias><separator><topic>"; IdentityReplicationPolicy
	// keeps the original name. We read the policy + alias + separator to reproduce this.
	sourceAlias, _ := ctx.String("source.cluster.alias")
	ctx.consume("target.cluster.alias")
	sep := "."
	if s, ok := ctx.String("replication.policy.separator"); ok && s != "" {
		sep = s
	}
	policy, _ := ctx.String("replication.policy.class")
	identity := strings.Contains(policy, "IdentityReplicationPolicy")
	if identity || sourceAlias == "" {
		// Identity policy (or no alias to prefix with) — keep the topic name.
		topicNode := scalar(`${! @kafka_topic }`)
		if !identity && sourceAlias == "" {
			topicNode.LineComment = "TODO: no source.cluster.alias — set the target topic naming policy if you use DefaultReplicationPolicy"
		}
		kv(out, "topic", topicNode)
	} else {
		// DefaultReplicationPolicy (the MM2 default) prefixes the source alias.
		topicNode := scalar(sourceAlias + sep + `${! @kafka_topic }`)
		topicNode.LineComment = "TODO: DefaultReplicationPolicy renames topics to <source-alias>" + sep + "<topic> — verify"
		kv(out, "topic", topicNode)
	}

	// Security: target cluster TLS/SASL.
	if proto, ok := ctx.String("target.cluster.security.protocol"); ok {
		addSecurityTODOs(out, proto, "target")
	}
	// Consume SASL/SSL target keys — values cannot be mapped (secrets absent).
	consumeIgnored(ctx,
		"target.cluster.sasl.mechanism",
		"target.cluster.sasl.jaas.config",
		"target.cluster.sasl.username",
		"target.cluster.sasl.password",
		"target.cluster.ssl.truststore.location",
		"target.cluster.ssl.truststore.password",
		"target.cluster.ssl.keystore.location",
		"target.cluster.ssl.keystore.password",
		"target.cluster.ssl.key.password",
	)

	// topics.exclude (and the legacy topics.blacklist alias) can't be expressed
	// in the redpanda input's topic list — surface it so the user adds a filter.
	if v, ok := ctx.String("topics.exclude"); ok && v != "" {
		ctx.Warn("topics.exclude", "redpanda input has no exclude list — topics matching ["+v+"] will be mirrored; add a filter processor to drop them")
	} else if v, ok := ctx.String("topics.blacklist"); ok && v != "" {
		ctx.Warn("topics.blacklist", "redpanda input has no exclude list — topics matching ["+v+"] will be mirrored; add a filter processor to drop them")
	}

	// MirrorMaker plumbing — no RPCN equivalent.
	consumeIgnored(ctx,
		"replication.factor",
		"replication.policy.class",
		"replication.policy.separator",
		"sync.topic.configs.enabled",
		"sync.topic.acls.enabled",
		"refresh.topics.enabled",
		"refresh.topics.interval.seconds",
		"emit.heartbeats.enabled",
		"emit.checkpoints.enabled",
		"refresh.groups.enabled",
		"sync.group.offsets.enabled",
		"groups",
		"groups.exclude",
		"consumer.auto.offset.reset",
		"offset-syncs.topic.location",
		"offset-syncs.topic.replication.factor",
		"offset.lag.max",
		"heartbeats.topic.replication.factor",
		"checkpoints.topic.replication.factor",
	)

	return Component{
		Input:  component("redpanda", in),
		Output: component("redpanda", out),
	}, nil
}

// boolScalar returns a YAML scalar node typed as !!bool.
func boolScalar(val bool) *yaml.Node {
	v := "false"
	if val {
		v = "true"
	}
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: v}
}

// addSecurityTODOs appends a tls/sasl comment stub to a mapping node when the
// security protocol indicates encryption or authentication is required. Actual
// credentials are never present in connector JSON, so only TODOs are emitted.
func addSecurityTODOs(m *yaml.Node, protocol, side string) {
	switch strings.ToUpper(protocol) {
	case "SSL", "SASL_SSL":
		tls := mapping()
		stub := boolScalar(true)
		stub.LineComment = "TODO: configure " + side + " cluster TLS (cert/key paths or skip_cert_verify)"
		kv(tls, "enabled", stub)
		kv(m, "tls", tls)
	}
	switch strings.ToUpper(protocol) {
	case "SASL_PLAINTEXT", "SASL_SSL":
		saslItem := mapping()
		mech := scalar("PLAIN")
		mech.LineComment = "TODO: set SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, etc.)"
		kv(saslItem, "mechanism", mech)
		user := scalar("")
		user.LineComment = "TODO: set " + side + " cluster SASL username"
		kv(saslItem, "username", user)
		pass := scalar("")
		pass.LineComment = "TODO: set " + side + " cluster SASL password"
		kv(saslItem, "password", pass)
		kv(m, "sasl", seq(saslItem))
	}
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
