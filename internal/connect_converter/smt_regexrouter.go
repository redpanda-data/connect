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

	"gopkg.in/yaml.v3"
)

func init() {
	registerSMT("org.apache.kafka.connect.transforms.RegexRouter", regexRouterSMT{})
}

type regexRouterSMT struct{}

func (regexRouterSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	regex, _ := smt.Props["regex"].(string)
	replacement, _ := smt.Props["replacement"].(string)
	ctx.Warn(smt.Alias, "RegexRouter rewrites the topic name; verify the rewrite applies to your connector's direction (sink path vs source topic)")
	// Kafka Connect uses $1 backrefs; Go's re_replace_all uses $1 too. The
	// mapping processor takes its Bloblang directly as a string value.
	expr := fmt.Sprintf(`meta kafka_topic = metadata("kafka_topic").re_replace_all(%q, %q)`, regex, replacement)
	return []*yaml.Node{component("mapping", scalar(expr))}, nil
}
