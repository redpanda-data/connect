// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"fmt"
)

// BenchNames is the single source of truth for the per-session, per-engine
// resource names a bench uses. The same naming conventions were previously
// duplicated across renderPipelineConfig, combineReset, buildKCRenderInputs,
// and AttributeByEngine.
type BenchNames struct {
	SessionID string
	Connector string
}

func newBenchNames(sessionID, connector string) BenchNames {
	return BenchNames{SessionID: sessionID, Connector: connector}
}

// ConnectTopic is the single topic Connect writes to in a source bench.
func (n BenchNames) ConnectTopic() string {
	return fmt.Sprintf("bench_%s_%s_connect", n.SessionID, n.Connector)
}

// KCTopicPrefix is the Debezium topic.prefix for a source bench; KC emits
// <prefix>.<schema>.<table> topics under it.
func (n BenchNames) KCTopicPrefix() string {
	return fmt.Sprintf("bench_%s_%s_kc", n.SessionID, n.Connector)
}
