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
	registerSMT("io.debezium.transforms.ExtractNewRecordState", extractNewRecordStateSMT{})
}

// extractNewRecordStateSMT handles the canonical Debezium "unwrap envelope" SMT.
// Redpanda Connect *_cdc inputs already deliver unwrapped row state directly
// from the WAL/binlog, so this SMT is a no-op in an RPCN pipeline. We register
// it to avoid the generic "unsupported SMT — map manually" stub and instead
// emit an informational warning explaining why no processor is generated.
type extractNewRecordStateSMT struct{}

func (extractNewRecordStateSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	ctx.Warn(
		"transforms."+smt.Alias+".type",
		"ExtractNewRecordState is a no-op for RPCN *_cdc inputs — records are already "+
			"unwrapped; advanced options (add.fields, delete.handling.mode) are not translated",
	)
	// Emit no processor nodes.
	return nil, nil
}
