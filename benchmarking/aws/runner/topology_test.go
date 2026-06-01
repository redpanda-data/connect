// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import "testing"

func TestBenchNames_SourceTopicConventions(t *testing.T) {
	n := newBenchNames("sess-abc", "postgres_cdc")
	if got := n.ConnectTopic(); got != "bench_sess-abc_postgres_cdc_connect" {
		t.Errorf("ConnectTopic = %q, want bench_sess-abc_postgres_cdc_connect", got)
	}
	if got := n.KCTopicPrefix(); got != "bench_sess-abc_postgres_cdc_kc" {
		t.Errorf("KCTopicPrefix = %q, want bench_sess-abc_postgres_cdc_kc", got)
	}
}
