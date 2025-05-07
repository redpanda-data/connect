// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package changestreams

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/redpanda-data/connect/v4/internal/impl/gcp/enterprise/changestreams/metadata"
)

func TestGroupPartitionsByCreatedAt(t *testing.T) {
	pms := []metadata.PartitionMetadata{
		{PartitionToken: "a", CreatedAt: time.Unix(0, 10_000)},
		{PartitionToken: "b", CreatedAt: time.Unix(0, 10_000)},
		{PartitionToken: "c", CreatedAt: time.Unix(0, 20_000)},
		{PartitionToken: "d", CreatedAt: time.Unix(0, 20_000)},
	}

	got := groupPartitionsByCreatedAt(pms)

	want := [][]metadata.PartitionMetadata{
		{{PartitionToken: "a", CreatedAt: time.Unix(0, 10_000)}, {PartitionToken: "b", CreatedAt: time.Unix(0, 10_000)}},
		{{PartitionToken: "c", CreatedAt: time.Unix(0, 20_000)}, {PartitionToken: "d", CreatedAt: time.Unix(0, 20_000)}},
	}

	if diff := cmp.Diff(got, want); diff != "" {
		t.Errorf("groupPartitionsByCreatedAt() mismatch (-want +got):\n%s", diff)
	}
}

func TestTokensOf(t *testing.T) {
	pms := []metadata.PartitionMetadata{
		{PartitionToken: "a"},
		{PartitionToken: "b"},
		{PartitionToken: "c"},
		{PartitionToken: "d"},
	}

	got := tokensOf(pms)
	want := []string{"a", "b", "c", "d"}
	if diff := cmp.Diff(got, want); diff != "" {
		t.Errorf("tokensOf() mismatch (-want +got):\n%s", diff)
	}
}
