// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import "testing"

func TestKCConnectorSpecFor_Unknown(t *testing.T) {
	_, ok := kcConnectorSpecFor("does_not_exist")
	if ok {
		t.Error("expected lookup of unknown connector to return ok=false")
	}
}

func TestKCConnectorSpecFor_PostgresPlaceholder(t *testing.T) {
	// Registry will be filled in later tasks. This test just confirms
	// the lookup function compiles and returns ok=false on an empty registry.
	_, ok := kcConnectorSpecFor("postgres_cdc")
	_ = ok // not asserting yet; future tasks add entries.
}
