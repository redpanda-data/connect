// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import "testing"

func TestSinkSpecFor_Iceberg(t *testing.T) {
	sp, ok := sinkSpecFor("iceberg")
	if !ok {
		t.Fatal("iceberg sinkSpec must be registered")
	}
	if sp.OutputComponent != "iceberg" {
		t.Errorf("OutputComponent = %q", sp.OutputComponent)
	}
	if sp.Namespace == "" {
		t.Error("Namespace must be set")
	}
}

func TestSinkSpecFor_Unknown(t *testing.T) {
	if _, ok := sinkSpecFor("nope"); ok {
		t.Error("unknown sink must not resolve")
	}
}
