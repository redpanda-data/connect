// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import "testing"

func TestOrdersSchema_SixFields(t *testing.T) {
	s := ordersSchema()
	want := []string{"id", "ts", "region", "amount", "status", "payload"}
	if got := len(s.Fields()); got != len(want) {
		t.Fatalf("schema has %d fields, want %d", got, len(want))
	}
	for i, name := range want {
		if s.Field(i).Name != name {
			t.Errorf("field %d = %q, want %q", i, s.Field(i).Name, name)
		}
	}
}
