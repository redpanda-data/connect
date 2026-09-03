// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import "testing"

func TestTranslateInfraSource(t *testing.T) {
	const region = "us-east-2"
	tests := []struct {
		name string
		src  map[string]any
		key  string
		want string
	}{
		{
			name: "string passes through",
			src:  map[string]any{"table_name": "orders"},
			key:  "table_name",
			want: "orders",
		},
		{
			name: "int formats as decimal",
			src:  map[string]any{"write_capacity": 40000},
			key:  "write_capacity",
			want: "40000",
		},
		{
			name: "slice JSON-encodes to an HCL list literal",
			src:  map[string]any{"table_names": []any{"a", "b", "c"}},
			key:  "table_names",
			want: `["a","b","c"]`,
		},
		{
			name: "empty slice encodes to empty list",
			src:  map[string]any{"table_names": []any{}},
			key:  "table_names",
			want: "[]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := translateInfraSource(tt.src, region)
			if got[tt.key] != tt.want {
				t.Errorf("translateInfraSource[%q] = %q, want %q", tt.key, got[tt.key], tt.want)
			}
			if got["region"] != region {
				t.Errorf("region = %q, want %q", got["region"], region)
			}
		})
	}
}
