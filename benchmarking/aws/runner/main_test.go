// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"reflect"
	"testing"
)

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

// TestLoadGenTFVars covers the key regression this override exists to
// prevent: a scenario that omits infra.load_gen.instance_type must produce
// a map with the key absent entirely, not present-but-empty -- an empty
// -var string would override Terraform's own "c8g.large" default instead of
// leaving it alone.
//
// Both runBench and downCmd call loadGenTFVars directly (see main.go), so
// asserting its output here is what keeps the apply and destroy paths from
// ever disagreeing on the load generator's instance type -- a divergence
// there would make Terraform try to replace the instance during teardown.
func TestLoadGenTFVars(t *testing.T) {
	tests := []struct {
		name string
		s    *Scenario
		want map[string]string
	}{
		{
			name: "override set produces the tf var",
			s:    &Scenario{Infra: InfraSpec{LoadGen: LoadGenSpec{InstanceType: "c8g.4xlarge"}}},
			want: map[string]string{"load_gen_instance_type": "c8g.4xlarge"},
		},
		{
			name: "override omitted produces no key at all",
			s:    &Scenario{},
			want: map[string]string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := loadGenTFVars(tt.s)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("loadGenTFVars(%+v) = %#v, want %#v", tt.s.Infra.LoadGen, got, tt.want)
			}
			if _, exists := got["load_gen_instance_type"]; tt.s.Infra.LoadGen.InstanceType == "" && exists {
				t.Errorf("expected load_gen_instance_type key to be absent, not present-and-empty")
			}
		})
	}
}

func TestEffectiveLoadGenInstanceType(t *testing.T) {
	tests := []struct {
		name string
		s    *Scenario
		want string
	}{
		{
			name: "override set returns the override",
			s:    &Scenario{Infra: InfraSpec{LoadGen: LoadGenSpec{InstanceType: "c8g.4xlarge"}}},
			want: "c8g.4xlarge",
		},
		{
			name: "override unset returns the real terraform default, not empty",
			s:    &Scenario{},
			want: defaultLoadGenInstanceType,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := effectiveLoadGenInstanceType(tt.s)
			if got != tt.want {
				t.Errorf("effectiveLoadGenInstanceType() = %q, want %q", got, tt.want)
			}
		})
	}
}
