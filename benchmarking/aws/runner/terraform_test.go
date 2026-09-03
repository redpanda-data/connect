// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeTerraformOutputs(t *testing.T) {
	tests := []struct {
		name string
		json string
		want map[string]string
	}{
		{
			name: "string output",
			json: `{"bucket_name":{"sensitive":false,"type":"string","value":"rpcn-bench-results"}}`,
			want: map[string]string{"bucket_name": "rpcn-bench-results"},
		},
		{
			name: "numeric output",
			json: `{"vcpu_count":{"sensitive":false,"type":"number","value":8}}`,
			want: map[string]string{"vcpu_count": "8"},
		},
		{
			name: "bool output",
			json: `{"multi_az":{"sensitive":false,"type":"bool","value":true}}`,
			want: map[string]string{"multi_az": "true"},
		},
		{
			name: "list output",
			json: `{"subnet_ids":{"sensitive":false,"type":["list","string"],"value":["subnet-1","subnet-2"]}}`,
			want: map[string]string{"subnet_ids": `["subnet-1","subnet-2"]`},
		},
		{
			name: "nested object output",
			json: `{"endpoint":{"sensitive":false,"type":["object",{"host":"string","port":"number"}],"value":{"host":"db.internal","port":5432}}}`,
			want: map[string]string{"endpoint": `{"host":"db.internal","port":5432}`},
		},
		{
			name: "sensitive-marked output still surfaces its real value",
			json: `{"db_password":{"sensitive":true,"type":"string","value":"s3cr3t"}}`,
			want: map[string]string{"db_password": "s3cr3t"},
		},
		{
			name: "multiple outputs of mixed types",
			json: `{
				"bucket_name":{"sensitive":false,"type":"string","value":"rpcn-bench-results"},
				"vcpu_count":{"sensitive":false,"type":"number","value":8}
			}`,
			want: map[string]string{
				"bucket_name": "rpcn-bench-results",
				"vcpu_count":  "8",
			},
		},
		{
			name: "empty outputs",
			json: `{}`,
			want: map[string]string{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decodeTerraformOutputs([]byte(tc.json))
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestDecodeTerraformOutputs_InvalidJSON(t *testing.T) {
	_, err := decodeTerraformOutputs([]byte("not json"))
	require.Error(t, err)
}
