// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildValueConfigYAML(t *testing.T) {
	tests := []struct {
		name, key, component string
		conf                 fieldSpec
		want                 string
	}{
		{
			name: "string config", key: "inputs", component: "resource",
			conf: fieldSpec{Type: "string", Kind: "scalar", Default: json.RawMessage(`""`)},
			want: "inputs:\n  label: \"\"\n  resource: \"\"",
		},
		{
			name: "list config", key: "outputs", component: "fallback",
			conf: fieldSpec{Type: "output", Kind: "array", Default: json.RawMessage(`[]`)},
			want: "outputs:\n  label: \"\"\n  fallback: []",
		},
		{
			name: "list config without a default", key: "caches", component: "multilevel",
			conf: fieldSpec{Type: "string", Kind: "array", Default: json.RawMessage(`null`)},
			want: "caches:\n  multilevel: []",
		},
		{
			name: "object without fields", key: "metrics", component: "json_api",
			conf: fieldSpec{Type: "object", Kind: "scalar", Default: json.RawMessage(`{}`)},
			want: "metrics:\n  json_api: {}",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, buildValueConfigYAML(test.key, test.component, test.conf))
		})
	}
}
