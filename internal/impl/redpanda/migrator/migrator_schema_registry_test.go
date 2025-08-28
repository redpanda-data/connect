// Copyright 2025 Redpanda Data, Inc.
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

package migrator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/sr"
)

func TestParseVersions(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Versions
		wantErr  bool
	}{
		{
			name:     "valid latest version",
			input:    "latest",
			expected: VersionsLatest,
			wantErr:  false,
		},
		{
			name:     "valid all versions",
			input:    "all",
			expected: VersionsAll,
			wantErr:  false,
		},
		{
			name:     "invalid versions",
			input:    "invalid_versions",
			expected: "",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseVersions(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestVersionsString(t *testing.T) {
	assert.Equal(t, "latest", VersionsLatest.String())
	assert.Equal(t, "all", VersionsAll.String())
}

func TestSchemaEquals(t *testing.T) {
	tests := []struct {
		name string
		a    sr.Schema
		b    sr.Schema
		eq   bool
	}{
		{
			name: "equal when schema differs only by whitespace and newlines",
			a:    sr.Schema{Schema: "{\n  \"type\": \"string\"\n}\n"},
			b:    sr.Schema{Schema: "{\"type\":\"string\"}"},
			eq:   true,
		},
		{
			name: "not equal when schema text differs materially",
			a:    sr.Schema{Schema: "{\"type\":\"string\"}"},
			b:    sr.Schema{Schema: "{\"type\":\"int\"}"},
			eq:   false,
		},
		{
			name: "not equal when other fields differ (Type)",
			a:    sr.Schema{Schema: "{\"type\":\"string\"}", Type: sr.TypeJSON},
			b:    sr.Schema{Schema: "{\n\t\"type\": \"string\"\n}", Type: sr.TypeAvro},
			eq:   false,
		},
		{
			name: "not equal when references differ",
			a:    sr.Schema{Schema: "{\"type\":\"string\"}", References: []sr.SchemaReference{{Name: "A", Subject: "s", Version: 1}}},
			b:    sr.Schema{Schema: "{\n\t\"type\": \"string\"\n}", References: []sr.SchemaReference{{Name: "B", Subject: "s", Version: 1}}},
			eq:   false,
		},
		{
			name: "equal when schema and all other fields equal",
			a:    sr.Schema{Schema: "{\"type\":\"string\"}", Type: sr.TypeAvro},
			b:    sr.Schema{Schema: "\n{\n  \"type\": \"string\"\n}\n", Type: sr.TypeAvro},
			eq:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.eq, schemaEquals(tt.a, tt.b))
		})
	}
}
