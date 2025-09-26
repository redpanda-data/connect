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

package confluent

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
)

func TestWithSchemaRegistryHeader(t *testing.T) {
	tests := []struct {
		name             string
		mapping          string
		expectedSchemaID uint32
		expectedText     string
	}{
		{
			name:             "simple schema id with string message",
			mapping:          `root = with_schema_registry_header(123, "hello world")`,
			expectedSchemaID: 123,
			expectedText:     "hello world",
		},
		{
			name:             "zero schema id",
			mapping:          `root = with_schema_registry_header(0, "test")`,
			expectedSchemaID: 0,
			expectedText:     "test",
		},
		{
			name:             "max uint32 schema id",
			mapping:          `root = with_schema_registry_header(4294967295, "test")`,
			expectedSchemaID: 4294967295,
			expectedText:     "test",
		},
		{
			name:             "empty message",
			mapping:          `root = with_schema_registry_header(456, "")`,
			expectedSchemaID: 456,
			expectedText:     "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			e, err := bloblang.Parse(test.mapping)
			require.NoError(t, err)

			res, err := e.Query(nil)
			require.NoError(t, err)

			resultBytes, ok := res.([]byte)
			require.True(t, ok)
			assert.Len(t, resultBytes, 5+len(test.expectedText))

			assert.Equal(t, byte(0x00), resultBytes[0])
			assert.Equal(t, test.expectedSchemaID, binary.BigEndian.Uint32(resultBytes[1:5]))
			assert.Equal(t, test.expectedText, string(resultBytes[5:]))
		})
	}
}

func TestWithSchemaRegistryHeaderErrors(t *testing.T) {
	tests := []struct {
		name          string
		mapping       string
		expectedError string
	}{
		{
			name:          "negative schema id",
			mapping:       `root = with_schema_registry_header(-1, "test")`,
			expectedError: "schema ID must be between 0 and 4294967295",
		},
		{
			name:          "schema id too large",
			mapping:       `root = with_schema_registry_header(4294967296, "test")`,
			expectedError: "schema ID must be between 0 and 4294967295",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			e, err := bloblang.Parse(test.mapping)
			require.NoError(t, err)

			_, err = e.Query(nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.expectedError)
		})
	}
}
