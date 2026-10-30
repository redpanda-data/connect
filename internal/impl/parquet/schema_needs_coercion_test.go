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

package parquet

import (
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
)

func TestSchemaNeedsCoercion(t *testing.T) {
	tests := []struct {
		name string
		node parquet.Node
		want bool
	}{
		{name: "boolean", node: parquet.Group{"a": parquet.Leaf(parquet.BooleanType)}},
		{name: "int32", node: parquet.Group{"a": parquet.Leaf(parquet.Int32Type)}},
		{name: "int64", node: parquet.Group{"a": parquet.Leaf(parquet.Int64Type)}},
		{name: "float", node: parquet.Group{"a": parquet.Leaf(parquet.FloatType)}},
		{name: "double", node: parquet.Group{"a": parquet.Leaf(parquet.DoubleType)}},
		{name: "byte array", node: parquet.Group{"a": parquet.Leaf(parquet.ByteArrayType)}},
		{name: "utf8", node: parquet.Group{"a": parquet.String()}},
		{name: "enum", node: parquet.Group{"a": parquet.Enum()}},
		{name: "bson", node: parquet.Group{"a": parquet.BSON()}},
		{
			name: "nested plain",
			node: parquet.Group{
				"a": parquet.Leaf(parquet.Int64Type),
				"g": parquet.Optional(parquet.Group{
					"b": parquet.String(),
					"h": parquet.Repeated(parquet.Group{"c": parquet.Leaf(parquet.Int32Type)}),
				}),
			},
		},
		{name: "timestamp", node: parquet.Group{"a": parquet.Timestamp(parquet.Millisecond)}, want: true},
		{name: "date", node: parquet.Group{"a": parquet.Date()}, want: true},
		{name: "time", node: parquet.Group{"a": parquet.Time(parquet.Microsecond)}, want: true},
		{name: "json", node: parquet.Group{"a": parquet.JSON()}, want: true},
		{name: "uuid", node: parquet.Group{"a": parquet.UUID()}, want: true},
		{name: "decimal", node: parquet.Group{"a": parquet.Decimal(2, 9, parquet.Int32Type)}, want: true},
		{name: "optional timestamp", node: parquet.Group{"a": parquet.Optional(parquet.Timestamp(parquet.Millisecond))}, want: true},
		{name: "repeated uuid", node: parquet.Group{"a": parquet.Repeated(parquet.UUID())}, want: true},
		{
			name: "nested timestamp",
			node: parquet.Group{
				"a": parquet.Leaf(parquet.Int64Type),
				"g": parquet.Optional(parquet.Group{
					"h": parquet.Repeated(parquet.Group{"c": parquet.Timestamp(parquet.Nanosecond)}),
				}),
			},
			want: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, schemaNeedsCoercion(test.node))
			assert.Equal(t, test.want, schemaNeedsCoercion(parquet.NewSchema("", test.node)))
		})
	}
}
