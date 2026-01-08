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

package otlpconv

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	pb "github.com/redpanda-data/common-go/redpanda-otel-exporter/proto"
)

func TestAnyValueRoundtrip(t *testing.T) {
	tests := []struct {
		name  string
		setup func(pcommon.Value)
	}{
		{
			name: "string",
			setup: func(v pcommon.Value) {
				v.SetStr("test string")
			},
		},
		{
			name: "empty string",
			setup: func(v pcommon.Value) {
				v.SetStr("")
			},
		},
		{
			name: "bool true",
			setup: func(v pcommon.Value) {
				v.SetBool(true)
			},
		},
		{
			name: "bool false",
			setup: func(v pcommon.Value) {
				v.SetBool(false)
			},
		},
		{
			name: "int positive",
			setup: func(v pcommon.Value) {
				v.SetInt(12345)
			},
		},
		{
			name: "int negative",
			setup: func(v pcommon.Value) {
				v.SetInt(-67890)
			},
		},
		{
			name: "int zero",
			setup: func(v pcommon.Value) {
				v.SetInt(0)
			},
		},
		{
			name: "double positive",
			setup: func(v pcommon.Value) {
				v.SetDouble(123.456)
			},
		},
		{
			name: "double negative",
			setup: func(v pcommon.Value) {
				v.SetDouble(-789.012)
			},
		},
		{
			name: "double zero",
			setup: func(v pcommon.Value) {
				v.SetDouble(0.0)
			},
		},
		{
			name: "bytes",
			setup: func(v pcommon.Value) {
				v.SetEmptyBytes().FromRaw([]byte{0x01, 0x02, 0x03, 0xff})
			},
		},
		{
			name: "empty bytes",
			setup: func(v pcommon.Value) {
				v.SetEmptyBytes().FromRaw([]byte{})
			},
		},
		{
			name: "slice of strings",
			setup: func(v pcommon.Value) {
				slice := v.SetEmptySlice()
				slice.AppendEmpty().SetStr("one")
				slice.AppendEmpty().SetStr("two")
				slice.AppendEmpty().SetStr("three")
			},
		},
		{
			name: "slice of ints",
			setup: func(v pcommon.Value) {
				slice := v.SetEmptySlice()
				slice.AppendEmpty().SetInt(1)
				slice.AppendEmpty().SetInt(2)
				slice.AppendEmpty().SetInt(3)
			},
		},
		{
			name: "slice of mixed types",
			setup: func(v pcommon.Value) {
				slice := v.SetEmptySlice()
				slice.AppendEmpty().SetStr("string")
				slice.AppendEmpty().SetInt(42)
				slice.AppendEmpty().SetBool(true)
				slice.AppendEmpty().SetDouble(3.14)
			},
		},
		{
			name: "empty slice",
			setup: func(v pcommon.Value) {
				v.SetEmptySlice()
			},
		},
		{
			name: "nested slice",
			setup: func(v pcommon.Value) {
				slice := v.SetEmptySlice()
				inner := slice.AppendEmpty().SetEmptySlice()
				inner.AppendEmpty().SetInt(1)
				inner.AppendEmpty().SetInt(2)
			},
		},
		{
			name: "map",
			setup: func(v pcommon.Value) {
				m := v.SetEmptyMap()
				m.PutStr("key1", "value1")
				m.PutInt("key2", 123)
				m.PutBool("key3", true)
			},
		},
		{
			name: "empty map",
			setup: func(v pcommon.Value) {
				v.SetEmptyMap()
			},
		},
		{
			name: "nested map",
			setup: func(v pcommon.Value) {
				m := v.SetEmptyMap()
				inner := m.PutEmptyMap("nested")
				inner.PutStr("inner_key", "inner_value")
			},
		},
		{
			name: "unicode string",
			setup: func(v pcommon.Value) {
				v.SetStr("Hello 世界 🌍")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create original value
			original := pcommon.NewValueEmpty()
			tt.setup(original)

			// Convert to Redpanda
			redpanda := anyValueToRedpanda(original)
			require.NotNil(t, redpanda)

			// Convert back to pdata
			reconstructed := pcommon.NewValueEmpty()
			anyValueFromRedpanda(redpanda, reconstructed)

			// Verify equality
			assert.Equal(t, original.Type(), reconstructed.Type(), "type mismatch")
			assert.Equal(t, original.AsString(), reconstructed.AsString(), "value mismatch")
		})
	}
}

func TestAttributesRoundtrip(t *testing.T) {
	// Create attributes map
	attrs := pcommon.NewMap()
	attrs.PutStr("service.name", "test-service")
	attrs.PutStr("service.namespace", "test-namespace")
	attrs.PutInt("service.instance.id", 12345)
	attrs.PutBool("is_production", true)
	attrs.PutDouble("version", 1.23)
	attrs.PutEmptyBytes("binary").FromRaw([]byte{0xde, 0xad, 0xbe, 0xef})

	// Add nested slice
	slice := attrs.PutEmptySlice("tags")
	slice.AppendEmpty().SetStr("tag1")
	slice.AppendEmpty().SetStr("tag2")

	// Add nested map
	nested := attrs.PutEmptyMap("metadata")
	nested.PutStr("region", "us-west-2")
	nested.PutInt("shard", 5)

	// Convert to Redpanda
	redpanda := attributesToRedpanda(attrs)
	require.Len(t, redpanda, 8)

	// Convert back to pdata
	reconstructed := pcommon.NewMap()
	attributesFromRedpanda(redpanda, reconstructed)

	// Verify
	assert.Equal(t, attrs.Len(), reconstructed.Len())
	v, ok := reconstructed.Get("service.name")
	assert.True(t, ok)
	assert.Equal(t, "test-service", v.Str())
	v, ok = reconstructed.Get("service.instance.id")
	assert.True(t, ok)
	assert.Equal(t, int64(12345), v.Int())
	v, ok = reconstructed.Get("is_production")
	assert.True(t, ok)
	assert.True(t, v.Bool())
	v, ok = reconstructed.Get("version")
	assert.True(t, ok)
	assert.Equal(t, 1.23, v.Double())
}

func TestResourceRoundtrip(t *testing.T) {
	// Create resource
	original := pcommon.NewResource()
	attrs := original.Attributes()
	attrs.PutStr("service.name", "my-service")
	attrs.PutStr("host.name", "localhost")
	original.SetDroppedAttributesCount(5)

	// Convert to Redpanda
	redpanda := resourceToRedpanda(original)
	require.NotNil(t, redpanda)
	assert.Len(t, redpanda.Attributes, 2)
	assert.Equal(t, uint32(5), redpanda.DroppedAttributesCount)

	// Convert back to pdata
	reconstructed := pcommon.NewResource()
	resourceFromRedpanda(redpanda, reconstructed)

	// Verify
	assert.Equal(t, original.Attributes().Len(), reconstructed.Attributes().Len())
	v, ok := reconstructed.Attributes().Get("service.name")
	assert.True(t, ok)
	assert.Equal(t, "my-service", v.Str())
	assert.Equal(t, uint32(5), reconstructed.DroppedAttributesCount())
}

func TestScopeRoundtrip(t *testing.T) {
	// Create scope
	original := pcommon.NewInstrumentationScope()
	original.SetName("my-instrumentation-lib")
	original.SetVersion("v1.2.3")
	attrs := original.Attributes()
	attrs.PutStr("scope.attr", "value")
	original.SetDroppedAttributesCount(2)

	// Convert to Redpanda
	redpanda := scopeToRedpanda(original)
	require.NotNil(t, redpanda)
	assert.Equal(t, "my-instrumentation-lib", redpanda.Name)
	assert.Equal(t, "v1.2.3", redpanda.Version)
	assert.Len(t, redpanda.Attributes, 1)
	assert.Equal(t, uint32(2), redpanda.DroppedAttributesCount)

	// Convert back to pdata
	reconstructed := pcommon.NewInstrumentationScope()
	scopeFromRedpanda(redpanda, reconstructed)

	// Verify
	assert.Equal(t, original.Name(), reconstructed.Name())
	assert.Equal(t, original.Version(), reconstructed.Version())
	assert.Equal(t, original.Attributes().Len(), reconstructed.Attributes().Len())
	assert.Equal(t, uint32(2), reconstructed.DroppedAttributesCount())
}

func TestEmptyResource(t *testing.T) {
	// Empty resource
	original := pcommon.NewResource()

	// Convert to Redpanda
	redpanda := resourceToRedpanda(original)
	require.NotNil(t, redpanda)
	assert.Empty(t, redpanda.Attributes)

	// Convert back
	reconstructed := pcommon.NewResource()
	resourceFromRedpanda(redpanda, reconstructed)

	assert.Equal(t, 0, reconstructed.Attributes().Len())
}

func TestNilResource(t *testing.T) {
	// Nil resource
	var redpanda *pb.Resource = nil

	// Convert back
	reconstructed := pcommon.NewResource()
	resourceFromRedpanda(redpanda, reconstructed)

	assert.Equal(t, 0, reconstructed.Attributes().Len())
}

func TestTimestampConversion(t *testing.T) {
	tests := []struct {
		name     string
		input    int64
		expected uint64
	}{
		{"positive timestamp", 1609459200000000000, 1609459200000000000},
		{"zero timestamp", 0, 0},
		{"negative timestamp", -1000, 0}, // Should be converted to 0
		{"max int64", 9223372036854775807, 9223372036854775807},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := int64ToUint64(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
