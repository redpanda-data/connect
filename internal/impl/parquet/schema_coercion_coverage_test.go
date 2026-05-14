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
	"math"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/schema"
)

// TestParquetNodeFromCommonField_NewTypes pins coverage for the
// schema.CommonType variants the encoder previously rejected with
// "unsupported by this processor". These are the analogues of the
// type-coverage holes the iceberg sink closed earlier in this branch;
// pipelines flowing CDC schemas → parquet need them just as much.
func TestParquetNodeFromCommonField_NewTypes(t *testing.T) {
	t.Run("Date", func(t *testing.T) {
		n, err := parquetNodeFromCommonField(schema.Common{Name: "d", Type: schema.Date}, parquet.Millisecond)
		require.NoError(t, err)
		require.NotNil(t, n.Type().LogicalType())
		assert.NotNil(t, n.Type().LogicalType().Date, "DATE column must carry the Date logical-type annotation")
	})

	t.Run("TimeOfDay millis", func(t *testing.T) {
		n, err := parquetNodeFromCommonField(schema.Common{
			Name: "t", Type: schema.TimeOfDay,
			Logical: &schema.LogicalParams{TimeOfDay: &schema.TimeOfDayParams{Unit: schema.TimeUnitMillis}},
		}, parquet.Millisecond)
		require.NoError(t, err)
		require.NotNil(t, n.Type().LogicalType())
		require.NotNil(t, n.Type().LogicalType().Time)
		assert.NotNil(t, n.Type().LogicalType().Time.Unit.Millis)
	})

	t.Run("TimeOfDay micros", func(t *testing.T) {
		n, err := parquetNodeFromCommonField(schema.Common{
			Name: "t", Type: schema.TimeOfDay,
			Logical: &schema.LogicalParams{TimeOfDay: &schema.TimeOfDayParams{Unit: schema.TimeUnitMicros}},
		}, parquet.Millisecond)
		require.NoError(t, err)
		require.NotNil(t, n.Type().LogicalType().Time)
		assert.NotNil(t, n.Type().LogicalType().Time.Unit.Micros)
	})

	t.Run("UUID", func(t *testing.T) {
		n, err := parquetNodeFromCommonField(schema.Common{Name: "u", Type: schema.UUID}, parquet.Millisecond)
		require.NoError(t, err)
		require.NotNil(t, n.Type().LogicalType())
		assert.NotNil(t, n.Type().LogicalType().UUID, "UUID column must carry the UUID logical-type annotation")
	})

	t.Run("Map of String->Int64", func(t *testing.T) {
		n, err := parquetNodeFromCommonField(schema.Common{
			Name: "m", Type: schema.Map,
			Children: []schema.Common{{Type: schema.Int64}},
		}, parquet.Millisecond)
		require.NoError(t, err)
		require.NotNil(t, n.Type().LogicalType())
		assert.NotNil(t, n.Type().LogicalType().Map, "Map column must carry the Map logical-type annotation")
	})

	t.Run("Union refused loudly", func(t *testing.T) {
		_, err := parquetNodeFromCommonField(schema.Common{Name: "u", Type: schema.Union}, parquet.Millisecond)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "union", "error message should explain the constraint")
	})

	t.Run("Null refused loudly", func(t *testing.T) {
		_, err := parquetNodeFromCommonField(schema.Common{Name: "n", Type: schema.Null}, parquet.Millisecond)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "NULL")
	})
}

// TestEncodingCoercionVisitor_Temporal pins the cross-type bridges the
// encoder now applies. The bug class is the same one the iceberg
// shredder closes via coerceTemporalToNumeric: a pipeline using
// preserve_logical_types: true emits time.Time / time.Duration values,
// but the parquet encoder was previously rejecting anything but RFC3339
// strings for TIMESTAMP columns.
func TestEncodingCoercionVisitor_Temporal(t *testing.T) {
	const tsMillis = int64(1_700_000_000_000)
	visitor := encodingCoercionVisitor{}

	t.Run("time.Time into TIMESTAMP(millis)", func(t *testing.T) {
		node := parquet.Timestamp(parquet.Millisecond)
		out, err := visitor.visitLeaf(time.UnixMilli(tsMillis).UTC(), node)
		require.NoError(t, err)
		assert.Equal(t, tsMillis, out)
	})

	t.Run("time.Time into TIMESTAMP(micros)", func(t *testing.T) {
		node := parquet.Timestamp(parquet.Microsecond)
		out, err := visitor.visitLeaf(time.UnixMilli(tsMillis).UTC(), node)
		require.NoError(t, err)
		assert.Equal(t, tsMillis*1000, out)
	})

	t.Run("int64 millis into TIMESTAMP(millis) — pre-scaled passthrough", func(t *testing.T) {
		node := parquet.Timestamp(parquet.Millisecond)
		out, err := visitor.visitLeaf(tsMillis, node)
		require.NoError(t, err)
		assert.Equal(t, tsMillis, out)
	})

	t.Run("RFC3339 string into TIMESTAMP(millis) — legacy path", func(t *testing.T) {
		node := parquet.Timestamp(parquet.Millisecond)
		out, err := visitor.visitLeaf("2023-11-14T22:13:20Z", node)
		require.NoError(t, err)
		assert.Equal(t, tsMillis, out)
	})

	t.Run("time.Time into DATE", func(t *testing.T) {
		out, err := visitor.visitLeaf(time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC), parquet.Date())
		require.NoError(t, err)
		assert.Equal(t, int32(19737), out)
	})

	t.Run("date string into DATE", func(t *testing.T) {
		out, err := visitor.visitLeaf("2024-01-15", parquet.Date())
		require.NoError(t, err)
		assert.Equal(t, int32(19737), out)
	})

	t.Run("time.Duration into TIME(millis)", func(t *testing.T) {
		d := 8*time.Hour + 30*time.Minute
		out, err := visitor.visitLeaf(d, parquet.Time(parquet.Millisecond))
		require.NoError(t, err)
		assert.Equal(t, int32(8*3600+30*60)*1000, out)
	})

	t.Run("time.Duration into TIME(micros)", func(t *testing.T) {
		d := 8*time.Hour + 30*time.Minute
		out, err := visitor.visitLeaf(d, parquet.Time(parquet.Microsecond))
		require.NoError(t, err)
		assert.Equal(t, int64(8*3600+30*60)*1_000_000, out)
	})

	t.Run("unsupported type into TIMESTAMP errors clearly", func(t *testing.T) {
		_, err := visitor.visitLeaf(struct{}{}, parquet.Timestamp(parquet.Millisecond))
		require.Error(t, err)
		assert.True(t,
			strings.Contains(err.Error(), "time.Time") || strings.Contains(err.Error(), "RFC3339") || strings.Contains(err.Error(), "numeric"),
			"error message should name an accepted type, got %q", err.Error())
	})
}

// TestEncodingCoercionVisitor_RejectsNaNInf locks in defense-in-depth
// against silent corruption: a NaN or ±Inf float reaching a time-typed
// column must error, not int64-cast to implementation-defined garbage.
// Mirrors the iceberg shredder's TestTemporalRejectsNaNInf so the guard
// is symmetric across the two sinks.
func TestEncodingCoercionVisitor_RejectsNaNInf(t *testing.T) {
	visitor := encodingCoercionVisitor{}
	for _, tc := range []struct {
		name string
		node parquet.Node
	}{
		{"TIMESTAMP(millis)", parquet.Timestamp(parquet.Millisecond)},
		{"TIMESTAMP(micros)", parquet.Timestamp(parquet.Microsecond)},
		{"TIMESTAMP(nanos)", parquet.Timestamp(parquet.Nanosecond)},
		{"DATE", parquet.Date()},
		{"TIME(millis)", parquet.Time(parquet.Millisecond)},
		{"TIME(micros)", parquet.Time(parquet.Microsecond)},
	} {
		for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
			t.Run(tc.name, func(t *testing.T) {
				_, err := visitor.visitLeaf(v, tc.node)
				require.Errorf(t, err, "must reject %v into %s, not silently cast", v, tc.name)
			})
		}
	}
}

// TestCoerceDateForEncode_StringErrorSurfacesBothAttempts verifies that
// when a string fails both the RFC3339 and YYYY-MM-DD parses, the error
// surfaces both attempts rather than only the RFC3339 one — which would
// mislead a user who passed a clearly date-shaped (but invalid) string.
func TestCoerceDateForEncode_StringErrorSurfacesBothAttempts(t *testing.T) {
	_, err := coerceDateForEncode("2024-13-99")
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "RFC3339", "error should mention the RFC3339 attempt")
	assert.Contains(t, msg, "YYYY-MM-DD", "error should mention the bare-date attempt")
}
