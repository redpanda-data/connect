// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package sqlutil

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/schema"
)

func decimalCommon(precision, scale int32) schema.Common {
	c, err := schema.NewDecimal("col", precision, scale, true)
	if err != nil {
		panic(err)
	}
	return c
}

func TestCoerceToCommon(t *testing.T) {
	tests := []struct {
		name    string
		common  schema.Common
		in      any
		want    any
		wantErr bool
	}{
		// Int64.
		{name: "int64 from string", common: schema.Common{Type: schema.Int64}, in: "42", want: int64(42)},
		{name: "int64 from int64", common: schema.Common{Type: schema.Int64}, in: int64(42), want: int64(42)},
		{name: "int64 from json.Number", common: schema.Common{Type: schema.Int64}, in: json.Number("42"), want: int64(42)},
		{name: "int64 from non-numeric string preserved", common: schema.Common{Type: schema.Int64}, in: "nope", want: "nope", wantErr: true},

		// Float.
		{name: "float64 from string", common: schema.Common{Type: schema.Float64}, in: "3.14", want: float64(3.14)},
		{name: "float32 widens to float64", common: schema.Common{Type: schema.Float32}, in: "1.5", want: float64(1.5)},
		{name: "float from json.Number", common: schema.Common{Type: schema.Float64}, in: json.Number("2.5"), want: float64(2.5)},
		{name: "float from int64", common: schema.Common{Type: schema.Float64}, in: int64(7), want: float64(7)},
		{name: "float NaN preserved", common: schema.Common{Type: schema.Float64}, in: "NaN", want: "NaN", wantErr: true},

		// Decimal: every numeric input shape must become a canonical string.
		{name: "decimal from string", common: decimalCommon(10, 2), in: "12.5", want: "12.50"},
		{name: "decimal from json.Number fractional", common: decimalCommon(10, 2), in: json.Number("12.5"), want: "12.50"},
		// The regression: a bare integer (int64) for a Decimal column must be
		// canonicalised to a string, not left as a JSON number.
		{name: "decimal from int64", common: decimalCommon(10, 2), in: int64(2), want: "2.00"},
		{name: "decimal from json.Number integer", common: decimalCommon(38, 2), in: json.Number("2"), want: "2.00"},
		{name: "decimal without logical params left alone", common: schema.Common{Type: schema.Decimal}, in: int64(2), want: int64(2)},

		// BigDecimal.
		{name: "bigdecimal from string", common: schema.Common{Type: schema.BigDecimal}, in: "12.50", want: "12.50"},
		{name: "bigdecimal from int64", common: schema.Common{Type: schema.BigDecimal}, in: int64(678), want: "678"},
		{name: "bigdecimal from json.Number", common: schema.Common{Type: schema.BigDecimal}, in: json.Number("3.14159"), want: "3.14159"},

		// Passthroughs.
		{name: "string column untouched", common: schema.Common{Type: schema.String}, in: "hello", want: "hello"},
		{name: "nil untouched", common: decimalCommon(10, 2), in: nil, want: nil},
		{name: "byte slice on bytearray untouched", common: schema.Common{Type: schema.ByteArray}, in: []byte{1, 2}, want: []byte{1, 2}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CoerceToCommon(tt.common, tt.in)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}
