// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package sqlutil

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCanonicaliseDecimal(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		precision int32
		scale     int32
		want      string
		wantErr   bool
	}{
		{name: "already canonical at scale", input: "1.5000", precision: 18, scale: 4, want: "1.5000"},
		{name: "fewer fractional digits than scale right-pads", input: "1.5", precision: 18, scale: 4, want: "1.5000"},
		{name: "integer at scale 0", input: "42", precision: 18, scale: 0, want: "42"},
		{name: "negative value", input: "-12.34", precision: 5, scale: 2, want: "-12.34"},
		{name: "zero", input: "0", precision: 5, scale: 2, want: "0.00"},
		{name: "leading plus accepted via fallback", input: "+1.5", precision: 18, scale: 4, want: "1.5000"},
		{name: "scientific notation accepted via fallback", input: "1.5e2", precision: 5, scale: 0, want: "150"},
		{name: "rejects values that lose precision at the declared scale", input: "1.56789", precision: 10, scale: 2, wantErr: true},
		{name: "rejects scientific notation that exceeds scale", input: "1.5e-5", precision: 10, scale: 2, wantErr: true},
		{name: "boundary precision 38 scale 0", input: "12345678901234567890123456789012345678", precision: 38, scale: 0, want: "12345678901234567890123456789012345678"},
		{name: "exceeds precision returns error", input: "12345.6789", precision: 5, scale: 2, wantErr: true},
		{name: "malformed input returns error", input: "not-a-number", precision: 5, scale: 2, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CanonicaliseDecimal(tt.input, tt.precision, tt.scale)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCanonicaliseDecimalBytes(t *testing.T) {
	got, err := CanonicaliseDecimalBytes([]byte("1.5"), 10, 4)
	require.NoError(t, err)
	assert.Equal(t, "1.5000", got)
}

func TestCanonicaliseBigDecimal(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "natural scale preserved", input: "1.50000", want: "1.50000"},
		{name: "integer no decimal point", input: "42", want: "42"},
		{name: "negative value", input: "-1.5", want: "-1.5"},
		{name: "zero", input: "0", want: "0"},
		{name: "leading zeros tolerated by parser", input: "01.5", want: "1.5"},
		{name: "leading plus tolerated by parser", input: "+1.5", want: "1.5"},
		{name: "missing integer part tolerated", input: ".5", want: "0.5"},
		{name: "very large value", input: "12345678901234567890.1234567890", want: "12345678901234567890.1234567890"},
		{name: "multiple decimal points rejected", input: "1.2.3", wantErr: true},
		{name: "non-digit chars rejected", input: "1.2a", wantErr: true},
		{name: "empty string rejected", input: "", wantErr: true},
		{name: "scientific notation rejected", input: "1e2", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CanonicaliseBigDecimal(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCanonicaliseBigDecimalBytes(t *testing.T) {
	got, err := CanonicaliseBigDecimalBytes([]byte("1.5"))
	require.NoError(t, err)
	assert.Equal(t, "1.5", got)
}

// TestCanonicaliseDecimalNoLossAtMaxPrecision exercises the high end of the
// supported precision range. Values up to 38 digits must round-trip through
// the big.Float fallback without losing the trailing digits.
func TestCanonicaliseDecimalNoLossAtMaxPrecision(t *testing.T) {
	// 38 nines as integer.
	input := strings.Repeat("9", 38)
	got, err := CanonicaliseDecimal(input, 38, 0)
	require.NoError(t, err)
	assert.Equal(t, input, got)
}
