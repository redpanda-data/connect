/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package icebergx

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGoValueToLiteralDecimal(t *testing.T) {
	tests := []struct {
		name      string
		bytes     []byte
		iceType   iceberg.Type
		wantStr   string
		wantScale int
	}{
		{
			name:      "positive decimal(20,0)",
			bytes:     []byte{0x30, 0x39}, // 12345 big-endian two's complement
			iceType:   iceberg.DecimalTypeOf(20, 0),
			wantStr:   "12345",
			wantScale: 0,
		},
		{
			name:      "negative decimal(10,2)",
			bytes:     []byte{0xFF}, // -1 unscaled, scale 2 => -0.01
			iceType:   iceberg.DecimalTypeOf(10, 2),
			wantStr:   "-0.01",
			wantScale: 2,
		},
		{
			name:      "zero decimal(5,0)",
			bytes:     []byte{0x00},
			iceType:   iceberg.DecimalTypeOf(5, 0),
			wantStr:   "0",
			wantScale: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lit, err := goValueToLiteral(tt.bytes, tt.iceType)
			require.NoError(t, err)

			decLit, ok := lit.(iceberg.DecimalLiteral)
			require.True(t, ok, "expected iceberg.DecimalLiteral, got %T", lit)
			assert.Equal(t, tt.wantScale, decLit.Scale)
			assert.Equal(t, tt.wantStr, lit.String())
		})
	}
}
