// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package saphana

import (
	"io"
	"math"
	"math/big"
	"testing"
	"time"

	gohdb "github.com/SAP/go-hdb/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/schema"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/license"
)

func parseInputConf(t *testing.T, yaml string) *service.ParsedConfig {
	t.Helper()
	conf, err := sapHANAInputConfigSpec.ParseYAML(yaml, nil)
	require.NoError(t, err)
	return conf
}

func enterpriseResources() *service.Resources {
	res := service.MockResources()
	license.InjectTestService(res)
	return res
}

func TestSAPHANAInputConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		yaml        string
		errContains string
	}{
		{
			name: "valid bulk",
			yaml: `
dsn: hdb://user:pass@host:39017
mode: bulk
table: MY_TABLE
`,
		},
		{
			name: "valid query",
			yaml: `
dsn: hdb://user:pass@host:39017
mode: query
query: "SELECT * FROM MY_TABLE"
`,
		},
		{
			name: "valid incrementing",
			yaml: `
dsn: hdb://user:pass@host:39017
mode: incrementing
table: MY_TABLE
incrementing_column: ID
`,
		},
		{
			name: "bulk without table",
			yaml: `
dsn: hdb://user:pass@host:39017
mode: bulk
`,
			errContains: "table",
		},
		{
			name: "incrementing without table",
			yaml: `
dsn: hdb://user:pass@host:39017
mode: incrementing
incrementing_column: ID
`,
			errContains: "table",
		},
		{
			name: "incrementing without incrementing_column",
			yaml: `
dsn: hdb://user:pass@host:39017
mode: incrementing
table: MY_TABLE
`,
			errContains: "incrementing_column",
		},
		{
			name: "query without query field",
			yaml: `
dsn: hdb://user:pass@host:39017
mode: query
`,
			errContains: "query",
		},
		// Fields that are inert for the selected mode are rejected rather than
		// silently ignored: the reviewer's example was an incremental-intent
		// config that ran as a one-shot bulk scan because mode defaulted.
		{
			name: "bulk (default mode) with incrementing_column",
			yaml: `
dsn: hdb://user:pass@host:39017
table: ORDERS
incrementing_column: ID
`,
			errContains: "incrementing_column",
		},
		{
			name: "bulk with checkpoint_cache",
			yaml: `
dsn: hdb://user:pass@host:39017
mode: bulk
table: ORDERS
checkpoint_cache: redis_cache
`,
			errContains: "checkpoint_cache",
		},
		{
			name: "query with table",
			yaml: `
dsn: hdb://user:pass@host:39017
mode: query
query: "SELECT 1 FROM DUMMY"
table: ORDERS
`,
			errContains: "table",
		},
		{
			name: "incrementing with timestamp_column",
			yaml: `
dsn: hdb://user:pass@host:39017
mode: incrementing
table: ORDERS
incrementing_column: ID
timestamp_column: TS
`,
			errContains: "timestamp_column",
		},
		{
			name: "timestamp with query",
			yaml: `
dsn: hdb://user:pass@host:39017
mode: timestamp
table: ORDERS
timestamp_column: TS
query: "SELECT 1 FROM DUMMY"
`,
			errContains: "query",
		},
		{
			name: "timestamp with incrementing_initial_value",
			yaml: `
dsn: hdb://user:pass@host:39017
mode: timestamp
table: ORDERS
timestamp_column: TS
incrementing_initial_value: "5"
`,
			errContains: "incrementing_initial_value",
		},
		{
			name: "fetch_size below one",
			yaml: `
dsn: hdb://user:pass@host:39017
mode: bulk
table: MY_TABLE
fetch_size: 0
`,
			errContains: "fetch_size",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conf, err := sapHANAInputConfigSpec.ParseYAML(tc.yaml, nil)
			if err != nil {
				if tc.errContains != "" {
					require.ErrorContains(t, err, tc.errContains)
					return
				}
				require.NoError(t, err)
				return
			}

			_, err = newSAPHANAInput(conf, enterpriseResources())
			if tc.errContains != "" {
				require.ErrorContains(t, err, tc.errContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSAPHANAInputRequiresEnterpriseLicense(t *testing.T) {
	conf := parseInputConf(t, `
dsn: hdb://user:pass@host:39017
mode: bulk
table: MY_TABLE
`)
	_, err := newSAPHANAInput(conf, service.MockResources())
	require.Error(t, err)
}

// fakeLob mimics the value go-hdb yields for a LOB column scanned into *any:
// an object whose content is only reachable through Scan(io.Writer).
type fakeLob string

func (l fakeLob) Scan(wr io.Writer) error {
	_, err := io.WriteString(wr, string(l))
	return err
}

func decimalColType(t *testing.T, precision, scale int32) *schema.Common {
	t.Helper()
	c, err := schema.NewDecimal("col", precision, scale, false)
	require.NoError(t, err)
	return &c
}

func int64ColType() *schema.Common {
	c := schema.Common{Name: "col", Type: schema.Int64}
	return &c
}

func TestNormalizeHANAValue(t *testing.T) {
	ratFrom := func(s string) *big.Rat {
		r, ok := new(big.Rat).SetString(s)
		require.True(t, ok, "bad rat literal %q", s)
		return r
	}
	decVal := func(s string) gohdb.Decimal {
		return gohdb.Decimal(*ratFrom(s))
	}
	decPtr := func(s string) *gohdb.Decimal {
		d := decVal(s)
		return &d
	}

	tests := []struct {
		name           string
		input          any
		colType        *schema.Common
		numericMapping string
		want           any
	}{
		// ── []byte / string round-trips ────────────────────────────────────
		{
			name:  "unicode bytes to string",
			input: []byte("héllo wörld 日本語"),
			want:  "héllo wörld 日本語",
		},
		{
			// JSON marshalling would mangle invalid UTF-8 in a string into
			// U+FFFD; binary stays []byte so it base64-encodes losslessly.
			name:    "varbinary bytes stay bytes with ByteArray schema",
			input:   []byte{0xDE, 0xAD, 0xBE, 0xEF},
			colType: &schema.Common{Name: "col", Type: schema.ByteArray},
			want:    []byte{0xDE, 0xAD, 0xBE, 0xEF},
		},
		{
			name:  "invalid UTF-8 bytes stay bytes without schema",
			input: []byte{0xDE, 0xAD, 0xBE, 0xEF},
			want:  []byte{0xDE, 0xAD, 0xBE, 0xEF},
		},
		{
			name:    "text bytes to string with String schema",
			input:   []byte("plain text"),
			colType: &schema.Common{Name: "col", Type: schema.String},
			want:    "plain text",
		},
		{
			// A column the catalog calls text can still deliver bytes that
			// are not UTF-8; losing them is worse than a schema mismatch.
			name:    "invalid UTF-8 bytes stay bytes even with String schema",
			input:   []byte{0xDE, 0xAD, 0xBE, 0xEF},
			colType: &schema.Common{Name: "col", Type: schema.String},
			want:    []byte{0xDE, 0xAD, 0xBE, 0xEF},
		},
		// ── LOB columns (CLOB/NCLOB/TEXT/BLOB) arrive as a lob scanner ────
		{
			name:    "text LOB drained to string with String schema",
			input:   fakeLob("a long clob body"),
			colType: &schema.Common{Name: "col", Type: schema.String},
			want:    "a long clob body",
		},
		{
			name:    "binary LOB drained to bytes with ByteArray schema",
			input:   fakeLob("\xde\xad\xbe\xef"),
			colType: &schema.Common{Name: "col", Type: schema.ByteArray},
			want:    []byte{0xDE, 0xAD, 0xBE, 0xEF},
		},
		{
			name:  "text LOB drained to string without schema",
			input: fakeLob("clob without schema"),
			want:  "clob without schema",
		},
		{
			name:  "binary LOB stays bytes without schema",
			input: fakeLob("\xde\xad\xbe\xef"),
			want:  []byte{0xDE, 0xAD, 0xBE, 0xEF},
		},
		// ── integer passthrough ────────────────────────────────────────────
		{
			name:  "max int64 passthrough",
			input: int64(math.MaxInt64),
			want:  int64(math.MaxInt64),
		},
		{
			name:  "int64 zero passthrough",
			input: int64(0),
			want:  int64(0),
		},
		// ── time.Time passthrough ──────────────────────────────────────────
		{
			name:  "date lower boundary passthrough",
			input: time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
			want:  time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:  "date upper boundary passthrough",
			input: time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC),
			want:  time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC),
		},
		{
			name:  "sub-second timestamp nanoseconds preserved",
			input: time.Date(2024, 3, 15, 12, 34, 56, 123456789, time.UTC),
			want:  time.Date(2024, 3, 15, 12, 34, 56, 123456789, time.UTC),
		},
		// ── DECIMAL with Decimal(18,4) schema → canonical string ──────────
		{
			name:           "decimal value with Decimal schema",
			input:          decVal("123456.7890"),
			colType:        decimalColType(t, 18, 4),
			numericMapping: shNumericMappingNone,
			want:           "123456.7890",
		},
		{
			name:           "decimal pointer with Decimal schema",
			input:          decPtr("123456.7890"),
			colType:        decimalColType(t, 18, 4),
			numericMapping: shNumericMappingNone,
			want:           "123456.7890",
		},
		{
			name:           "decimal negative value with Decimal schema",
			input:          decVal("-0.0001"),
			colType:        decimalColType(t, 18, 4),
			numericMapping: shNumericMappingNone,
			want:           "-0.0001",
		},
		{
			// The cached schema can lag an online ALTER that widened the
			// scale; the value must not be silently rounded to the stale scale.
			name:           "decimal wider than cached schema scale is not truncated",
			input:          decVal("1.2345"),
			colType:        decimalColType(t, 12, 2),
			numericMapping: shNumericMappingNone,
			want:           "1.2345",
		},
		{
			name:           "decimal shorter than schema scale is padded",
			input:          decVal("1.5"),
			colType:        decimalColType(t, 12, 2),
			numericMapping: shNumericMappingNone,
			want:           "1.50",
		},
		// ── DECIMAL with Int64 schema (scale=0) → int64 ───────────────────
		{
			name:           "decimal integer with Int64 schema",
			input:          decVal("42"),
			colType:        int64ColType(),
			numericMapping: shNumericMappingNone,
			want:           int64(42),
		},
		{
			name:           "decimal overflows int64 with Int64 schema falls back to BigDecimal",
			input:          decVal("9999999999999999999"),
			colType:        int64ColType(),
			numericMapping: shNumericMappingNone,
			// 9999999999999999999 > math.MaxInt64 — IsInt64() guard prevents truncation.
			want: "9999999999999999999",
		},
		// ── nil DECIMAL pointer → nil ──────────────────────────────────────
		{
			name:  "nil *gohdb.Decimal → nil",
			input: (*gohdb.Decimal)(nil),
			want:  nil,
		},
		{
			name:  "nil *big.Rat → nil",
			input: (*big.Rat)(nil),
			want:  nil,
		},
		// ── DECIMAL without schema → BigDecimal canonical string ──────────
		// Note: big.Rat normalises fractions; trailing zeros in the fractional
		// part are absorbed (123456.7890 → 123456789/1000, scale 3 → "123456.789").
		{
			name:           "decimal no schema falls back to BigDecimal string",
			input:          decVal("123456.789"),
			colType:        nil,
			numericMapping: shNumericMappingNone,
			want:           "123456.789",
		},
		{
			name:           "decimal high precision no schema",
			input:          decVal("1234567890123456789.123456789"),
			colType:        nil,
			numericMapping: shNumericMappingNone,
			want:           "1234567890123456789.123456789",
		},
		// ── DECIMAL with best_fit mapping ──────────────────────────────────
		{
			// The schema layer never emits Decimal for p<=15 under best_fit,
			// but a stale/mismatched schema must still canonicalise safely.
			name:           "decimal with best_fit mapping and Decimal schema stays canonical",
			input:          decVal("99.99"),
			colType:        decimalColType(t, 10, 2),
			numericMapping: shNumericMappingBestFit,
			want:           "99.99",
		},
		{
			name:           "decimal with best_fit mapping and Float64 schema emits float",
			input:          decVal("99.99"),
			colType:        &schema.Common{Name: "col", Type: schema.Float64},
			numericMapping: shNumericMappingBestFit,
			want:           99.99,
		},
		{
			name:           "decimal best_fit no schema fits float64",
			input:          decVal("99.99"),
			colType:        nil,
			numericMapping: shNumericMappingBestFit,
			want:           99.99,
		},
		{
			name:           "decimal best_fit no schema integer emits int64",
			input:          decVal("42"),
			colType:        nil,
			numericMapping: shNumericMappingBestFit,
			want:           int64(42),
		},
		{
			name:           "decimal best_fit no schema too many digits stays canonical",
			input:          decVal("1234567890123456789.123456789"),
			colType:        nil,
			numericMapping: shNumericMappingBestFit,
			want:           "1234567890123456789.123456789",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeHANAValue(tc.input, tc.colType, tc.numericMapping)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestRatToNaturalDecimalString(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"1/1", "1"},
		{"1/10", "0.1"},
		{"123456789/10000", "12345.6789"},
		{"-7/100", "-0.07"},
		{"0/1", "0"},
		// go-hdb builds decimals with big.Rat.SetFrac, which reduces the
		// fraction: the denominator is then 2^a·5^b rather than a power of 10.
		{"1/2", "0.5"},
		{"51/4", "12.75"},
		{"157/50", "3.14"},
		{"1/8", "0.125"},
		{"-3/40", "-0.075"},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			r, ok := new(big.Rat).SetString(tc.input)
			require.True(t, ok)
			assert.Equal(t, tc.want, ratToNaturalDecimalString(r))
		})
	}
}

func TestSAPHANAInputTableRef(t *testing.T) {
	tests := []struct {
		name       string
		schemaName string
		tableName  string
		want       string
	}{
		{
			name:       "with schema",
			schemaName: "MY_SCHEMA",
			tableName:  "MY_TABLE",
			want:       `"MY_SCHEMA"."MY_TABLE"`,
		},
		{
			name:      "without schema",
			tableName: "MY_TABLE",
			want:      `"MY_TABLE"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &sapHANAInput{
				schemaName: tc.schemaName,
				tableName:  tc.tableName,
			}
			require.Equal(t, tc.want, s.tableRef())
		})
	}
}
