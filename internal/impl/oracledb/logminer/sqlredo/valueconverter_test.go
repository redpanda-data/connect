// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package sqlredo

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConvertDateValue(t *testing.T) {
	converter := NewOracleValueConverter(time.UTC)

	tests := []struct {
		name     string
		input    string
		wantTime time.Time
		wantNil  bool
	}{
		{
			name:     "TO_DATE with standard format",
			input:    "TO_DATE('2020-01-15','YYYY-MM-DD')",
			wantTime: time.Date(2020, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "TO_DATE with timestamp",
			input:    "TO_DATE('2020-01-15 10:30:00','YYYY-MM-DD HH24:MI:SS')",
			wantTime: time.Date(2020, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			name:     "TO_DATE with month name",
			input:    "TO_DATE('15-Jan-20','DD-MON-YY')",
			wantTime: time.Date(2020, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:    "not a TO_DATE call",
			input:   "2020-01-15",
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := converter.convertDateValue(tt.input)
			if tt.wantNil {
				assert.Nil(t, result)
				return
			}
			assert.Equal(t, tt.wantTime, result)
		})
	}
}

func TestConvertTimestampValue(t *testing.T) {
	converter := NewOracleValueConverter(time.UTC)

	tests := []struct {
		name     string
		input    string
		wantTime time.Time
		wantNil  bool
	}{
		{
			name:     "TO_TIMESTAMP without fractional seconds",
			input:    "TO_TIMESTAMP('2020-01-15 10:30:00','YYYY-MM-DD HH24:MI:SS')",
			wantTime: time.Date(2020, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			name:     "TO_TIMESTAMP with milliseconds",
			input:    "TO_TIMESTAMP('2020-01-15 10:30:00.123','YYYY-MM-DD HH24:MI:SS.FF3')",
			wantTime: time.Date(2020, 1, 15, 10, 30, 0, 123000000, time.UTC),
		},
		{
			name:     "TO_TIMESTAMP with microseconds",
			input:    "TO_TIMESTAMP('2020-01-15 10:30:00.123456','YYYY-MM-DD HH24:MI:SS.FF6')",
			wantTime: time.Date(2020, 1, 15, 10, 30, 0, 123456000, time.UTC),
		},
		{
			name:     "TO_TIMESTAMP with nanoseconds",
			input:    "TO_TIMESTAMP('2020-01-15 10:30:00.123456789','YYYY-MM-DD HH24:MI:SS.FF9')",
			wantTime: time.Date(2020, 1, 15, 10, 30, 0, 123456789, time.UTC),
		},
		{
			name:     "TO_TIMESTAMP without format string",
			input:    "TO_TIMESTAMP('2020-01-15 10:30:00')",
			wantTime: time.Date(2020, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			name:     "TO_TIMESTAMP with AM/PM format",
			input:    "TO_TIMESTAMP('15-Jan-20 10.30.00 AM')",
			wantTime: time.Date(2020, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			name:    "not a TO_TIMESTAMP call",
			input:   "2020-01-15 10:30:00",
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := converter.convertTimestampValue(tt.input)
			if tt.wantNil {
				assert.Nil(t, result)
				return
			}
			assert.Equal(t, tt.wantTime, result)
		})
	}
}

func TestConvertTimestampWithZone(t *testing.T) {
	converter := NewOracleValueConverter(time.UTC)

	tests := []struct {
		name     string
		input    string
		wantTime time.Time
		wantNil  bool
	}{
		{
			name:     "TO_TIMESTAMP_TZ with UTC",
			input:    "TO_TIMESTAMP_TZ('2020-01-15 10:30:00 +00:00')",
			wantTime: time.Date(2020, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			name:     "TO_TIMESTAMP_TZ with offset",
			input:    "TO_TIMESTAMP_TZ('2020-01-15 10:30:00 -05:00')",
			wantTime: time.Date(2020, 1, 15, 15, 30, 0, 0, time.UTC),
		},
		{
			name:     "TO_TIMESTAMP_TZ with microseconds",
			input:    "TO_TIMESTAMP_TZ('2020-01-15 10:30:00.123456 +00:00')",
			wantTime: time.Date(2020, 1, 15, 10, 30, 0, 123456000, time.UTC),
		},
		{
			name:    "not a TO_TIMESTAMP_TZ call",
			input:   "2020-01-15 10:30:00",
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := converter.convertTimestampWithZone(tt.input)
			if tt.wantNil {
				assert.Nil(t, result)
				return
			}
			// convertTimestampWithZone preserves the parsed timezone rather than
			// normalising to UTC, so compare the instant with time.Equal rather
			// than the full time.Time value (which includes the location).
			gotTime, ok := result.(time.Time)
			assert.True(t, ok, "expected time.Time, got %T", result)
			assert.True(t, gotTime.Equal(tt.wantTime), "got %v, want %v", gotTime, tt.wantTime)
		})
	}
}

func TestConvertRawValue(t *testing.T) {
	converter := NewOracleValueConverter(time.UTC)

	tests := []struct {
		name      string
		input     string
		wantBytes []byte
		wantStr   string
	}{
		{
			name:      "HEXTORAW simple",
			input:     "HEXTORAW('48656C6C6F')",
			wantBytes: []byte("Hello"),
		},
		{
			name:      "HEXTORAW with lowercase",
			input:     "hextoraw('776f726c64')",
			wantBytes: []byte("world"),
		},
		{
			name:    "not a HEXTORAW call",
			input:   "48656C6C6F",
			wantStr: "48656C6C6F",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := converter.convertRawValue(tt.input)
			if tt.wantBytes != nil {
				assert.Equal(t, tt.wantBytes, result)
			} else {
				assert.Equal(t, tt.wantStr, result)
			}
		})
	}
}

func TestConvertLobValue(t *testing.T) {
	converter := NewOracleValueConverter(time.UTC)

	tests := []struct {
		name      string
		input     string
		wantEmpty bool
		wantStr   string
	}{
		{
			name:      "EMPTY_CLOB()",
			input:     "EMPTY_CLOB()",
			wantEmpty: true,
		},
		{
			name:      "EMPTY_BLOB()",
			input:     "EMPTY_BLOB()",
			wantEmpty: true,
		},
		{
			name:    "regular string",
			input:   "some text",
			wantStr: "some text",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := converter.convertLobValue(tt.input)
			if tt.wantEmpty {
				assert.IsType(t, []byte{}, result)
				assert.Empty(t, result)
			} else {
				assert.Equal(t, tt.wantStr, result)
			}
		})
	}
}

func TestConvertValue(t *testing.T) {
	converter := NewOracleValueConverter(time.UTC)

	tests := []struct {
		name       string
		input      any
		columnType string
		wantValue  any
	}{
		{
			name:       "DATE column with TO_DATE",
			input:      "TO_DATE('2020-01-15','YYYY-MM-DD')",
			columnType: "DATE",
			wantValue:  time.Date(2020, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:       "TIMESTAMP column with TO_TIMESTAMP",
			input:      "TO_TIMESTAMP('2020-01-15 10:30:00','YYYY-MM-DD HH24:MI:SS')",
			columnType: "TIMESTAMP",
			wantValue:  time.Date(2020, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			name:       "VARCHAR2 with regular string",
			input:      "Hello World",
			columnType: "VARCHAR2",
			wantValue:  "Hello World",
		},
		{
			name:       "NUMBER with integer",
			input:      "123",
			columnType: "NUMBER",
			wantValue:  int64(123),
		},
		{
			name:       "NUMBER with float",
			input:      "123.456",
			columnType: "NUMBER",
			wantValue:  123.456,
		},
		{
			name:       "RAW with HEXTORAW",
			input:      "HEXTORAW('48656C6C6F')",
			columnType: "RAW",
			wantValue:  []byte("Hello"),
		},
		{
			name:       "non-string value passes through",
			input:      123,
			columnType: "NUMBER",
			wantValue:  123,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := converter.ConvertValue(tt.input, tt.columnType)
			assert.IsType(t, tt.wantValue, result)
			assert.Equal(t, tt.wantValue, result)
		})
	}
}

// Benchmark tests
func BenchmarkConvertTimestamp(b *testing.B) {
	converter := NewOracleValueConverter(time.UTC)
	input := "TO_TIMESTAMP('2020-01-15 10:30:00.123456','YYYY-MM-DD HH24:MI:SS.FF6')"

	for b.Loop() {
		converter.ConvertValue(input, "TIMESTAMP")
	}
}

func BenchmarkConvertDate(b *testing.B) {
	converter := NewOracleValueConverter(time.UTC)
	input := "TO_DATE('2020-01-15','YYYY-MM-DD')"

	for b.Loop() {
		converter.ConvertValue(input, "DATE")
	}
}

func BenchmarkConvertRaw(b *testing.B) {
	converter := NewOracleValueConverter(time.UTC)
	input := "HEXTORAW('48656C6C6F576F726C64')"

	for b.Loop() {
		converter.ConvertValue(input, "RAW")
	}
}
