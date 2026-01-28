//
// Copyright Debezium Authors.
//
// Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
//

package dmlparser

import (
	"reflect"
	"testing"
	"time"
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
				if result != nil {
					t.Errorf("expected nil, got %v", result)
				}
				return
			}

			gotTime, ok := result.(time.Time)
			if !ok {
				t.Fatalf("expected time.Time, got %T", result)
			}

			if !gotTime.Equal(tt.wantTime) {
				t.Errorf("got %v, want %v", gotTime, tt.wantTime)
			}
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
				if result != nil {
					t.Errorf("expected nil, got %v", result)
				}
				return
			}

			gotTime, ok := result.(time.Time)
			if !ok {
				t.Fatalf("expected time.Time, got %T", result)
			}

			if !gotTime.Equal(tt.wantTime) {
				t.Errorf("got %v, want %v", gotTime, tt.wantTime)
			}
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
				if result != nil {
					t.Errorf("expected nil, got %v", result)
				}
				return
			}

			gotTime, ok := result.(time.Time)
			if !ok {
				t.Fatalf("expected time.Time, got %T", result)
			}

			if !gotTime.Equal(tt.wantTime) {
				t.Errorf("got %v, want %v", gotTime, tt.wantTime)
			}
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
				gotBytes, ok := result.([]byte)
				if !ok {
					t.Fatalf("expected []byte, got %T", result)
				}
				if !reflect.DeepEqual(gotBytes, tt.wantBytes) {
					t.Errorf("got %v, want %v", gotBytes, tt.wantBytes)
				}
			} else {
				gotStr, ok := result.(string)
				if !ok {
					t.Fatalf("expected string, got %T", result)
				}
				if gotStr != tt.wantStr {
					t.Errorf("got %v, want %v", gotStr, tt.wantStr)
				}
			}
		})
	}
}

func TestConvertLobValue(t *testing.T) {
	converter := NewOracleValueConverter(time.UTC)

	tests := []struct {
		name       string
		input      string
		wantEmpty  bool
		wantString bool
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
			name:       "regular string",
			input:      "some text",
			wantString: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := converter.convertLobValue(tt.input)

			if tt.wantEmpty {
				bytes, ok := result.([]byte)
				if !ok {
					t.Fatalf("expected []byte, got %T", result)
				}
				if len(bytes) != 0 {
					t.Errorf("expected empty bytes, got %v", bytes)
				}
			} else if tt.wantString {
				str, ok := result.(string)
				if !ok {
					t.Fatalf("expected string, got %T", result)
				}
				if str != tt.input {
					t.Errorf("got %v, want %v", str, tt.input)
				}
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
		wantType   string
		wantValue  any
	}{
		{
			name:       "DATE column with TO_DATE",
			input:      "TO_DATE('2020-01-15','YYYY-MM-DD')",
			columnType: "DATE",
			wantType:   "time.Time",
			wantValue:  time.Date(2020, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:       "TIMESTAMP column with TO_TIMESTAMP",
			input:      "TO_TIMESTAMP('2020-01-15 10:30:00','YYYY-MM-DD HH24:MI:SS')",
			columnType: "TIMESTAMP",
			wantType:   "time.Time",
			wantValue:  time.Date(2020, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			name:       "VARCHAR2 with regular string",
			input:      "Hello World",
			columnType: "VARCHAR2",
			wantType:   "string",
			wantValue:  "Hello World",
		},
		{
			name:       "NUMBER with integer",
			input:      "123",
			columnType: "NUMBER",
			wantType:   "int64",
			wantValue:  int64(123),
		},
		{
			name:       "NUMBER with float",
			input:      "123.456",
			columnType: "NUMBER",
			wantType:   "float64",
			wantValue:  123.456,
		},
		{
			name:       "RAW with HEXTORAW",
			input:      "HEXTORAW('48656C6C6F')",
			columnType: "RAW",
			wantType:   "[]uint8",
			wantValue:  []byte("Hello"),
		},
		{
			name:       "non-string value passes through",
			input:      123,
			columnType: "NUMBER",
			wantType:   "int",
			wantValue:  123,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := converter.ConvertValue(tt.input, tt.columnType)

			resultType := reflect.TypeOf(result).String()
			if resultType != tt.wantType {
				t.Errorf("got type %v, want %v", resultType, tt.wantType)
			}

			// For time.Time, use Equal method
			if wantTime, ok := tt.wantValue.(time.Time); ok {
				gotTime, ok := result.(time.Time)
				if !ok {
					t.Fatalf("expected time.Time, got %T", result)
				}
				if !gotTime.Equal(wantTime) {
					t.Errorf("got %v, want %v", gotTime, wantTime)
				}
			} else if !reflect.DeepEqual(result, tt.wantValue) {
				t.Errorf("got %v, want %v", result, tt.wantValue)
			}
		})
	}
}

func TestToKafkaValue(t *testing.T) {
	converter := NewOracleValueConverter(time.UTC)
	timestamp := time.Date(2020, 1, 15, 10, 30, 0, 123456789, time.UTC)

	tests := []struct {
		name          string
		input         any
		columnType    string
		precisionMode temporalPrecisionMode
		wantValue     any
	}{
		{
			name:          "timestamp to milliseconds",
			input:         timestamp,
			columnType:    "TIMESTAMP",
			precisionMode: temporalPrecisionConnect,
			wantValue:     int64(1579084200123),
		},
		{
			name:          "timestamp to microseconds",
			input:         timestamp,
			columnType:    "TIMESTAMP",
			precisionMode: temporalPrecisionAdaptiveTimeMicros,
			wantValue:     int64(1579084200123456),
		},
		{
			name:          "timestamp to nanoseconds",
			input:         timestamp,
			columnType:    "TIMESTAMP",
			precisionMode: temporalPrecisionAdaptive,
			wantValue:     int64(1579084200123456789),
		},
		{
			name:          "byte array to hex string",
			input:         []byte{0x48, 0x65, 0x6C, 0x6C, 0x6F},
			columnType:    "RAW",
			precisionMode: temporalPrecisionConnect,
			wantValue:     "48656c6c6f",
		},
		{
			name:          "string passes through",
			input:         "Hello World",
			columnType:    "VARCHAR2",
			precisionMode: temporalPrecisionConnect,
			wantValue:     "Hello World",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := converter.ToKafkaValue(tt.input, tt.columnType, tt.precisionMode)

			if !reflect.DeepEqual(result, tt.wantValue) {
				t.Errorf("got %v (type %T), want %v (type %T)", result, result, tt.wantValue, tt.wantValue)
			}
		})
	}
}

func TestIsOracleFunctionCall(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{
			name:  "TO_DATE",
			input: "TO_DATE('2020-01-15','YYYY-MM-DD')",
			want:  true,
		},
		{
			name:  "TO_TIMESTAMP",
			input: "TO_TIMESTAMP('2020-01-15 10:30:00')",
			want:  true,
		},
		{
			name:  "TO_TIMESTAMP_TZ",
			input: "TO_TIMESTAMP_TZ('2020-01-15 10:30:00 +00:00')",
			want:  true,
		},
		{
			name:  "HEXTORAW",
			input: "HEXTORAW('48656C6C6F')",
			want:  true,
		},
		{
			name:  "EMPTY_CLOB",
			input: "EMPTY_CLOB()",
			want:  true,
		},
		{
			name:  "EMPTY_BLOB",
			input: "EMPTY_BLOB()",
			want:  true,
		},
		{
			name:  "regular string",
			input: "Hello World",
			want:  false,
		},
		{
			name:  "date-like string",
			input: "2020-01-15",
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsOracleFunctionCall(tt.input)
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
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
