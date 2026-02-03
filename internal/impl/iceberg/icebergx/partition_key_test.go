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
	"fmt"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to create a test schema with all primitive types
func makeTestSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "test_bool", Type: iceberg.PrimitiveTypes.Bool, Required: true},
		iceberg.NestedField{ID: 2, Name: "test_int", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 3, Name: "test_long", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 4, Name: "test_float", Type: iceberg.PrimitiveTypes.Float32, Required: true},
		iceberg.NestedField{ID: 5, Name: "test_double", Type: iceberg.PrimitiveTypes.Float64, Required: true},
		iceberg.NestedField{ID: 6, Name: "test_decimal", Type: iceberg.DecimalTypeOf(9, 2), Required: true},
		iceberg.NestedField{ID: 7, Name: "test_date", Type: iceberg.PrimitiveTypes.Date, Required: true},
		iceberg.NestedField{ID: 8, Name: "test_time", Type: iceberg.PrimitiveTypes.Time, Required: true},
		iceberg.NestedField{ID: 9, Name: "test_timestamp", Type: iceberg.PrimitiveTypes.Timestamp, Required: true},
		iceberg.NestedField{ID: 10, Name: "test_timestamptz", Type: iceberg.PrimitiveTypes.TimestampTz, Required: true},
		iceberg.NestedField{ID: 11, Name: "test_string", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 12, Name: "test_uuid", Type: iceberg.PrimitiveTypes.UUID, Required: true},
		iceberg.NestedField{ID: 13, Name: "test_fixed", Type: iceberg.FixedTypeOf(11), Required: true},
		iceberg.NestedField{ID: 14, Name: "test_binary", Type: iceberg.PrimitiveTypes.Binary, Required: true},
	)
}

// Helper to create partition key and convert to path
func partitionKeyToPath(t *testing.T, spec iceberg.PartitionSpec, schema *iceberg.Schema, values []parquet.Value) string {
	key, err := NewPartitionKey(spec, schema, values)
	require.NoError(t, err)

	result, err := PartitionKeyToPath(spec, key)
	require.NoError(t, err)

	return result
}

// TestIdentityTransform tests identity transforms for all primitive types.
// This corresponds to TestIdentityTransform in the C++ tests.
func TestIdentityTransform(t *testing.T) {
	schema := makeTestSchema()

	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceID: 1, FieldID: 1000, Name: "bool_partition", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceID: 2, FieldID: 1001, Name: "int_partition", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceID: 3, FieldID: 1002, Name: "long_test_partition", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceID: 4, FieldID: 1003, Name: "fl_partition", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceID: 5, FieldID: 1004, Name: "d_partition", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceID: 6, FieldID: 1005, Name: "decimal_partition", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceID: 7, FieldID: 1006, Name: "date_identity", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceID: 8, FieldID: 1007, Name: "time_identity", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceID: 9, FieldID: 1008, Name: "timestamp_identity", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceID: 10, FieldID: 1009, Name: "timestamptz_identity", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceID: 11, FieldID: 1010, Name: "string_identity", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceID: 12, FieldID: 1011, Name: "uuid_identity", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceID: 13, FieldID: 1012, Name: "fixed_identity", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceID: 14, FieldID: 1013, Name: "binary_identity", Transform: iceberg.IdentityTransform{}},
	)

	// Create partition values matching the C++ test
	testUUID, _ := uuid.Parse("f47ac10b-58cc-4372-a567-0e02b2c3d479")

	values := []parquet.Value{
		parquet.BooleanValue(true),                            // bool: true
		parquet.Int32Value(128),                               // int: 128
		parquet.Int64Value(4096),                              // long: 4096
		parquet.FloatValue(3.1415),                            // float: 3.1415
		parquet.DoubleValue(2.7182),                           // double: 2.7182
		parquet.Int32Value(1231123),                           // decimal: 1231123 (stored as int32 for small precision)
		parquet.Int32Value(20140),                             // date: 20140 days from epoch = 2025-02-21
		parquet.Int64Value(52_995_167_000),                    // time: 14:43:15.167 in microseconds (14*3600 + 43*60 + 15)*1e6 + 167*1e3
		parquet.Int64Value(1740143929000000),                  // timestamp: 2025-02-21T13:18:49 in microseconds
		parquet.Int64Value(1740143929000000),                  // timestamptz: 2025-02-21T13:18:49 in microseconds
		parquet.ByteArrayValue([]byte("test_string_value")),   // string
		parquet.FixedLenByteArrayValue(testUUID[:]),           // uuid
		parquet.FixedLenByteArrayValue([]byte("Hello world")), // fixed
		parquet.ByteArrayValue([]byte("PandasAreCuties")),     // binary
	}

	result := partitionKeyToPath(t, spec, schema, values)

	// iceberg-go's ToHumanStr formats:
	// - Timestamp without Z/+0000 suffix
	// - Time with format 15:04:05.999999 (omits trailing zeros)
	expected := "bool_partition=true/" +
		"int_partition=128/" +
		"long_test_partition=4096/" +
		"fl_partition=3.1415/" +
		"d_partition=2.7182/" +
		"decimal_partition=1231123/" +
		"date_identity=2025-02-21/" +
		"time_identity=14:43:15.167/" +
		"timestamp_identity=2025-02-21T13:18:49/" +
		"timestamptz_identity=2025-02-21T13:18:49/" +
		"string_identity=test_string_value/" +
		"uuid_identity=f47ac10b-58cc-4372-a567-0e02b2c3d479/" +
		"fixed_identity=SGVsbG8gd29ybGQ=/" +
		"binary_identity=UGFuZGFzQXJlQ3V0aWVz"

	assert.Equal(t, expected, result)
}

// TestTimestampTransform tests timestamp formatting with different precision levels.
// This corresponds to TestTimestampTransform in the C++ tests.
func TestTimestampTransform(t *testing.T) {
	schema := makeTestSchema()

	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceID: 9, FieldID: 1000, Name: "timestamp_no_ms", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceID: 9, FieldID: 1001, Name: "timestamp_ms", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceID: 9, FieldID: 1002, Name: "timestamp_us", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceID: 10, FieldID: 1003, Name: "timestamp_tz_no_ms", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceID: 10, FieldID: 1004, Name: "timestamp_tz_ms", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceID: 10, FieldID: 1005, Name: "timestamp_tz_us", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceID: 8, FieldID: 1006, Name: "time_s", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceID: 8, FieldID: 1007, Name: "time_ms", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceID: 8, FieldID: 1008, Name: "time_us", Transform: iceberg.IdentityTransform{}},
	)

	values := []parquet.Value{
		// Timestamps: 2025-02-10 10:37:13 with different precisions
		parquet.Int64Value(1739183833000000), // 10-02-2025 10:37:13 (no subseconds)
		parquet.Int64Value(1739183833321000), // 10-02-2025 10:37:13.321
		parquet.Int64Value(1739183833321123), // 10-02-2025 10:37:13.321123

		// Timestamptz: same values
		parquet.Int64Value(1739183833000000),
		parquet.Int64Value(1739183833321000),
		parquet.Int64Value(1739183833321123),

		// Time: 11:11:11 with different precisions
		parquet.Int64Value(40271000000), // 11:11:11 (no subseconds)
		parquet.Int64Value(40271456000), // 11:11:11.456
		parquet.Int64Value(40271000789), // 11:11:11.000789
	}

	result := partitionKeyToPath(t, spec, schema, values)

	// iceberg-go's ToHumanStr uses format "2006-01-02T15:04:05.999999" (no Z suffix)
	// and "15:04:05.999999" for time (omits trailing zeros)
	expected := "timestamp_no_ms=2025-02-10T10:37:13/" +
		"timestamp_ms=2025-02-10T10:37:13.321/" +
		"timestamp_us=2025-02-10T10:37:13.321123/" +
		"timestamp_tz_no_ms=2025-02-10T10:37:13/" +
		"timestamp_tz_ms=2025-02-10T10:37:13.321/" +
		"timestamp_tz_us=2025-02-10T10:37:13.321123/" +
		"time_s=11:11:11/" +
		"time_ms=11:11:11.456/" +
		"time_us=11:11:11.000789"

	assert.Equal(t, expected, result)
}

// TestTimeTransforms tests year, month, day, and hour transforms.
// This corresponds to TimeTransformsTest in the C++ tests.
func TestTimeTransforms(t *testing.T) {
	schema := makeTestSchema()

	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceID: 9, FieldID: 1000, Name: "year_transform", Transform: iceberg.YearTransform{}},
		iceberg.PartitionField{SourceID: 9, FieldID: 1001, Name: "month_transform", Transform: iceberg.MonthTransform{}},
		iceberg.PartitionField{SourceID: 9, FieldID: 1002, Name: "day_transform", Transform: iceberg.DayTransform{}},
		iceberg.PartitionField{SourceID: 9, FieldID: 1003, Name: "hour_transform", Transform: iceberg.HourTransform{}},
	)

	// Raw timestamp value: 2025-02-24 11:30:00 UTC in microseconds since epoch
	// All transforms will be applied to this same timestamp
	ts := int64(1740397800000000) // 2025-02-24 11:30:00 UTC

	values := []parquet.Value{
		parquet.Int64Value(ts), // -> year 2025
		parquet.Int64Value(ts), // -> month 2025-02
		parquet.Int64Value(ts), // -> day 2025-02-24
		parquet.Int64Value(ts), // -> hour 2025-02-24-11
	}

	result := partitionKeyToPath(t, spec, schema, values)

	expected := "year_transform=2025/" +
		"month_transform=2025-02/" +
		"day_transform=2025-02-24/" +
		"hour_transform=2025-02-24-11"

	assert.Equal(t, expected, result)
}

// TestVoidTransform tests that void transforms always return "null".
// This corresponds to VoidTransformTest in the C++ tests.
func TestVoidTransform(t *testing.T) {
	schema := makeTestSchema()

	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceID: 2, FieldID: 1000, Name: "void_transform", Transform: iceberg.VoidTransform{}},
	)

	// Void transform should return "null" regardless of input value
	values := []parquet.Value{
		parquet.Int32Value(42), // any value - void transform ignores it
	}

	result := partitionKeyToPath(t, spec, schema, values)

	assert.Equal(t, "void_transform=null", result)
}

// TestBucketTransform tests bucket transform formatting.
// This corresponds to BucketTransformTest in the C++ tests.
func TestBucketTransform(t *testing.T) {
	schema := makeTestSchema()

	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceID: 2, FieldID: 1000, Name: "bucket_transform", Transform: iceberg.BucketTransform{NumBuckets: 16}},
	)

	// Raw int value - bucket transform will compute bucket number
	values := []parquet.Value{
		parquet.Int32Value(100), // bucket(100, 16) will compute a bucket 0-15
	}

	key, err := NewPartitionKey(spec, schema, values)
	require.NoError(t, err)

	// Verify bucket result is in valid range [0, 16)
	require.True(t, key[0].Valid)
	bucketVal := key[0].Val.Any().(int32)
	assert.GreaterOrEqual(t, bucketVal, int32(0))
	assert.Less(t, bucketVal, int32(16))
}

// TestElementSizeLimiting tests that individual partition values are truncated to 64 bytes.
// This corresponds to TestElementSizeLimiting in the C++ tests.
func TestElementSizeLimiting(t *testing.T) {
	schema := makeTestSchema()

	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceID: 11, FieldID: 1000, Name: "identity_string", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceID: 14, FieldID: 1001, Name: "identity_binary", Transform: iceberg.IdentityTransform{}},
	)

	longString := "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Pellentesque ipsum magna, pellentesque quis nisl eu, congue aliquam id."

	values := []parquet.Value{
		parquet.ByteArrayValue([]byte(longString)),
		parquet.ByteArrayValue([]byte(longString)),
	}

	result := partitionKeyToPath(t, spec, schema, values)

	// String should be truncated to 64 bytes: "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Pellent"
	// Binary should be truncated to 64 bytes and base64 encoded
	expected := "identity_string=Lorem%20ipsum%20dolor%20sit%20amet%2C%20consectetur%20adipiscing%20elit.%20Pellent/" +
		"identity_binary=TG9yZW0gaXBzdW0gZG9sb3Igc2l0IGFtZXQsIGNvbnNlY3RldHVyIGFkaXBpc2NpbmcgZWxpdC4gUGVsbGVudA=="

	assert.Equal(t, expected, result)
}

// TestPathSizeLimiting tests that the total path is truncated to 512 bytes.
// This corresponds to TestPathSizeLimitting in the C++ tests.
func TestPathSizeLimiting(t *testing.T) {
	schema := makeTestSchema()

	// Create 64 partition fields
	fields := make([]iceberg.PartitionField, 64)
	for i := range 64 {
		fields[i] = iceberg.PartitionField{
			SourceID:  2,
			FieldID:   1000 + i,
			Name:      fmt.Sprintf("identity_int_%d", i),
			Transform: iceberg.IdentityTransform{},
		}
	}
	spec := iceberg.NewPartitionSpec(fields...)

	// Create 64 values
	values := make([]parquet.Value, 64)
	for i := range 64 {
		values[i] = parquet.Int32Value(int32(i))
	}

	result := partitionKeyToPath(t, spec, schema, values)

	// Ensure path is at most 512 bytes
	assert.LessOrEqual(t, len(result), maxPathLength)

	// Path should end with a complete segment
	assert.True(t, strings.HasSuffix(result, "identity_int_27=27"))
}

// TestSpecValuesMismatch tests that an error is returned when the number of values
// doesn't match the number of partition fields.
func TestSpecValuesMismatch(t *testing.T) {
	schema := makeTestSchema()

	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceID: 1, FieldID: 1000, Name: "bool_partition", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceID: 2, FieldID: 1001, Name: "int_partition", Transform: iceberg.IdentityTransform{}},
	)

	// Only provide one value when two are expected
	values := []parquet.Value{
		parquet.BooleanValue(true),
	}

	_, err := NewPartitionKey(spec, schema, values)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mismatch")
}

// TestEmptyPartitionSpec tests that an empty partition spec returns an empty path.
func TestEmptyPartitionSpec(t *testing.T) {
	schema := makeTestSchema()
	spec := iceberg.NewPartitionSpec()

	key, err := NewPartitionKey(spec, schema, []parquet.Value{})
	require.NoError(t, err)

	result, err := PartitionKeyToPath(spec, key)
	require.NoError(t, err)
	assert.Empty(t, result)
}

// TestNullValues tests that null values are formatted as "null".
func TestNullValues(t *testing.T) {
	schema := makeTestSchema()

	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceID: 2, FieldID: 1000, Name: "null_int", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceID: 11, FieldID: 1001, Name: "null_string", Transform: iceberg.IdentityTransform{}},
	)

	values := []parquet.Value{
		parquet.NullValue(),
		parquet.NullValue(),
	}

	result := partitionKeyToPath(t, spec, schema, values)

	assert.Equal(t, "null_int=null/null_string=null", result)
}

// TestTruncateTransform tests truncate transform formatting.
func TestTruncateTransform(t *testing.T) {
	schema := makeTestSchema()

	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceID: 2, FieldID: 1000, Name: "truncate_int", Transform: iceberg.TruncateTransform{Width: 10}},
		iceberg.PartitionField{SourceID: 11, FieldID: 1001, Name: "truncate_string", Transform: iceberg.TruncateTransform{Width: 5}},
	)

	// Raw values - truncate transform will be applied
	values := []parquet.Value{
		parquet.Int32Value(128),                       // truncate(128, 10) = 120
		parquet.ByteArrayValue([]byte("Hello World")), // truncate("Hello World", 5) = "Hello"
	}

	result := partitionKeyToPath(t, spec, schema, values)

	assert.Equal(t, "truncate_int=120/truncate_string=Hello", result)
}

// TestURLEncoding tests that special characters are properly URL-encoded.
func TestURLEncoding(t *testing.T) {
	schema := makeTestSchema()

	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceID: 11, FieldID: 1000, Name: "special/chars", Transform: iceberg.IdentityTransform{}},
	)

	values := []parquet.Value{
		parquet.ByteArrayValue([]byte("hello world&foo=bar")),
	}

	result := partitionKeyToPath(t, spec, schema, values)

	// Both field name and value should be URL-encoded (PathEscape encoding)
	assert.Equal(t, "special%2Fchars=hello%20world&foo=bar", result)
}

// TestNewPartitionKey tests the NewPartitionKey function directly.
func TestNewPartitionKey(t *testing.T) {
	schema := makeTestSchema()

	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceID: 1, FieldID: 1000, Name: "bool_partition", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceID: 2, FieldID: 1001, Name: "int_partition", Transform: iceberg.IdentityTransform{}},
	)

	values := []parquet.Value{
		parquet.BooleanValue(true),
		parquet.Int32Value(42),
	}

	key, err := NewPartitionKey(spec, schema, values)
	require.NoError(t, err)

	assert.Len(t, key, 2)
	assert.True(t, key[0].Valid)
	assert.True(t, key[1].Valid)
	assert.Equal(t, true, key[0].Val.Any())
	assert.Equal(t, int32(42), key[1].Val.Any())
}

// TestPartitionKeyWithNulls tests that null values in PartitionKey are handled correctly.
func TestPartitionKeyWithNulls(t *testing.T) {
	schema := makeTestSchema()

	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceID: 2, FieldID: 1000, Name: "int_partition", Transform: iceberg.IdentityTransform{}},
	)

	values := []parquet.Value{
		parquet.NullValue(),
	}

	key, err := NewPartitionKey(spec, schema, values)
	require.NoError(t, err)

	assert.Len(t, key, 1)
	assert.False(t, key[0].Valid)

	result, err := PartitionKeyToPath(spec, key)
	require.NoError(t, err)
	assert.Equal(t, "int_partition=null", result)
}

// ============================================================================
// ParsePartitionSpec tests - matching Redpanda broker's partition_spec_parser_test.cc
// ============================================================================

// TestParsePartitionSpecEmpty tests parsing empty partition specs.
// Corresponds to empty spec tests in partition_spec_parser_test.cc.
func TestParsePartitionSpecEmpty(t *testing.T) {
	schema := makeTestSchema()

	testCases := []string{
		"",
		"()",
		"( )",
		"   (  )  ",
		"\t\r\n",
	}

	for _, input := range testCases {
		t.Run(fmt.Sprintf("input=%q", input), func(t *testing.T) {
			spec, err := ParsePartitionSpec(input, schema)
			require.NoError(t, err, "input: %q", input)
			assert.Equal(t, 0, spec.NumFields(), "expected empty spec for input: %q", input)
		})
	}
}

// TestParsePartitionSpecIdentity tests parsing identity transforms.
// Corresponds to single field identity tests in partition_spec_parser_test.cc.
func TestParsePartitionSpecIdentity(t *testing.T) {
	schema := makeTestSchema()

	testCases := []struct {
		input      string
		expectName string
	}{
		{"(test_int)", "test_int"},
		{"test_int", "test_int"},
		{"  test_int  ", "test_int"},
		{"(  test_int  )", "test_int"},
		{"`test_int`", "test_int"},
		{"identity(test_int)", "test_int"}, // explicit identity transform
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("input=%q", tc.input), func(t *testing.T) {
			spec, err := ParsePartitionSpec(tc.input, schema)
			require.NoError(t, err, "input: %q", tc.input)
			require.Equal(t, 1, spec.NumFields())

			field := spec.Field(0)
			assert.Equal(t, tc.expectName, field.Name)
			assert.IsType(t, iceberg.IdentityTransform{}, field.Transform)
		})
	}
}

// TestParsePartitionSpecMultipleFields tests parsing multiple fields.
func TestParsePartitionSpecMultipleFields(t *testing.T) {
	schema := makeTestSchema()

	spec, err := ParsePartitionSpec("(test_int, test_string)", schema)
	require.NoError(t, err)
	require.Equal(t, 2, spec.NumFields())

	assert.Equal(t, "test_int", spec.Field(0).Name)
	assert.Equal(t, 2, spec.Field(0).SourceID) // test_int has ID 2
	assert.IsType(t, iceberg.IdentityTransform{}, spec.Field(0).Transform)

	assert.Equal(t, "test_string", spec.Field(1).Name)
	assert.Equal(t, 11, spec.Field(1).SourceID) // test_string has ID 11
	assert.IsType(t, iceberg.IdentityTransform{}, spec.Field(1).Transform)
}

// TestParsePartitionSpecTimeTransforms tests parsing time-based transforms.
// Corresponds to time transform tests in partition_spec_parser_test.cc.
func TestParsePartitionSpecTimeTransforms(t *testing.T) {
	schema := makeTestSchema()

	testCases := []struct {
		input         string
		expectName    string
		transformType iceberg.Transform
	}{
		{"year(test_timestamp)", "test_timestamp", iceberg.YearTransform{}},
		{"YEAR(test_timestamp)", "test_timestamp", iceberg.YearTransform{}},
		{"month(test_timestamp)", "test_timestamp", iceberg.MonthTransform{}},
		{"day(test_timestamp)", "test_timestamp", iceberg.DayTransform{}},
		{"hour(test_timestamp)", "test_timestamp", iceberg.HourTransform{}},
		{"void(test_int)", "test_int", iceberg.VoidTransform{}},
		{"year(test_timestamp) as ts_year", "ts_year", iceberg.YearTransform{}},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("input=%q", tc.input), func(t *testing.T) {
			spec, err := ParsePartitionSpec(tc.input, schema)
			require.NoError(t, err)
			require.Equal(t, 1, spec.NumFields())

			field := spec.Field(0)
			assert.Equal(t, tc.expectName, field.Name)
			assert.Equal(t, tc.transformType, field.Transform)
		})
	}
}

// TestParsePartitionSpecBucketTransform tests parsing bucket transforms.
// Corresponds to bucket transform tests in partition_spec_parser_test.cc.
func TestParsePartitionSpecBucketTransform(t *testing.T) {
	schema := makeTestSchema()

	testCases := []struct {
		input      string
		numBuckets int
	}{
		{"bucket(16, test_int)", 16},
		{"bucket(0, test_int)", 0},
		{"bucket(1000000, test_int)", 1000000},
		{"BUCKET(32, test_string)", 32},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("input=%q", tc.input), func(t *testing.T) {
			spec, err := ParsePartitionSpec(tc.input, schema)
			require.NoError(t, err)
			require.Equal(t, 1, spec.NumFields())

			field := spec.Field(0)
			bucket, ok := field.Transform.(iceberg.BucketTransform)
			require.True(t, ok, "expected BucketTransform")
			assert.Equal(t, tc.numBuckets, bucket.NumBuckets)
		})
	}
}

// TestParsePartitionSpecTruncateTransform tests parsing truncate transforms.
// Corresponds to truncate transform tests in partition_spec_parser_test.cc.
func TestParsePartitionSpecTruncateTransform(t *testing.T) {
	schema := makeTestSchema()

	testCases := []struct {
		input      string
		width      int
		expectName string
	}{
		{"truncate(10, test_int)", 10, "test_int"},
		{"truncate(9000, test_string)", 9000, "test_string"},
		{"TRUNCATE(5, test_int)", 5, "test_int"},
		{"truncate(10, test_int) as int_trunc", 10, "int_trunc"},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("input=%q", tc.input), func(t *testing.T) {
			spec, err := ParsePartitionSpec(tc.input, schema)
			require.NoError(t, err)
			require.Equal(t, 1, spec.NumFields())

			field := spec.Field(0)
			assert.Equal(t, tc.expectName, field.Name)
			trunc, ok := field.Transform.(iceberg.TruncateTransform)
			require.True(t, ok, "expected TruncateTransform")
			assert.Equal(t, tc.width, trunc.Width)
		})
	}
}

// TestParsePartitionSpecWithAlias tests parsing partition specs with aliases.
// Corresponds to alias tests in partition_spec_parser_test.cc.
func TestParsePartitionSpecWithAlias(t *testing.T) {
	schema := makeTestSchema()

	testCases := []struct {
		input      string
		expectName string
	}{
		{"test_int as my_int", "my_int"},
		{"hour(test_timestamp) as ts_hour", "ts_hour"},
		{"bucket(16, test_int) AS bucketed_int", "bucketed_int"},
		{"(test_int as foo, test_string as bar)", "foo"}, // first field
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("input=%q", tc.input), func(t *testing.T) {
			spec, err := ParsePartitionSpec(tc.input, schema)
			require.NoError(t, err)
			require.GreaterOrEqual(t, spec.NumFields(), 1)

			field := spec.Field(0)
			assert.Equal(t, tc.expectName, field.Name)
		})
	}
}

// TestParsePartitionSpecQuotedIdentifiers tests parsing quoted identifiers with special chars.
// Corresponds to quoted identifier tests in partition_spec_parser_test.cc.
func TestParsePartitionSpecQuotedIdentifiers(t *testing.T) {
	// Create schema with special field names
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "normal", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "has space", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 3, Name: "has`backtick", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 4, Name: "special@chars!", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)

	testCases := []struct {
		input      string
		expectName string
		sourceID   int
	}{
		{"`has space`", "has space", 2},
		{"`has``backtick`", "has`backtick", 3}, // doubled backtick = escaped backtick
		{"`special@chars!`", "special@chars!", 4},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("input=%q", tc.input), func(t *testing.T) {
			spec, err := ParsePartitionSpec(tc.input, schema)
			require.NoError(t, err)
			require.Equal(t, 1, spec.NumFields())

			field := spec.Field(0)
			assert.Equal(t, tc.expectName, field.Name)
			assert.Equal(t, tc.sourceID, field.SourceID)
		})
	}
}

// TestParsePartitionSpecNestedFields tests parsing nested field references.
func TestParsePartitionSpecNestedFields(t *testing.T) {
	// Create schema with nested struct
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{
			ID:       1,
			Name:     "outer",
			Required: true,
			Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 2, Name: "inner", Type: iceberg.PrimitiveTypes.Int32, Required: true},
					{
						ID:       3,
						Name:     "nested",
						Required: true,
						Type: &iceberg.StructType{
							FieldList: []iceberg.NestedField{
								{ID: 4, Name: "deep", Type: iceberg.PrimitiveTypes.String, Required: true},
							},
						},
					},
				},
			},
		},
	)

	testCases := []struct {
		input      string
		expectName string
		sourceID   int
	}{
		{"outer.inner", "outer_inner", 2},
		{"outer.nested.deep", "outer_nested_deep", 4},
		{"hour(outer.nested.deep) as deep_hour", "deep_hour", 4},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("input=%q", tc.input), func(t *testing.T) {
			spec, err := ParsePartitionSpec(tc.input, schema)
			require.NoError(t, err)
			require.Equal(t, 1, spec.NumFields())

			field := spec.Field(0)
			assert.Equal(t, tc.expectName, field.Name)
			assert.Equal(t, tc.sourceID, field.SourceID)
		})
	}
}

// TestParsePartitionSpecComplexSpec tests parsing complex partition specs.
func TestParsePartitionSpecComplexSpec(t *testing.T) {
	schema := makeTestSchema()

	input := "(hour(test_timestamp) as ts_hour, bucket(16, test_int) as int_bucket, test_string)"
	spec, err := ParsePartitionSpec(input, schema)
	require.NoError(t, err)
	require.Equal(t, 3, spec.NumFields())

	// First field: hour transform with alias
	f0 := spec.Field(0)
	assert.Equal(t, "ts_hour", f0.Name)
	assert.Equal(t, 9, f0.SourceID) // test_timestamp
	assert.IsType(t, iceberg.HourTransform{}, f0.Transform)

	// Second field: bucket transform with alias
	f1 := spec.Field(1)
	assert.Equal(t, "int_bucket", f1.Name)
	assert.Equal(t, 2, f1.SourceID) // test_int
	bucket, ok := f1.Transform.(iceberg.BucketTransform)
	require.True(t, ok)
	assert.Equal(t, 16, bucket.NumBuckets)

	// Third field: identity transform
	f2 := spec.Field(2)
	assert.Equal(t, "test_string", f2.Name)
	assert.Equal(t, 11, f2.SourceID) // test_string
	assert.IsType(t, iceberg.IdentityTransform{}, f2.Transform)
}

// TestParsePartitionSpecErrors tests parsing errors.
// Corresponds to failure tests in partition_spec_parser_test.cc.
func TestParsePartitionSpecErrors(t *testing.T) {
	schema := makeTestSchema()

	testCases := []struct {
		input       string
		errContains string
	}{
		{"(,test_int)", "expected identifier"},
		{"((test_int))", "expected identifier"},
		{"test_int)", "unexpected characters"},
		{"(test_int", "expected ')'"},
		{"unknown_field", "field not found"},
		{"bucket(test_int)", "expected number"},   // missing bucket count
		{"bucket(16)", "expected ','"},            // missing column after number
		{"truncate(test_int)", "expected number"}, // missing width
		{"unknown_transform(test_int)", "unknown transform"},
		{"`unclosed", "unterminated quoted"},
		{"test_int.nonexistent", "non-struct"}, // can't navigate into primitive
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("input=%q", tc.input), func(t *testing.T) {
			_, err := ParsePartitionSpec(tc.input, schema)
			require.Error(t, err)
			assert.Contains(t, strings.ToLower(err.Error()), strings.ToLower(tc.errContains),
				"error should contain %q, got: %v", tc.errContains, err)
		})
	}
}

// TestParsePartitionSpecCaseInsensitiveTransforms tests that transform names are case-insensitive.
func TestParsePartitionSpecCaseInsensitiveTransforms(t *testing.T) {
	schema := makeTestSchema()

	testCases := []struct {
		input         string
		transformType iceberg.Transform
	}{
		{"HOUR(test_timestamp)", iceberg.HourTransform{}},
		{"Hour(test_timestamp)", iceberg.HourTransform{}},
		{"hoUr(test_timestamp)", iceberg.HourTransform{}},
		{"BUCKET(16, test_int)", iceberg.BucketTransform{NumBuckets: 16}},
		{"Truncate(10, test_int)", iceberg.TruncateTransform{Width: 10}},
		{"IDENTITY(test_int)", iceberg.IdentityTransform{}},
		{"VOID(test_int)", iceberg.VoidTransform{}},
		{"YEAR(test_timestamp)", iceberg.YearTransform{}},
		{"MONTH(test_timestamp)", iceberg.MonthTransform{}},
		{"DAY(test_timestamp)", iceberg.DayTransform{}},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("input=%q", tc.input), func(t *testing.T) {
			spec, err := ParsePartitionSpec(tc.input, schema)
			require.NoError(t, err)
			require.Equal(t, 1, spec.NumFields())
			assert.Equal(t, tc.transformType, spec.Field(0).Transform)
		})
	}
}

// TestParsePartitionSpecWhitespaceHandling tests various whitespace scenarios.
func TestParsePartitionSpecWhitespaceHandling(t *testing.T) {
	schema := makeTestSchema()

	testCases := []string{
		"  test_int  ",
		"\ttest_int\t",
		"\ntest_int\n",
		"  (  test_int  )  ",
		"bucket(  16  ,  test_int  )",
		"hour(  test_timestamp  )  as  ts_hour",
		"  test_int  ,  test_string  ",
	}

	for _, input := range testCases {
		t.Run(fmt.Sprintf("input=%q", input), func(t *testing.T) {
			spec, err := ParsePartitionSpec(input, schema)
			require.NoError(t, err)
			require.GreaterOrEqual(t, spec.NumFields(), 1)
		})
	}
}
