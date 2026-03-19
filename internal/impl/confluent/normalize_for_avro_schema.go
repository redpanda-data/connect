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

package confluent

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"time"
)

// normalizeForAvroSchema walks a parsed Avro JSON schema and coerces values
// from AsStructuredMut() into the native Go types that goavro's
// BinaryFromNative expects. It works directly with the Avro schema, preserving
// full fidelity: namespaced record names, all logical types, etc.
//
// avroSchema is the parsed JSON representation of an Avro schema node — it may
// be a string (primitive type name), a map (complex type), or a slice (union).
// rawJSON controls whether the input data uses plain values (true) or
// pre-wrapped Avro JSON union format (false).
func normalizeForAvroSchema(data any, avroSchema any, rawJSON bool) (any, error) {
	if data == nil {
		return nil, nil
	}

	switch s := avroSchema.(type) {
	case string:
		return normalizeAvroPrimitive(data, s)
	case map[string]any:
		return normalizeAvroComplex(data, s, rawJSON)
	case []any:
		return normalizeAvroUnion(data, s, rawJSON)
	default:
		return data, nil
	}
}

func normalizeAvroPrimitive(data any, typeName string) (any, error) {
	switch typeName {
	case "null":
		return nil, nil
	case "boolean":
		if v, ok := data.(bool); ok {
			return v, nil
		}
		return nil, fmt.Errorf("expected bool for Avro boolean, got %T", data)
	case "int":
		return avroToInt32(data)
	case "long":
		return avroToInt64(data)
	case "float":
		return avroToFloat32(data)
	case "double":
		return avroToFloat64(data)
	case "string":
		if v, ok := data.(string); ok {
			return v, nil
		}
		return nil, fmt.Errorf("expected string for Avro string, got %T", data)
	case "bytes":
		return avroToBytes(data)
	default:
		// Named type reference (e.g. "my.namespace.com.address") — treat
		// as opaque and pass through. goavro resolves named types itself.
		return data, nil
	}
}

func normalizeAvroComplex(data any, s map[string]any, rawJSON bool) (any, error) {
	typeVal := s["type"]
	logicalType, _ := s["logicalType"].(string)

	// Handle logical types first.
	if logicalType != "" {
		return normalizeAvroLogicalType(data, s)
	}

	typeStr, isStr := typeVal.(string)
	if !isStr {
		// Nested complex type (shouldn't normally happen at this level).
		return normalizeForAvroSchema(data, typeVal, rawJSON)
	}

	switch typeStr {
	case "record":
		return normalizeAvroRecord(data, s, rawJSON)
	case "array":
		return normalizeAvroArray(data, s, rawJSON)
	case "map":
		return normalizeAvroMap(data, s, rawJSON)
	case "enum":
		// Enums are encoded as strings.
		if v, ok := data.(string); ok {
			return v, nil
		}
		return nil, fmt.Errorf("expected string for Avro enum, got %T", data)
	default:
		return normalizeAvroPrimitive(data, typeStr)
	}
}

func normalizeAvroLogicalType(data any, s map[string]any) (any, error) {
	logicalType, _ := s["logicalType"].(string)
	switch logicalType {
	case "timestamp-millis":
		return avroToTimestamp(data, time.Millisecond)
	case "timestamp-micros":
		return avroToTimestamp(data, time.Microsecond)
	case "time-millis":
		return avroToTimeDuration(data, time.Millisecond)
	case "time-micros":
		return avroToTimeDuration(data, time.Microsecond)
	case "date":
		return avroToDate(data)
	case "decimal":
		scale := 0
		if s, ok := s["scale"].(float64); ok {
			scale = int(s)
		}
		return avroToDecimal(data, scale)
	default:
		// Unknown logical type — normalize as the base type.
		return normalizeForAvroSchema(data, s["type"], false)
	}
}

func normalizeAvroRecord(data any, s map[string]any, rawJSON bool) (any, error) {
	m, ok := data.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected map for Avro record, got %T", data)
	}

	fields, _ := s["fields"].([]any)
	out := make(map[string]any, len(fields))

	for _, f := range fields {
		fieldDef, ok := f.(map[string]any)
		if !ok {
			continue
		}
		fieldName, _ := fieldDef["name"].(string)
		fieldType := avroFieldTypeSchema(fieldDef)

		val, exists := m[fieldName]
		if !exists {
			if _, hasDefault := fieldDef["default"]; hasDefault {
				continue
			}
			if isNullableUnion(fieldType) {
				out[fieldName] = nil
				continue
			}
			return nil, fmt.Errorf("required field %q is missing", fieldName)
		}

		norm, err := normalizeForAvroSchema(val, fieldType, rawJSON)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", fieldName, err)
		}
		out[fieldName] = norm
	}
	return out, nil
}

// avroFieldTypeSchema extracts the effective type schema from an Avro field
// definition. Avro allows "flat" field definitions where complex type
// attributes (items, values, fields) sit alongside the field's name and type.
// For example: {"name": "J", "type": "map", "values": "long"}. In this case,
// the entire field definition acts as the type schema.
func avroFieldTypeSchema(fieldDef map[string]any) any {
	fieldType := fieldDef["type"]
	typeStr, isStr := fieldType.(string)
	if !isStr {
		return fieldType
	}
	switch typeStr {
	case "map", "array", "record", "enum":
		return fieldDef
	default:
		// Check for logical type on the field definition itself.
		if _, hasLogical := fieldDef["logicalType"]; hasLogical {
			return fieldDef
		}
		return fieldType
	}
}

func normalizeAvroArray(data any, s map[string]any, rawJSON bool) (any, error) {
	arr, ok := data.([]any)
	if !ok {
		return nil, fmt.Errorf("expected slice for Avro array, got %T", data)
	}
	itemsSchema := s["items"]
	out := make([]any, len(arr))
	for i, elem := range arr {
		norm, err := normalizeForAvroSchema(elem, itemsSchema, rawJSON)
		if err != nil {
			return nil, fmt.Errorf("array[%d]: %w", i, err)
		}
		out[i] = norm
	}
	return out, nil
}

func normalizeAvroMap(data any, s map[string]any, rawJSON bool) (any, error) {
	m, ok := data.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected map for Avro map, got %T", data)
	}
	valuesSchema := s["values"]
	out := make(map[string]any, len(m))
	for k, v := range m {
		norm, err := normalizeForAvroSchema(v, valuesSchema, rawJSON)
		if err != nil {
			return nil, fmt.Errorf("map[%q]: %w", k, err)
		}
		out[k] = norm
	}
	return out, nil
}

func normalizeAvroUnion(data any, branches []any, rawJSON bool) (any, error) {
	if data == nil {
		return nil, nil
	}

	// Non-rawJSON mode: input may be pre-wrapped as map[string]any{"typeName": value}.
	if !rawJSON {
		if wrapped, ok := data.(map[string]any); ok && len(wrapped) == 1 {
			for key, inner := range wrapped {
				branch := findUnionBranch(branches, key)
				if branch != nil {
					norm, err := normalizeForAvroSchema(inner, branch, rawJSON)
					if err != nil {
						return nil, err
					}
					return map[string]any{key: norm}, nil
				}
				// Unknown key — pass through for goavro to handle.
				return wrapped, nil
			}
		}
	}

	// rawJSON mode (or unwrapped value): try each non-null branch, wrap with
	// the correct type name for BinaryFromNative.
	for _, branch := range branches {
		typeName := avroSchemaTypeName(branch)
		if typeName == "null" {
			continue
		}
		norm, err := normalizeForAvroSchema(data, branch, rawJSON)
		if err == nil {
			return map[string]any{typeName: norm}, nil
		}
	}

	return nil, fmt.Errorf("no union branch matched value of type %T", data)
}

// avroSchemaTypeName returns the Avro type name for a schema node, including
// fully qualified names for records and logical type qualifiers.
func avroSchemaTypeName(schema any) string {
	switch s := schema.(type) {
	case string:
		return s
	case map[string]any:
		if lt, ok := s["logicalType"].(string); ok {
			if base, ok := s["type"].(string); ok {
				return base + "." + lt
			}
		}
		typeVal, _ := s["type"].(string)
		switch typeVal {
		case "record":
			name, _ := s["name"].(string)
			if ns, _ := s["namespace"].(string); ns != "" {
				return ns + "." + name
			}
			return name
		case "array":
			return "array"
		case "map":
			return "map"
		case "enum":
			name, _ := s["name"].(string)
			if ns, _ := s["namespace"].(string); ns != "" {
				return ns + "." + name
			}
			return name
		default:
			return typeVal
		}
	}
	return ""
}

// findUnionBranch returns the schema node in the union whose type name matches
// the given key, or nil if none matches.
func findUnionBranch(branches []any, key string) any {
	for _, branch := range branches {
		if avroSchemaTypeName(branch) == key {
			return branch
		}
	}
	return nil
}

// --- Type coercion helpers ---

func avroToInt32(data any) (int32, error) {
	n, err := toInt64(data)
	if err != nil {
		return 0, err
	}
	if n < math.MinInt32 || n > math.MaxInt32 {
		return 0, fmt.Errorf("value %d overflows int32", n)
	}
	return int32(n), nil
}

func avroToInt64(data any) (int64, error) {
	return toInt64(data)
}

func avroToFloat32(data any) (float32, error) {
	f, err := toFloat64(data)
	if err != nil {
		return 0, err
	}
	return float32(f), nil
}

func avroToFloat64(data any) (float64, error) {
	return toFloat64(data)
}

func avroToBytes(data any) ([]byte, error) {
	switch v := data.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("expected []byte or string for Avro bytes, got %T", data)
	}
}

// avroToTimestamp converts various representations to time.Time for
// timestamp-millis and timestamp-micros logical types.
func avroToTimestamp(data any, precision time.Duration) (time.Time, error) {
	switch v := data.(type) {
	case time.Time:
		return v, nil
	case string:
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			return time.Time{}, fmt.Errorf("parsing timestamp: %w", err)
		}
		return t, nil
	case float64:
		return timeFromUnits(int64(v), precision), nil
	case int64:
		return timeFromUnits(v, precision), nil
	case int:
		return timeFromUnits(int64(v), precision), nil
	case int32:
		return timeFromUnits(int64(v), precision), nil
	case json.Number:
		n, err := v.Int64()
		if err != nil {
			return time.Time{}, fmt.Errorf("parsing timestamp from json.Number: %w", err)
		}
		return timeFromUnits(n, precision), nil
	default:
		return time.Time{}, fmt.Errorf("expected time.Time, string, or numeric for timestamp, got %T", data)
	}
}

// avroToTimeDuration converts various representations to time.Duration for
// time-millis and time-micros logical types.
func avroToTimeDuration(data any, precision time.Duration) (time.Duration, error) {
	switch v := data.(type) {
	case time.Duration:
		return v, nil
	case float64:
		return time.Duration(int64(v)) * precision, nil
	case int64:
		return time.Duration(v) * precision, nil
	case int:
		return time.Duration(v) * precision, nil
	case int32:
		return time.Duration(v) * precision, nil
	case json.Number:
		n, err := v.Int64()
		if err != nil {
			return 0, fmt.Errorf("parsing time duration from json.Number: %w", err)
		}
		return time.Duration(n) * precision, nil
	default:
		return 0, fmt.Errorf("expected time.Duration or numeric for time, got %T", data)
	}
}

// avroToDate converts various representations to time.Time for the date
// logical type (days since epoch).
func avroToDate(data any) (time.Time, error) {
	switch v := data.(type) {
	case time.Time:
		return v, nil
	case string:
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			t, err = time.Parse("2006-01-02", v)
			if err != nil {
				return time.Time{}, fmt.Errorf("parsing date: %w", err)
			}
		}
		return t, nil
	case float64:
		return time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(v)), nil
	case int64:
		return time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(v)), nil
	case int:
		return time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, v), nil
	case int32:
		return time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(v)), nil
	case json.Number:
		n, err := v.Int64()
		if err != nil {
			return time.Time{}, fmt.Errorf("parsing date from json.Number: %w", err)
		}
		return time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(n)), nil
	default:
		return time.Time{}, fmt.Errorf("expected time.Time, string, or numeric for date, got %T", data)
	}
}

// avroToDecimal converts various representations to *big.Rat for the decimal
// logical type. scale is the Avro schema's scale, used to reconstruct the
// rational from raw bytes.
func avroToDecimal(data any, scale int) (*big.Rat, error) {
	switch v := data.(type) {
	case *big.Rat:
		return v, nil
	case float64:
		return new(big.Rat).SetFloat64(v), nil
	case float32:
		return new(big.Rat).SetFloat64(float64(v)), nil
	case json.Number:
		r, ok := new(big.Rat).SetString(v.String())
		if !ok {
			return nil, fmt.Errorf("cannot parse json.Number %q as decimal", v)
		}
		return r, nil
	case string:
		// Try parsing as a numeric string first (e.g. "3.14").
		if r, ok := new(big.Rat).SetString(v); ok {
			return r, nil
		}
		// Otherwise treat as raw Avro bytes encoding and reconstruct
		// the *big.Rat from the two's-complement representation.
		return decimalFromRawBytes([]byte(v), scale), nil
	case []byte:
		return decimalFromRawBytes(v, scale), nil
	default:
		return nil, fmt.Errorf("expected *big.Rat, string, or numeric for decimal, got %T", data)
	}
}

func decimalFromRawBytes(b []byte, scale int) *big.Rat {
	num := new(big.Int)
	if len(b) > 0 && b[0]&0x80 != 0 {
		// Negative two's complement.
		tmp := make([]byte, len(b))
		for i, v := range b {
			tmp[i] = ^v
		}
		num.SetBytes(tmp)
		num.Add(num, big.NewInt(1))
		num.Neg(num)
	} else {
		num.SetBytes(b)
	}
	denom := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	return new(big.Rat).SetFrac(num, denom)
}

func timeFromUnits(n int64, precision time.Duration) time.Time {
	nsPerUnit := precision.Nanoseconds()
	unitsPerSec := int64(time.Second / precision)
	seconds := n / unitsPerSec
	remainder := n - (seconds * unitsPerSec)
	nanos := remainder * nsPerUnit
	return time.Unix(seconds, nanos).UTC()
}

// isNullableUnion checks if an Avro type definition is a union containing
// "null" as one of its branches (e.g. ["null", "string"]).
func isNullableUnion(avroType any) bool {
	arr, ok := avroType.([]any)
	if !ok {
		return false
	}
	for _, branch := range arr {
		if s, ok := branch.(string); ok && s == "null" {
			return true
		}
	}
	return false
}

// toInt64 coerces various numeric types to int64.
func toInt64(data any) (int64, error) {
	switch v := data.(type) {
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case float64:
		if v != math.Trunc(v) {
			return 0, fmt.Errorf("expected integer, got float %v", v)
		}
		return int64(v), nil
	case float32:
		f := float64(v)
		if f != math.Trunc(f) {
			return 0, fmt.Errorf("expected integer, got float %v", v)
		}
		return int64(v), nil
	case json.Number:
		return v.Int64()
	default:
		return 0, fmt.Errorf("expected numeric, got %T", data)
	}
}

// toFloat64 coerces various numeric types to float64.
func toFloat64(data any) (float64, error) {
	switch v := data.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case json.Number:
		return v.Float64()
	default:
		return 0, fmt.Errorf("expected numeric, got %T", data)
	}
}
