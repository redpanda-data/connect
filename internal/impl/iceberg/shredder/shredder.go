// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package shredder

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"slices"

	"github.com/apache/iceberg-go"
	"github.com/gofrs/uuid/v5"
	"github.com/parquet-go/parquet-go"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/icebergx"
)

// RequiredFieldNullError is returned when a required field has a null or missing value.
type RequiredFieldNullError struct {
	Field iceberg.NestedField
	Path  icebergx.Path
}

func (e *RequiredFieldNullError) Error() string {
	return fmt.Sprintf("missing required field %q at path %v", e.Field.Name, e.Path)
}

// ShreddedValue represents a single leaf value with its repetition and definition levels.
// This is the output of the Dremel shredding algorithm.
type ShreddedValue struct {
	// FieldID is the Iceberg field ID for this column.
	FieldID int
	// Value is the parquet value (may be null).
	Value parquet.Value
	// RepLevel is the repetition level - indicates at what repeated field level
	// this value repeats (0 = new record, higher = nested repetition).
	RepLevel int
	// DefLevel is the definition level - indicates how many optional/repeated
	// fields in the path are actually defined (non-null).
	DefLevel int
}

// Sink receives output from the shredding process.
type Sink interface {
	// EmitValue is called for each leaf value with its repetition/definition levels.
	EmitValue(sv ShreddedValue) error

	// OnNewField is called when a field exists in the input but not in the schema.
	// path is the parent path (may be empty for top-level fields), name is the unknown field name.
	//
	// The value parameter contains the raw input value with the following types:
	//   - Primitives: string, bool, float64, int64, []byte, etc.
	//   - Structs: map[string]any
	//   - Lists: []any
	//   - Maps: map[string]any (keys are always strings in JSON)
	//   - Null: nil
	OnNewField(path icebergx.Path, name string, value any)
}

// RecordShredder implements the Dremel record shredding algorithm.
// It converts nested records into flat columnar format with repetition
// and definition levels that allow perfect reconstruction.
type RecordShredder struct {
	schema *iceberg.Schema
}

// NewRecordShredder creates a new shredder for the given schema.
func NewRecordShredder(schema *iceberg.Schema) *RecordShredder {
	return &RecordShredder{
		schema: schema,
	}
}

// Shred converts a nested record into a sequence of shredded values.
// The record should be a map[string]any matching the schema structure.
// The sink receives each leaf value and notifications of unknown fields.
func (rs *RecordShredder) Shred(record map[string]any, sink Sink) error {
	return rs.shredStruct(rs.schema.Fields(), record, nil, 0, 0, 0, sink)
}

// shredStruct processes a struct value.
// maxRepLevel is the maximum repetition level at the current nesting depth.
func (rs *RecordShredder) shredStruct(
	fields []iceberg.NestedField,
	value map[string]any,
	path icebergx.Path,
	repLevel, defLevel, maxRepLevel int,
	sink Sink,
) error {
	// Build set of known field names for new field detection.
	knownFields := make(map[string]struct{}, len(fields))

	// Process schema fields.
	for _, field := range fields {
		knownFields[field.Name] = struct{}{}
		fieldValue, exists := value[field.Name]

		// Validate required fields.
		if field.Required && (!exists || fieldValue == nil) {
			return &RequiredFieldNullError{field, path}
		}

		// Compute this field's definition level contribution.
		fieldDefLevel := defLevel
		if !field.Required {
			fieldDefLevel++ // Optional field adds to max def level.
		}

		// Build path for this field.
		fieldPath := append(path, icebergx.PathSegment{Kind: icebergx.PathField, Name: field.Name})

		if !exists || fieldValue == nil {
			// Field is null or missing - emit null for all leaf descendants.
			if err := rs.shredNull(field.Type, field.ID, repLevel, defLevel, sink); err != nil {
				return err
			}
			continue
		}

		// Field is defined - process based on type.
		if err := rs.shredValue(field.Type, field.ID, fieldValue, fieldPath, repLevel, fieldDefLevel, maxRepLevel, sink); err != nil {
			return fmt.Errorf("field %q: %w", field.Name, err)
		}
	}

	// Detect unknown fields in input.
	for key, val := range value {
		if _, known := knownFields[key]; !known {
			sink.OnNewField(slices.Clone(path), key, val)
		}
	}

	return nil
}

// shredValue processes a value according to its schema type.
// maxRepLevel is the maximum repetition level at the current nesting depth.
func (rs *RecordShredder) shredValue(
	typ iceberg.Type,
	fieldID int,
	value any,
	path icebergx.Path,
	repLevel, defLevel, maxRepLevel int,
	sink Sink,
) error {
	switch t := typ.(type) {
	case *iceberg.StructType:
		mapVal, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("expected map for struct type, got %T", value)
		}
		return rs.shredStruct(t.Fields(), mapVal, path, repLevel, defLevel, maxRepLevel, sink)

	case *iceberg.ListType:
		return rs.shredList(t, value, path, repLevel, defLevel, maxRepLevel, sink)

	case *iceberg.MapType:
		return rs.shredMap(t, value, path, repLevel, defLevel, maxRepLevel, sink)

	default:
		// Leaf/primitive type.
		pqVal, err := convertLeafValue(value, typ)
		if err != nil {
			return err
		}
		return sink.EmitValue(ShreddedValue{
			FieldID:  fieldID,
			Value:    pqVal,
			RepLevel: repLevel,
			DefLevel: defLevel,
		})
	}
}

// shredList processes a list value.
// maxRepLevel is the maximum repetition level from parent context.
func (rs *RecordShredder) shredList(
	listType *iceberg.ListType,
	value any,
	path icebergx.Path,
	repLevel, defLevel, maxRepLevel int,
	sink Sink,
) error {
	slice, ok := value.([]any)
	if !ok {
		return fmt.Errorf("expected slice for list type, got %T", value)
	}

	// This list adds one to the max repetition level.
	listMaxRepLevel := maxRepLevel + 1

	// Empty list is treated like null.
	if len(slice) == 0 {
		return rs.shredNull(listType.Element, listType.ElementID, repLevel, defLevel, sink)
	}

	// Element's definition level.
	elemDefLevel := defLevel + 1
	if !listType.ElementRequired {
		elemDefLevel++
	}

	// Path for list elements.
	elemPath := append(path, icebergx.PathSegment{Kind: icebergx.PathListElement})

	for i, elem := range slice {
		elemRepLevel := repLevel
		if i > 0 {
			// Subsequent elements get this list's max repetition level.
			elemRepLevel = listMaxRepLevel
		}

		if elem == nil {
			// Null element.
			nullDefLevel := defLevel + 1 // List is defined, but element is null.
			if err := rs.shredNull(listType.Element, listType.ElementID, elemRepLevel, nullDefLevel, sink); err != nil {
				return err
			}
			continue
		}

		if err := rs.shredValue(listType.Element, listType.ElementID, elem, elemPath, elemRepLevel, elemDefLevel, listMaxRepLevel, sink); err != nil {
			return fmt.Errorf("list element %d: %w", i, err)
		}
	}

	return nil
}

// shredMap processes a map value.
// maxRepLevel is the maximum repetition level from parent context.
func (rs *RecordShredder) shredMap(
	mapType *iceberg.MapType,
	value any,
	path []icebergx.PathSegment,
	repLevel, defLevel, maxRepLevel int,
	sink Sink,
) error {
	mapVal, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("expected map for map type, got %T", value)
	}

	// Maps are repeated (like lists), so they add one to the max repetition level.
	mapMaxRepLevel := maxRepLevel + 1

	// Empty map is treated like null.
	if len(mapVal) == 0 {
		// Emit nulls for both key and value columns.
		if err := rs.shredNull(mapType.KeyType, mapType.KeyID, repLevel, defLevel, sink); err != nil {
			return err
		}
		return rs.shredNull(mapType.ValueType, mapType.ValueID, repLevel, defLevel, sink)
	}

	keyDefLevel := defLevel + 1
	valueDefLevel := defLevel + 1
	if !mapType.ValueRequired {
		valueDefLevel++
	}

	// Path for map entries.
	entryPath := append(path, icebergx.PathSegment{Kind: icebergx.PathMapEntry})

	first := true
	for k, v := range mapVal {
		elemRepLevel := repLevel
		if !first {
			// Subsequent entries get this map's max repetition level.
			elemRepLevel = mapMaxRepLevel
		}
		first = false

		// Shred the key.
		keyVal, err := convertLeafValue(k, mapType.KeyType)
		if err != nil {
			return fmt.Errorf("map key: %w", err)
		}
		if err := sink.EmitValue(ShreddedValue{
			FieldID:  mapType.KeyID,
			Value:    keyVal,
			RepLevel: elemRepLevel,
			DefLevel: keyDefLevel,
		}); err != nil {
			return err
		}

		// Shred the value.
		if v == nil {
			nullDefLevel := defLevel + 1 // Map entry is defined but value is null.
			if err := rs.shredNull(mapType.ValueType, mapType.ValueID, elemRepLevel, nullDefLevel, sink); err != nil {
				return err
			}
		} else {
			if err := rs.shredValue(mapType.ValueType, mapType.ValueID, v, entryPath, elemRepLevel, valueDefLevel, mapMaxRepLevel, sink); err != nil {
				return fmt.Errorf("map value for key %q: %w", k, err)
			}
		}
	}

	return nil
}

// shredNull emits null values for all leaf descendants of a type.
// This is called when an optional/repeated field is null/missing.
func (rs *RecordShredder) shredNull(
	typ iceberg.Type,
	fieldID int,
	repLevel, defLevel int,
	sink Sink,
) error {
	switch t := typ.(type) {
	case *iceberg.StructType:
		// Recurse into struct fields to emit nulls for all leaves.
		for _, field := range t.Fields() {
			if err := rs.shredNull(field.Type, field.ID, repLevel, defLevel, sink); err != nil {
				return err
			}
		}
		return nil

	case *iceberg.ListType:
		return rs.shredNull(t.Element, t.ElementID, repLevel, defLevel, sink)

	case *iceberg.MapType:
		if err := rs.shredNull(t.KeyType, t.KeyID, repLevel, defLevel, sink); err != nil {
			return err
		}
		return rs.shredNull(t.ValueType, t.ValueID, repLevel, defLevel, sink)

	default:
		// Leaf type - emit null value.
		return sink.EmitValue(ShreddedValue{
			FieldID:  fieldID,
			Value:    parquet.NullValue(),
			RepLevel: repLevel,
			DefLevel: defLevel,
		})
	}
}

// convertLeafValue converts a Go value to a parquet.Value based on the Iceberg type.
// This is a stub - full implementation would handle all type conversions.
func convertLeafValue(value any, typ iceberg.Type) (parquet.Value, error) {
	if value == nil {
		return parquet.NullValue(), nil
	}

	switch typ := typ.(type) {
	case iceberg.BooleanType:
		switch v := value.(type) {
		case bool:
			return parquet.BooleanValue(v), nil
		default:
			return parquet.NullValue(), fmt.Errorf("cannot convert %T to boolean", value)
		}

	case iceberg.Int32Type:
		i, err := bloblang.ValueAsInt64(value)
		return parquet.Int32Value(int32(i)), err

	case iceberg.Int64Type:
		if v, ok := value.(uint64); ok && v > math.MaxInt64 {
			return parquet.NullValue(), fmt.Errorf("uint64 value %d exceeds int64 range", v)
		}
		i, err := bloblang.ValueAsInt64(value)
		return parquet.Int64Value(i), err

	case iceberg.Float32Type:
		i, err := bloblang.ValueAsFloat32(value)
		return parquet.FloatValue(i), err

	case iceberg.Float64Type:
		i, err := bloblang.ValueAsFloat64(value)
		return parquet.DoubleValue(i), err

	case iceberg.StringType:
		v, err := bloblang.ValueAsBytes(value)
		return parquet.ByteArrayValue(v), err

	case iceberg.BinaryType:
		v, err := bloblang.ValueAsBytes(value)
		return parquet.ByteArrayValue(v), err

	case iceberg.DateType:
		// Date is days since epoch as int32.
		// TODO: Handle time.Time conversion.
		switch v := value.(type) {
		case int32:
			return parquet.Int32Value(v), nil
		case int:
			return parquet.Int32Value(int32(v)), nil
		case float64:
			return parquet.Int32Value(int32(v)), nil
		default:
			return parquet.NullValue(), fmt.Errorf("cannot convert %T to date", value)
		}

	case iceberg.TimeType:
		// Time is microseconds since midnight as int64.
		switch v := value.(type) {
		case int64:
			return parquet.Int64Value(v), nil
		case int:
			return parquet.Int64Value(int64(v)), nil
		case float64:
			return parquet.Int64Value(int64(v)), nil
		default:
			return parquet.NullValue(), fmt.Errorf("cannot convert %T to time", value)
		}

	case iceberg.TimestampType, iceberg.TimestampTzType:
		// Timestamp is microseconds since epoch as int64.
		v, err := bloblang.ValueAsTimestamp(value)
		return parquet.Int64Value(v.UnixMicro()), err

	case iceberg.UUIDType:
		switch v := value.(type) {
		case []byte:
			id, err := uuid.FromBytes(v)
			if err != nil {
				return parquet.NullValue(), fmt.Errorf("invalid UUID bytes: %w", err)
			}
			return parquet.FixedLenByteArrayValue(id.Bytes()), nil
		case string:
			id, err := uuid.FromString(v)
			if err != nil {
				return parquet.NullValue(), fmt.Errorf("invalid UUID string: %w", err)
			}
			return parquet.FixedLenByteArrayValue(id.Bytes()), nil
		default:
			return parquet.NullValue(), fmt.Errorf("cannot convert %T to UUID", value)
		}

	case iceberg.DecimalType:
		return convertToDecimal(value, typ)

	case iceberg.FixedType:
		// TODO: Validate length
		switch v := value.(type) {
		case []byte:
			return parquet.FixedLenByteArrayValue(v), nil
		default:
			return parquet.NullValue(), fmt.Errorf("cannot convert %T to fixed", value)
		}

	default:
		return parquet.NullValue(), fmt.Errorf("unsupported Iceberg type: %T", typ)
	}
}

// convertToDecimal converts a Go value to a Parquet decimal value.
// Decimal values are stored as big-endian two's complement byte arrays.
func convertToDecimal(value any, dt iceberg.DecimalType) (parquet.Value, error) {
	scale := dt.Scale()

	var (
		unscaled *big.Int
		err      error
	)

	switch v := value.(type) {
	case []byte:
		expectedLen := icebergx.DecimalByteWidth(dt.Precision())
		if len(v) != expectedLen {
			return parquet.NullValue(), fmt.Errorf("decimal []byte length %d does not match expected %d for precision %d", len(v), expectedLen, dt.Precision())
		}
		return parquet.FixedLenByteArrayValue(v), nil
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return parquet.NullValue(), fmt.Errorf("cannot convert %v to decimal", v)
		}
		unscaled = floatToUnscaled(v, scale)
	case float32:
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return parquet.NullValue(), fmt.Errorf("cannot convert %v to decimal", v)
		}
		unscaled = floatToUnscaled(float64(v), scale)
	case int64:
		unscaled = intToUnscaled(v, scale)
	case int:
		unscaled = intToUnscaled(int64(v), scale)
	case int32:
		unscaled = intToUnscaled(int64(v), scale)
	case int16:
		unscaled = intToUnscaled(int64(v), scale)
	case int8:
		unscaled = intToUnscaled(int64(v), scale)
	case uint64:
		unscaled = new(big.Int).Mul(new(big.Int).SetUint64(v), pow10(scale))
	case uint32:
		unscaled = intToUnscaled(int64(v), scale)
	case uint16:
		unscaled = intToUnscaled(int64(v), scale)
	case uint8:
		unscaled = intToUnscaled(int64(v), scale)
	case uint:
		unscaled = new(big.Int).Mul(new(big.Int).SetUint64(uint64(v)), pow10(scale))
	case json.Number:
		unscaled, err = jsonNumberToUnscaled(v, scale)
		if err != nil {
			return parquet.NullValue(), err
		}
	case string:
		unscaled, err = stringToUnscaled(v, scale)
		if err != nil {
			return parquet.NullValue(), err
		}
	default:
		return parquet.NullValue(), fmt.Errorf("cannot convert %T to decimal", value)
	}

	// Validate that the unscaled value fits within the declared precision.
	// An out-of-range value would produce a valid Parquet file but violate the
	// Iceberg schema contract, causing downstream query failures or wrong results.
	maxUnscaled := pow10(dt.Precision())
	if unscaled.CmpAbs(maxUnscaled) >= 0 {
		return parquet.NullValue(), fmt.Errorf("value %v exceeds decimal(%d, %d) precision", value, dt.Precision(), dt.Scale())
	}

	b := encodeDecimalBytes(unscaled, dt.Precision())
	return parquet.FixedLenByteArrayValue(b), nil
}

// floatToUnscaled converts a float64 to an unscaled big.Int by multiplying by 10^scale.
// Precision is capped at 128 bits which exceeds float64's 53-bit mantissa (~15.9 decimal
// digits), so no additional precision loss occurs beyond what the float64 input already has.
func floatToUnscaled(f float64, scale int) *big.Int {
	bf := new(big.Float).SetPrec(128).SetFloat64(f)
	scaleFactor := new(big.Float).SetPrec(128).SetInt(pow10(scale))
	bf.Mul(bf, scaleFactor)
	if f < 0 {
		bf.Sub(bf, new(big.Float).SetFloat64(0.5))
	} else {
		bf.Add(bf, new(big.Float).SetFloat64(0.5))
	}
	result, _ := bf.Int(nil)
	return result
}

func intToUnscaled(i int64, scale int) *big.Int {
	return new(big.Int).Mul(big.NewInt(i), pow10(scale))
}

func jsonNumberToUnscaled(n json.Number, scale int) (*big.Int, error) {
	if i, err := n.Int64(); err == nil {
		return intToUnscaled(i, scale), nil
	}
	// Fall back to string parsing instead of Float64() to avoid the 53-bit
	// mantissa precision loss on large integers or high-precision decimals.
	return stringToUnscaled(n.String(), scale)
}

func stringToUnscaled(s string, scale int) (*big.Int, error) {
	bf, _, err := new(big.Float).SetPrec(256).Parse(s, 10)
	if err != nil {
		return nil, fmt.Errorf("cannot parse %q as decimal: %w", s, err)
	}
	scaleFactor := new(big.Float).SetPrec(256).SetInt(pow10(scale))
	bf.Mul(bf, scaleFactor)
	if bf.Sign() < 0 {
		bf.Sub(bf, new(big.Float).SetFloat64(0.5))
	} else {
		bf.Add(bf, new(big.Float).SetFloat64(0.5))
	}
	result, _ := bf.Int(nil)
	return result, nil
}

// pow10Cache holds precomputed powers of 10 for scales 0–18, covering all
// common decimal scales without repeated big.Int exponentiation.
var pow10Cache [19]*big.Int

func init() {
	pow10Cache[0] = big.NewInt(1)
	for i := 1; i < len(pow10Cache); i++ {
		pow10Cache[i] = new(big.Int).Mul(pow10Cache[i-1], big.NewInt(10))
	}
}

func pow10(n int) *big.Int {
	if n >= 0 && n < len(pow10Cache) {
		return new(big.Int).Set(pow10Cache[n])
	}
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil)
}

func encodeDecimalBytes(unscaled *big.Int, precision int) []byte {
	numBytes := icebergx.DecimalByteWidth(precision)

	if unscaled.Sign() >= 0 {
		b := unscaled.Bytes()
		result := make([]byte, numBytes)
		copy(result[numBytes-len(b):], b)
		return result
	}

	modulus := new(big.Int).Lsh(big.NewInt(1), uint(numBytes*8))
	tc := new(big.Int).Add(modulus, unscaled)
	b := tc.Bytes()
	result := make([]byte, numBytes)
	for i := range result {
		result[i] = 0xFF
	}
	copy(result[numBytes-len(b):], b)
	return result
}
