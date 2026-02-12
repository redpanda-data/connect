/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package icebergx

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"slices"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
	"github.com/hamba/avro/v2"
	"github.com/parquet-go/parquet-go/format"
)

// ParquetStats contains statistics extracted from a parquet file footer
// for registering with the iceberg catalog.
type ParquetStats struct {
	ColumnSizes     map[int]int64  // fieldID -> compressed size
	ValueCounts     map[int]int64  // fieldID -> value count
	NullValueCounts map[int]int64  // fieldID -> null count
	LowerBounds     map[int][]byte // fieldID -> min value (iceberg binary)
	UpperBounds     map[int][]byte // fieldID -> max value (iceberg binary)
	SplitOffsets    []int64        // sorted row group offsets
}

// minMaxAggregator tracks min/max bounds across row groups.
type minMaxAggregator struct {
	iceType iceberg.Type
	minVal  iceberg.Literal
	maxVal  iceberg.Literal
}

func (a *minMaxAggregator) update(minBytes, maxBytes []byte, pqType format.Type) error {
	if len(minBytes) > 0 {
		minLit, err := parquetBytesToLiteral(minBytes, pqType, a.iceType)
		if err != nil {
			return fmt.Errorf("decoding min value: %w", err)
		}
		if a.minVal == nil {
			a.minVal = minLit
		} else if compareLiteral(minLit, a.minVal) < 0 {
			a.minVal = minLit
		}
	}

	if len(maxBytes) > 0 {
		maxLit, err := parquetBytesToLiteral(maxBytes, pqType, a.iceType)
		if err != nil {
			return fmt.Errorf("decoding max value: %w", err)
		}
		if a.maxVal == nil {
			a.maxVal = maxLit
		} else if compareLiteral(maxLit, a.maxVal) > 0 {
			a.maxVal = maxLit
		}
	}

	return nil
}

func (a *minMaxAggregator) lowerBound() []byte {
	if a.minVal == nil {
		return nil
	}
	b, _ := a.minVal.MarshalBinary()
	return b
}

func (a *minMaxAggregator) upperBound() []byte {
	if a.maxVal == nil {
		return nil
	}
	b, _ := a.maxVal.MarshalBinary()
	return b
}

// ExtractParquetStats extracts statistics from a parquet file footer.
// colIdxToFieldID maps parquet column indices to iceberg field IDs.
func ExtractParquetStats(
	footer *format.FileMetaData,
	schema *iceberg.Schema,
	colIdxToFieldID map[int]int,
) (*ParquetStats, error) {
	stats := &ParquetStats{
		ColumnSizes:     make(map[int]int64),
		ValueCounts:     make(map[int]int64),
		NullValueCounts: make(map[int]int64),
		LowerBounds:     make(map[int][]byte),
		UpperBounds:     make(map[int][]byte),
		SplitOffsets:    make([]int64, 0, len(footer.RowGroups)),
	}

	// Build field type map for literal conversion
	fieldTypes := buildFieldTypeMap(schema)

	// Track min/max aggregators per field
	boundsAgg := make(map[int]*minMaxAggregator)

	for rgIdx, rg := range footer.RowGroups {
		// Track split offset (first page offset in row group)
		if len(rg.Columns) > 0 {
			col := rg.Columns[0].MetaData
			offset := col.DataPageOffset
			if col.DictionaryPageOffset > 0 && col.DictionaryPageOffset < offset {
				offset = col.DictionaryPageOffset
			}
			stats.SplitOffsets = append(stats.SplitOffsets, offset)
		}

		// Process each column chunk
		for colIdx, chunk := range rg.Columns {
			fieldID, ok := colIdxToFieldID[colIdx]
			if !ok {
				continue
			}

			meta := chunk.MetaData

			// Accumulate column sizes
			stats.ColumnSizes[fieldID] += meta.TotalCompressedSize

			// Accumulate value counts
			stats.ValueCounts[fieldID] += meta.NumValues

			// Accumulate null counts (if statistics present)
			colStats := meta.Statistics
			if colStats.NullCount > 0 {
				stats.NullValueCounts[fieldID] += colStats.NullCount
			}

			// Track min/max bounds
			iceType, hasType := fieldTypes[fieldID]
			if !hasType {
				continue
			}

			// Use MinValue/MaxValue (preferred) or fall back to deprecated Min/Max
			minBytes := colStats.MinValue
			maxBytes := colStats.MaxValue
			if len(minBytes) == 0 {
				minBytes = colStats.Min
			}
			if len(maxBytes) == 0 {
				maxBytes = colStats.Max
			}

			if len(minBytes) > 0 || len(maxBytes) > 0 {
				agg, ok := boundsAgg[fieldID]
				if !ok {
					agg = &minMaxAggregator{iceType: iceType}
					boundsAgg[fieldID] = agg
				}
				if err := agg.update(minBytes, maxBytes, meta.Type); err != nil {
					return nil, fmt.Errorf("row group %d, column %d (field %d): %w", rgIdx, colIdx, fieldID, err)
				}
			}
		}
	}

	// Sort split offsets
	slices.Sort(stats.SplitOffsets)

	// Extract final bounds
	for fieldID, agg := range boundsAgg {
		if lb := agg.lowerBound(); lb != nil {
			stats.LowerBounds[fieldID] = lb
		}
		if ub := agg.upperBound(); ub != nil {
			stats.UpperBounds[fieldID] = ub
		}
	}

	return stats, nil
}

// ReverseFieldIDMap reverses a fieldID->colIdx map to colIdx->fieldID.
func ReverseFieldIDMap(fieldToCol map[int]int) map[int]int {
	result := make(map[int]int, len(fieldToCol))
	for fieldID, colIdx := range fieldToCol {
		result[colIdx] = fieldID
	}
	return result
}

// buildFieldTypeMap creates a mapping from field ID to iceberg type for all leaf fields.
func buildFieldTypeMap(schema *iceberg.Schema) map[int]iceberg.Type {
	result := make(map[int]iceberg.Type)
	st := schema.AsStruct()
	for leaf := range schemaLeaves(&st, -1, nil) {
		result[leaf.FieldID] = leaf.Type
	}
	return result
}

// parquetBytesToLiteral converts parquet statistics bytes to an iceberg Literal.
// First decodes bytes based on parquet physical type, then converts to iceberg type.
func parquetBytesToLiteral(data []byte, pqType format.Type, iceType iceberg.Type) (iceberg.Literal, error) {
	// Decode bytes based on parquet physical type
	val, err := decodeParquetValue(data, pqType)
	if err != nil {
		return nil, err
	}
	// Convert to iceberg literal based on iceberg type
	return goValueToLiteral(val, iceType)
}

// decodeParquetValue decodes PLAIN-encoded parquet statistics bytes based on physical type.
func decodeParquetValue(data []byte, pqType format.Type) (any, error) {
	switch pqType {
	case format.Boolean:
		if len(data) < 1 {
			return nil, fmt.Errorf("boolean requires 1 byte, got %d", len(data))
		}
		return data[0] != 0, nil

	case format.Int32:
		if len(data) < 4 {
			return nil, fmt.Errorf("int32 requires 4 bytes, got %d", len(data))
		}
		return int32(binary.LittleEndian.Uint32(data)), nil

	case format.Int64:
		if len(data) < 8 {
			return nil, fmt.Errorf("int64 requires 8 bytes, got %d", len(data))
		}
		return int64(binary.LittleEndian.Uint64(data)), nil

	case format.Float:
		if len(data) < 4 {
			return nil, fmt.Errorf("float requires 4 bytes, got %d", len(data))
		}
		return math.Float32frombits(binary.LittleEndian.Uint32(data)), nil

	case format.Double:
		if len(data) < 8 {
			return nil, fmt.Errorf("double requires 8 bytes, got %d", len(data))
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(data)), nil

	case format.ByteArray, format.FixedLenByteArray:
		return bytes.Clone(data), nil

	default:
		return nil, fmt.Errorf("unsupported parquet type: %v", pqType)
	}
}

// goValueToLiteral converts a decoded Go value to an iceberg Literal based on iceberg type.
func goValueToLiteral(val any, iceType iceberg.Type) (iceberg.Literal, error) {
	switch t := iceType.(type) {
	case iceberg.BooleanType:
		return iceberg.NewLiteral(val.(bool)), nil
	case iceberg.Int32Type:
		return iceberg.NewLiteral(val.(int32)), nil
	case iceberg.Int64Type:
		return iceberg.NewLiteral(val.(int64)), nil
	case iceberg.Float32Type:
		return iceberg.NewLiteral(val.(float32)), nil
	case iceberg.Float64Type:
		return iceberg.NewLiteral(val.(float64)), nil
	case iceberg.DateType:
		return iceberg.NewLiteral(iceberg.Date(val.(int32))), nil
	case iceberg.TimeType:
		return iceberg.NewLiteral(iceberg.Time(val.(int64))), nil
	case iceberg.TimestampType, iceberg.TimestampTzType:
		return iceberg.NewLiteral(iceberg.Timestamp(val.(int64))), nil
	case iceberg.StringType:
		return iceberg.NewLiteral(string(val.([]byte))), nil
	case iceberg.BinaryType:
		return iceberg.NewLiteral(val.([]byte)), nil
	case iceberg.UUIDType:
		b := val.([]byte)
		if len(b) < 16 {
			return nil, fmt.Errorf("UUID requires 16 bytes, got %d", len(b))
		}
		var u uuid.UUID
		copy(u[:], b)
		return iceberg.NewLiteral(u), nil
	case *iceberg.FixedType:
		b := val.([]byte)
		if len(b) < t.Len() {
			return nil, fmt.Errorf("fixed type requires %d bytes, got %d", t.Len(), len(b))
		}
		return iceberg.NewLiteral(b[:t.Len()]), nil
	default:
		return nil, fmt.Errorf("unsupported iceberg type: %v", iceType)
	}
}

// PartitionFieldMaps returns avro logical types and fixed sizes for partition fields.
// These are needed for the DataFileBuilder to properly serialize partition data.
func PartitionFieldMaps(spec iceberg.PartitionSpec, schema *iceberg.Schema) (map[int]avro.LogicalType, map[int]int) {
	logicalTypes := make(map[int]avro.LogicalType)
	fixedSizes := make(map[int]int)

	partType := spec.PartitionType(schema)
	for _, field := range partType.FieldList {
		switch t := field.Type.(type) {
		case iceberg.DateType:
			logicalTypes[field.ID] = avro.Date
		case iceberg.TimeType:
			logicalTypes[field.ID] = avro.TimeMicros
		case iceberg.TimestampType, iceberg.TimestampTzType:
			logicalTypes[field.ID] = avro.TimestampMicros
		case iceberg.UUIDType:
			logicalTypes[field.ID] = avro.UUID
		case iceberg.DecimalType:
			logicalTypes[field.ID] = avro.Decimal
			fixedSizes[field.ID] = t.Scale()
		}
	}

	return logicalTypes, fixedSizes
}

// PartitionDataFromKey extracts partition field values from a PartitionKey.
// Returns a map from partition field ID to the partition value.
func PartitionDataFromKey(spec iceberg.PartitionSpec, key PartitionKey) map[int]any {
	if key == nil {
		return nil
	}

	result := make(map[int]any)
	for i := 0; i < spec.NumFields(); i++ {
		field := spec.Field(i)
		if i < len(key) {
			opt := key[i]
			if opt.Valid {
				result[field.FieldID] = opt.Val.Any()
			} else {
				result[field.FieldID] = nil
			}
		}
	}
	return result
}
