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
	"net/url"
	"path"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
)

const (
	// maxKeyValueLength is the maximum length of a single partition key value.
	// AWS S3 path size limit is 1024 bytes, we allow a single key to be up to 64 bytes.
	maxKeyValueLength = 64
	// maxPathLength is the maximum total length of the partition path.
	maxPathLength = 512
)

// PartitionKey holds the partition values as iceberg Literals.
type PartitionKey []iceberg.Optional[iceberg.Literal]

// NewPartitionKey creates a PartitionKey from parquet values based on the partition spec and schema.
// The parquet values must correspond to the partition fields and have already been transformed.
func NewPartitionKey(spec iceberg.PartitionSpec, schema *iceberg.Schema, values []parquet.Value) (PartitionKey, error) {
	if spec.NumFields() != len(values) {
		return nil, fmt.Errorf("partition key/spec mismatch: key has %d fields, but spec has %d fields",
			len(values), spec.NumFields())
	}

	if spec.NumFields() == 0 {
		return PartitionKey{}, nil
	}

	// Get the partition type which contains the result types for each field
	partitionType := spec.PartitionType(schema)

	key := make(PartitionKey, spec.NumFields())
	for i := 0; i < spec.NumFields(); i++ {
		value := values[i]

		if value.IsNull() {
			key[i] = iceberg.Optional[iceberg.Literal]{Valid: false}
			continue
		}

		// Get the result type for this partition field
		var resultType iceberg.Type
		if i < len(partitionType.FieldList) {
			resultType = partitionType.FieldList[i].Type
		}

		lit, err := parquetValueToLiteral(resultType, value)
		if err != nil {
			field := spec.Field(i)
			return nil, fmt.Errorf("failed to convert partition value for field %q: %w", field.Name, err)
		}

		key[i] = iceberg.Optional[iceberg.Literal]{Val: lit, Valid: true}
	}

	return key, nil
}

// parquetValueToLiteral converts a parquet value to an iceberg Literal based on the result type.
func parquetValueToLiteral(resultType iceberg.Type, value parquet.Value) (iceberg.Literal, error) {
	switch resultType.(type) {
	case iceberg.BooleanType:
		return iceberg.BoolLiteral(value.Boolean()), nil

	case iceberg.Int32Type:
		return iceberg.Int32Literal(value.Int32()), nil

	case iceberg.Int64Type:
		return iceberg.Int64Literal(value.Int64()), nil

	case iceberg.Float32Type:
		return iceberg.Float32Literal(value.Float()), nil

	case iceberg.Float64Type:
		return iceberg.Float64Literal(value.Double()), nil

	case iceberg.DateType:
		return iceberg.DateLiteral(iceberg.Date(value.Int32())), nil

	case iceberg.TimeType:
		return iceberg.TimeLiteral(iceberg.Time(value.Int64())), nil

	case iceberg.TimestampType, iceberg.TimestampTzType:
		return iceberg.TimestampLiteral(iceberg.Timestamp(value.Int64())), nil

	case iceberg.StringType:
		b := value.ByteArray()
		return iceberg.StringLiteral(string(b)), nil

	case iceberg.UUIDType:
		b := value.ByteArray()
		u, err := uuid.FromBytes(b)
		if err != nil {
			return nil, fmt.Errorf("invalid UUID bytes: %w", err)
		}
		return iceberg.UUIDLiteral(u), nil

	case iceberg.BinaryType:
		return iceberg.BinaryLiteral(value.ByteArray()), nil

	case iceberg.FixedType:
		return iceberg.FixedLiteral(value.ByteArray()), nil

	case iceberg.DecimalType:
		// Decimal can be stored as int32, int64, or fixed depending on precision
		switch value.Kind() {
		case parquet.Int32:
			return iceberg.Int32Literal(value.Int32()), nil
		case parquet.Int64:
			return iceberg.Int64Literal(value.Int64()), nil
		default:
			return iceberg.FixedLiteral(value.ByteArray()), nil
		}

	default:
		return nil, fmt.Errorf("unsupported iceberg type: %v", resultType)
	}
}

// PartitionKeyToPath converts a partition key into a path in remote storage.
//
// The path is constructed by concatenating partition fields in the form: <field_name>=<field_value>
// with subsequent fields separated by '/'.
//
// Returned path elements are URL-encoded. If the total path exceeds maxPathLength, it is truncated.
//
// See: https://github.com/redpanda-data/redpanda/blob/dev/src/v/datalake/partition_key_path.h
func PartitionKeyToPath(spec iceberg.PartitionSpec, key PartitionKey) (string, error) {
	if spec.NumFields() != len(key) {
		return "", fmt.Errorf("partition key/spec mismatch: key has %d fields, but spec has %d fields",
			len(key), spec.NumFields())
	}

	if spec.NumFields() == 0 {
		return "", nil
	}

	segments := make([]string, 0, spec.NumFields())
	totalLength := 0

	for i := 0; i < spec.NumFields(); i++ {
		field := spec.Field(i)
		opt := key[i]

		var valueStr string
		if !opt.Valid {
			valueStr = "null"
		} else {
			valueStr = formatLiteralValue(field.Transform, opt.Val)
		}

		segment := fmt.Sprintf("%s=%s", url.PathEscape(field.Name), url.PathEscape(valueStr))

		// Check if adding this segment would exceed max path length.
		// Account for the '/' separator (except for the first segment).
		segmentLen := len(segment)
		if len(segments) > 0 {
			segmentLen++ // for the '/' separator
		}

		if totalLength+segmentLen > maxPathLength {
			// Path would exceed max length, truncate here.
			break
		}

		totalLength += segmentLen
		segments = append(segments, segment)
	}

	return path.Join(segments...), nil
}

// formatLiteralValue formats an iceberg Literal using the transform's ToHumanStr method.
// It handles truncation for string/binary values.
func formatLiteralValue(transform iceberg.Transform, lit iceberg.Literal) string {
	val := lit.Any()

	// Handle truncation for string/binary values before formatting
	switch v := val.(type) {
	case string:
		if len(v) > maxKeyValueLength {
			val = v[:maxKeyValueLength]
		}
	case []byte:
		if len(v) > maxKeyValueLength {
			val = v[:maxKeyValueLength]
		}
	}

	return transform.ToHumanStr(val)
}
