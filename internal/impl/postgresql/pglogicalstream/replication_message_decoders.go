// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/redpanda-data/connect/v4/internal/sqlutil"
)

// ----------------------------------------------------------------------------
// PgOutput section

func isBeginMessage(WALData []byte) (bool, *BeginMessage, error) {
	logicalMsg, err := Parse(WALData)
	if err != nil {
		return false, nil, err
	}

	m, ok := logicalMsg.(*BeginMessage)
	return ok, m, nil
}

func isCommitMessage(WALData []byte) (bool, *CommitMessage, error) {
	logicalMsg, err := Parse(WALData)
	if err != nil {
		return false, nil, err
	}

	m, ok := logicalMsg.(*CommitMessage)
	return ok, m, nil
}

// toStreamMessage decodes a logical replication message in pgoutput format.
// It uses the provided relations map to look up the relation metadata for the
// as a side effect it updates the relations map with any new relation metadata
// When the relation is changes in the database, the relation message is sent
// before the change message.
func toStreamMessage(logicalMsg Message, relations map[uint32]*RelationMessage, typeMap *pgtype.Map, unchangedToastValue any) (*StreamMessage, error) {
	message := &StreamMessage{}
	switch logicalMsg := logicalMsg.(type) {
	case *RelationMessage:
		relations[logicalMsg.RelationID] = logicalMsg
		return nil, nil
	case *BeginMessage:
		message.Operation = BeginOpType
		return message, nil
	case *CommitMessage:
		message.Operation = CommitOpType
		return message, nil
	case *InsertMessage:
		rel, ok := relations[logicalMsg.RelationID]
		if !ok {
			return nil, fmt.Errorf("unknown relation ID %d", logicalMsg.RelationID)
		}
		message.Operation = InsertOpType
		message.Schema = rel.Namespace
		message.Table = rel.RelationName
		values, err := decodeTuple(logicalMsg.Tuple, rel, typeMap, unchangedToastValue, nil)
		if err != nil {
			return nil, fmt.Errorf("decoding data for insert message: %w", err)
		}
		message.Data = values
	case *UpdateMessage:
		rel, ok := relations[logicalMsg.RelationID]
		if !ok {
			return nil, fmt.Errorf("unknown relation ID %d", logicalMsg.RelationID)
		}
		message.Operation = UpdateOpType
		message.Schema = rel.Namespace
		message.Table = rel.RelationName
		// When REPLICA IDENTITY FULL is set, unchanged TOAST columns in the new tuple
		// are resolved against the old tuple which carries the actual pre-update value.
		var toastFallback *TupleData
		if logicalMsg.OldTupleType == 'O' {
			toastFallback = logicalMsg.OldTuple
		}
		values, err := decodeTuple(logicalMsg.NewTuple, rel, typeMap, unchangedToastValue, toastFallback)
		if err != nil {
			return nil, fmt.Errorf("decoding new data for update message: %w", err)
		}
		message.Data = values
		if logicalMsg.OldTuple != nil {
			before, err := decodeTuple(logicalMsg.OldTuple, rel, typeMap, unchangedToastValue, nil)
			if err != nil {
				return nil, fmt.Errorf("decoding before data for update message: %w", err)
			}
			message.BeforeData = before
		}
	case *DeleteMessage:
		rel, ok := relations[logicalMsg.RelationID]
		if !ok {
			return nil, fmt.Errorf("unknown relation ID %d", logicalMsg.RelationID)
		}
		message.Operation = DeleteOpType
		message.Schema = rel.Namespace
		message.Table = rel.RelationName
		values, err := decodeTuple(logicalMsg.OldTuple, rel, typeMap, unchangedToastValue, nil)
		if err != nil {
			return nil, fmt.Errorf("decoding data for delete message: %w", err)
		}
		message.Data = values
		message.BeforeData = values
	case *TruncateMessage:
	case *TypeMessage:
	case *OriginMessage:
	case *LogicalDecodingMessage:
		return nil, nil
	default:
		return nil, nil
	}

	return message, nil
}

func decodeTuple(tuple *TupleData, rel *RelationMessage, typeMap *pgtype.Map, unchangedToastValue any, toastFallback *TupleData) (map[string]any, error) {
	values := map[string]any{}
	for idx, col := range tuple.Columns {
		if idx >= len(rel.Columns) {
			break
		}
		colName := rel.Columns[idx].Name
		switch col.DataType {
		case 'n': // null
			values[colName] = nil
		case 'u': // unchanged toast
			values[colName] = unchangedToastValue
			if toastFallback != nil && idx < len(toastFallback.Columns) {
				col = toastFallback.Columns[idx]
				switch col.DataType {
				case 'n':
					values[colName] = nil
				case 'u':
					values[colName] = unchangedToastValue
				case 't':
					val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType, rel.Columns[idx].TypeModifier)
					if err != nil {
						return nil, fmt.Errorf("unable to decode column data: %w", err)
					}
					values[colName] = val
				default:
					return nil, fmt.Errorf("unable to decode column data, unknown data type: %d", col.DataType)
				}
			}
		case 't': // text
			val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType, rel.Columns[idx].TypeModifier)
			if err != nil {
				return nil, fmt.Errorf("unable to decode column data: %w", err)
			}
			values[colName] = val
		default:
			return nil, fmt.Errorf("unable to decode column data, unknown data type: %d", col.DataType)
		}
	}
	return values, nil
}

func decodeTextColumnData(mi *pgtype.Map, data []byte, dataType uint32, typeModifier int32) (any, error) {
	if data == nil {
		return nil, nil
	}
	if dt, ok := mi.TypeForOID(dataType); ok {
		val, err := dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
		if err != nil {
			return val, err
		}

		switch dt.Name {
		case "uuid":
			typesValueForUUID, ok := val.([16]uint8)
			if !ok {
				return nil, errors.New("unable to convert uuid to string. type casting failed")
			}
			return uuid.UUID(typesValueForUUID).String(), nil
		case "tsrange":
			return sanitizeTsrange(string(data)), nil
		case "int2":
			// pgx decodes int2 as int16; promote to int32 to match schema (Int32).
			if v, ok := val.(int16); ok {
				return int32(v), nil
			}
			return val, nil
		case "numeric":
			// NUMERIC values are emitted as canonical decimal strings to
			// match the schema.Decimal/BigDecimal value contract. When
			// atttypmod carries precision/scale, pad to the declared scale;
			// otherwise normalise the natural-scale form via BigDecimal.
			// PostgreSQL also allows the special values NaN, Infinity, and
			// -Infinity as valid NUMERIC values; pass these through as-is
			// since they have no canonical decimal representation.
			text := string(data)
			switch text {
			case "NaN", "Infinity", "-Infinity":
				return text, nil
			}
			if precision, scale, ok := pgNumericModFromAtttypmod(typeModifier); ok {
				canonical, err := sqlutil.CanonicaliseDecimal(text, precision, scale)
				if err != nil {
					return nil, fmt.Errorf("normalising numeric value: %w", err)
				}
				return canonical, nil
			}
			canonical, err := sqlutil.CanonicaliseBigDecimal(text)
			if err != nil {
				return nil, fmt.Errorf("normalising numeric value: %w", err)
			}
			return canonical, nil
		case "date":
			// ±infinity dates cannot be represented as time.Time; return nil.
			if ts, ok := val.(time.Time); ok {
				return ts, nil
			}
			return nil, nil
		case "time":
			// Return the raw PostgreSQL text representation as a string,
			// avoiding pgtype.Time struct.
			// Note: timetz (OID 1266) is not in pgx's default type map, so it
			// never reaches this switch — it is handled by the string(data)
			// fallback after the TypeForOID check.
			return string(data), nil
		case "tsvector":
			// pgx decodes tsvector as pgtype.TSVector (a struct with Lexemes/Valid),
			// but we want the raw PostgreSQL text representation as a string.
			return string(data), nil
		case "timestamp", "timestamptz":
			// ±infinity timestamps cannot be represented as time.Time; return nil.
			if ts, ok := val.(time.Time); ok {
				return ts, nil
			}
			return nil, nil
		default:
			return val, err
		}
	}
	return string(data), nil
}
