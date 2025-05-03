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

	"github.com/google/uuid"
	pgtypes "github.com/jackc/pgtype"
	"github.com/jackc/pgx/v5/pgtype"
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
		values := map[string]any{}
		for idx, col := range logicalMsg.Tuple.Columns {
			colName := rel.Columns[idx].Name
			switch col.DataType {
			case 'n': // null
				values[colName] = nil
			case 'u': // unchanged toast
				values[colName] = unchangedToastValue
			case 't': // text
				val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
				if err != nil {
					return nil, fmt.Errorf("unable to decode column data: %w", err)
				}
				values[colName] = val
			default:
				return nil, fmt.Errorf("unable to decode column data, unknown data type: %d", col.DataType)
			}
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
		values := map[string]any{}
		for idx, col := range logicalMsg.NewTuple.Columns {
			colName := rel.Columns[idx].Name
			switch col.DataType {
			case 'n': // null
				values[colName] = nil
			case 'u': // unchanged toast
				values[colName] = unchangedToastValue
				// In the case of an update of an unchanged toast value and the replica is set to
				// IDENTITY FULL, we need to look at the old tuple in order to get the data, it's
				// just marked as unchanged in the new tuple.
				if logicalMsg.OldTupleType == 'O' && logicalMsg.OldTuple != nil && idx < len(logicalMsg.OldTuple.Columns) {
					col = logicalMsg.OldTuple.Columns[idx]
					switch col.DataType {
					case 'n': // null
						values[colName] = nil
					case 'u': // unchanged toast
						values[colName] = unchangedToastValue
					case 't':
						val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
						if err != nil {
							return nil, fmt.Errorf("unable to decode column data: %w", err)
						}
						values[colName] = val
					default:
						return nil, fmt.Errorf("unable to decode column data, unknown data type: %d", col.DataType)
					}
				}
			case 't': // text
				val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
				if err != nil {
					return nil, fmt.Errorf("unable to decode column data: %w", err)
				}
				values[colName] = val
			default:
				return nil, fmt.Errorf("unable to decode column data, unknown data type: %d", col.DataType)
			}
		}
		message.Data = values
	case *DeleteMessage:
		rel, ok := relations[logicalMsg.RelationID]
		if !ok {
			return nil, fmt.Errorf("unknown relation ID %d", logicalMsg.RelationID)
		}
		message.Operation = DeleteOpType
		message.Schema = rel.Namespace
		message.Table = rel.RelationName
		values := map[string]any{}
		for idx, col := range logicalMsg.OldTuple.Columns {
			colName := rel.Columns[idx].Name
			switch col.DataType {
			case 'n': // null
				values[colName] = nil
			case 'u': // unchanged toast
				values[colName] = unchangedToastValue
			case 't': // text
				val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
				if err != nil {
					return nil, fmt.Errorf("unable to decode column data: %w", err)
				}
				values[colName] = val
			default:
			}
		}
		message.Data = values
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

func decodeTextColumnData(mi *pgtype.Map, data []byte, dataType uint32) (any, error) {
	if data == nil {
		return nil, nil
	}
	if dt, ok := mi.TypeForOID(dataType); ok {
		val, err := dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
		if err != nil {
			return val, err
		}

		if dt.Name == "uuid" {
			typesValueForUUID, ok := val.([16]uint8)
			if !ok {
				return nil, errors.New("unable to convert uuid to string. type casting failed")
			}

			return uuid.UUID(typesValueForUUID).String(), nil
		}

		if dt.Name == "tsrange" {
			newArray := pgtypes.Tsrange{}
			if err := newArray.Scan(data); err != nil {
				return nil, err
			}

			vv, _ := newArray.Value()
			return vv, err
		}

		return val, err
	}
	return string(data), nil
}
