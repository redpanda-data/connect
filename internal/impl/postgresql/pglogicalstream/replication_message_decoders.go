// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"fmt"
	"github.com/google/uuid"
	pgtypes "github.com/jackc/pgtype"
	"github.com/jackc/pgx/v5/pgtype"
	"log"
)

// ----------------------------------------------------------------------------
// PgOutput section

func isBeginMessage(WALData []byte) (bool, error) {
	logicalMsg, err := Parse(WALData)
	if err != nil {
		return false, err
	}

	_, ok := logicalMsg.(*BeginMessage)
	return ok, nil
}

func isCommitMessage(WALData []byte) (bool, *CommitMessage, error) {
	logicalMsg, err := Parse(WALData)
	if err != nil {
		return false, nil, err
	}

	m, ok := logicalMsg.(*CommitMessage)
	return ok, m, nil
}

// decodePgOutput decodes a logical replication message in pgoutput format.
// It uses the provided relations map to look up the relation metadata for the
// as a side effect it updates the relations map with any new relation metadata
// When the relation is changes in the database, the relation message is sent
// before the change message.
func decodePgOutput(WALData []byte, relations map[uint32]*RelationMessage, typeMap *pgtype.Map) (*StreamMessageChanges, error) {
	logicalMsg, err := Parse(WALData)
	message := &StreamMessageChanges{}

	if err != nil {
		return nil, err
	}
	switch logicalMsg := logicalMsg.(type) {
	case *RelationMessage:
		relations[logicalMsg.RelationID] = logicalMsg
		return nil, nil
	case *BeginMessage:
		return nil, nil
	case *CommitMessage:
		return nil, nil
	case *InsertMessage:
		rel, ok := relations[logicalMsg.RelationID]
		if !ok {
			return nil, fmt.Errorf("unknown relation ID %d", logicalMsg.RelationID)
		}
		message.Operation = "insert"
		message.Schema = rel.Namespace
		message.Table = rel.RelationName
		values := map[string]interface{}{}
		for idx, col := range logicalMsg.Tuple.Columns {
			colName := rel.Columns[idx].Name
			switch col.DataType {
			case 'n': // null
				values[colName] = nil
			case 'u': // unchanged toast
				// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
			case 't': //text
				val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
				if err != nil {
					return nil, err
				}
				values[colName] = val
			}
		}
		message.Data = values
	case *UpdateMessage:
		rel, ok := relations[logicalMsg.RelationID]
		if !ok {
			return nil, fmt.Errorf("unknown relation ID %d", logicalMsg.RelationID)
		}
		message.Operation = "update"
		message.Schema = rel.Namespace
		message.Table = rel.RelationName
		values := map[string]interface{}{}
		for idx, col := range logicalMsg.NewTuple.Columns {
			colName := rel.Columns[idx].Name
			switch col.DataType {
			case 'n': // null
				values[colName] = nil
			case 'u': // unchanged toast
				// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
			case 't': //text
				val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
				if err != nil {
					log.Fatalln("error decoding column data:", err)
				}
				values[colName] = val
			}
		}
		message.Data = values
	case *DeleteMessage:
		rel, ok := relations[logicalMsg.RelationID]
		if !ok {
			return nil, fmt.Errorf("unknown relation ID %d", logicalMsg.RelationID)
		}
		message.Operation = "delete"
		message.Schema = rel.Namespace
		message.Table = rel.RelationName
		values := map[string]interface{}{}
		for idx, col := range logicalMsg.OldTuple.Columns {
			colName := rel.Columns[idx].Name
			switch col.DataType {
			case 'n': // null
				values[colName] = nil
			case 'u': // unchanged toast
				// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
			case 't': //text
				val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
				if err != nil {
					log.Fatalln("error decoding column data:", err)
				}
				values[colName] = val
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

func decodeTextColumnData(mi *pgtype.Map, data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := mi.TypeForOID(dataType); ok {
		val, err := dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
		if err != nil {
			return val, err
		}

		if dt.Name == "uuid" {
			typesValueForUUID := val.([16]uint8)
			return uuid.UUID(typesValueForUUID).String(), err
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
