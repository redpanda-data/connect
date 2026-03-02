// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package mssqlserver

import (
	"database/sql"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/schema"
)

// mssqlTypeNameToCommonType maps an MSSQL DatabaseTypeName() string to a
// schema.CommonType. The comparison is case-insensitive.
func mssqlTypeNameToCommonType(typeName string) schema.CommonType {
	switch strings.ToUpper(typeName) {
	case "TINYINT", "SMALLINT", "INT", "BIGINT":
		return schema.Int64
	case "FLOAT":
		return schema.Float64
	case "REAL":
		return schema.Float32
	case "DECIMAL", "NUMERIC", "MONEY", "SMALLMONEY":
		// Arbitrary precision — preserve as string to avoid data loss.
		return schema.String
	case "BIT":
		return schema.Boolean
	case "DATETIME", "DATETIME2", "SMALLDATETIME", "DATETIMEOFFSET":
		return schema.Timestamp
	case "DATE", "TIME":
		// Date-only and time-only types are represented as strings for
		// compatibility with downstream processors (consistent with PostgreSQL).
		return schema.String
	case "BINARY", "VARBINARY", "VARBINARY(MAX)", "IMAGE",
		"TIMESTAMP", "ROWVERSION":
		// Note: MSSQL TIMESTAMP/ROWVERSION is a binary counter (varbinary(8)),
		// not a datetime type.
		return schema.ByteArray
	default:
		// CHAR, VARCHAR, VARCHAR(MAX), NCHAR, NVARCHAR, NVARCHAR(MAX), XML,
		// UNIQUEIDENTIFIER, JSON (stored as NVARCHAR), and any unknown type.
		return schema.String
	}
}

// columnTypesToSchema converts sql.ColumnType metadata from a snapshot or CDC
// query into a serialised schema.Common suitable for use as message metadata.
func columnTypesToSchema(tableName string, colNames []string, colTypes []*sql.ColumnType) any {
	children := make([]schema.Common, len(colTypes))
	for i, ct := range colTypes {
		children[i] = schema.Common{
			Name:     colNames[i],
			Type:     mssqlTypeNameToCommonType(ct.DatabaseTypeName()),
			Optional: true,
		}
	}
	c := schema.Common{
		Name:     tableName,
		Type:     schema.Object,
		Optional: false,
		Children: children,
	}
	return c.ToAny()
}
