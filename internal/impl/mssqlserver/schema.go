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

// mssqlColumnToCommon converts a *sql.ColumnType into a schema.Common entry.
// DECIMAL and NUMERIC columns become schema.Decimal with precision/scale
// from sql.ColumnType.DecimalSize(); MONEY and SMALLMONEY remain as
// schema.String for backwards compatibility (their fixed (19,4) / (10,4)
// shapes can be obtained via explicit CAST upstream).
func mssqlColumnToCommon(name string, ct *sql.ColumnType) schema.Common {
	typeName := strings.ToUpper(ct.DatabaseTypeName())
	if typeName == "DECIMAL" || typeName == "NUMERIC" {
		precision, scale, hasSize := ct.DecimalSize()
		if hasSize {
			if c, err := schema.NewDecimal(name, int32(precision), int32(scale), true); err == nil {
				return c
			}
		}
		return schema.Common{Name: name, Type: schema.String, Optional: true}
	}
	return schema.Common{
		Name:     name,
		Type:     mssqlTypeNameToCommonType(typeName),
		Optional: true,
	}
}

// mssqlTypeNameToCommonType maps an MSSQL DatabaseTypeName() string to a
// schema.CommonType for column types whose mapping doesn't depend on
// precision/scale. Use mssqlColumnToCommon for the full conversion.
func mssqlTypeNameToCommonType(typeName string) schema.CommonType {
	switch strings.ToUpper(typeName) {
	case "TINYINT", "SMALLINT", "INT", "BIGINT":
		return schema.Int64
	case "FLOAT":
		return schema.Float64
	case "REAL":
		return schema.Float32
	case "DECIMAL", "NUMERIC":
		// DECIMAL/NUMERIC should go through mssqlColumnToCommon to pick up
		// precision/scale; falling back to String here means precision was
		// unavailable.
		return schema.String
	case "MONEY", "SMALLMONEY":
		// Fixed (19,4) and (10,4) decimals — kept as String for backwards
		// compatibility. Users who want typed decimals should CAST to
		// DECIMAL upstream.
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
		children[i] = mssqlColumnToCommon(colNames[i], ct)
	}
	c := schema.Common{
		Name:     tableName,
		Type:     schema.Object,
		Optional: false,
		Children: children,
	}
	return c.ToAny()
}
