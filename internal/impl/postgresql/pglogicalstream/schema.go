// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"database/sql"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"

	bschema "github.com/redpanda-data/benthos/v4/public/schema"
)

// pgTypeNameToCommonType maps a PostgreSQL type name to a bschema.CommonType.
// The typeName argument is case-insensitive.
func pgTypeNameToCommonType(typeName string) bschema.CommonType {
	switch strings.ToLower(typeName) {
	case "bool", "boolean":
		return bschema.Boolean
	case "int2", "smallint":
		return bschema.Int32
	case "int4", "integer", "int":
		return bschema.Int32
	case "int8", "bigint":
		return bschema.Int64
	case "float4", "real":
		return bschema.Float32
	case "float8", "double precision", "double":
		return bschema.Float64
	case "numeric", "decimal":
		return bschema.String
	case "text", "varchar", "character varying", "char", "bpchar", "name":
		return bschema.String
	case "bytea":
		return bschema.ByteArray
	case "date":
		return bschema.String
	case "time", "timetz", "time without time zone", "time with time zone":
		return bschema.String
	case "timestamp", "timestamptz", "timestamp without time zone", "timestamp with time zone":
		return bschema.Timestamp
	case "json", "jsonb":
		return bschema.Any
	case "uuid":
		return bschema.String
	default:
		return bschema.Any
	}
}

// pgOIDToTypeName maps PostgreSQL type OIDs that pgtype.NewMap() does not register
// by default to their type names, so they can be resolved by pgTypeNameToCommonType.
var pgOIDToTypeName = map[uint32]string{
	pgtype.TimetzOID: "timetz", // OID 1266 — intentionally omitted from pgtype's default map
}

// relationMessageToSchema converts a RelationMessage to a serialized schema.Common,
// suitable for use as message metadata. Unknown OIDs fall back to string.
func relationMessageToSchema(rel *RelationMessage, typeMap *pgtype.Map) any {
	children := make([]bschema.Common, len(rel.Columns))
	for i, col := range rel.Columns {
		typeName := ""
		if dt, ok := typeMap.TypeForOID(col.DataType); ok {
			typeName = dt.Name
		} else if name, ok := pgOIDToTypeName[col.DataType]; ok {
			typeName = name
		}
		children[i] = bschema.Common{
			Name:     col.Name,
			Type:     pgTypeNameToCommonType(typeName),
			Optional: true,
		}
	}
	c := bschema.Common{
		Name:     rel.RelationName,
		Type:     bschema.Object,
		Optional: false,
		Children: children,
	}
	return c.ToAny()
}

// columnTypesToSchema converts sql.ColumnType slice (from a snapshot query) to a
// serialized schema.Common suitable for use as message metadata.
func columnTypesToSchema(tableName string, columnNames []string, columnTypes []*sql.ColumnType) any {
	children := make([]bschema.Common, len(columnTypes))
	for i, ct := range columnTypes {
		children[i] = bschema.Common{
			Name:     columnNames[i],
			Type:     pgTypeNameToCommonType(ct.DatabaseTypeName()),
			Optional: true,
		}
	}
	c := bschema.Common{
		Name:     tableName,
		Type:     bschema.Object,
		Optional: false,
		Children: children,
	}
	return c.ToAny()
}
