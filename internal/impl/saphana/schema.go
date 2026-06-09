// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package saphana

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/redpanda-data/benthos/v4/public/schema"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// quoteIdentifier wraps s in double-quotes and escapes internal double-quotes
// by doubling them, per the SQL standard identifier quoting rule.
func quoteIdentifier(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// hanaTypeToCommonType maps a HANA DATA_TYPE_NAME string to schema.CommonType.
// For DECIMAL columns, callers should use hanaDecimalToCommon which considers
// precision and scale for a more specific mapping.
func hanaTypeToCommonType(dataType string) schema.CommonType {
	switch dataType {
	case "TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT":
		return schema.Int64
	case "FLOAT", "DOUBLE":
		return schema.Float64
	case "REAL":
		return schema.Float32
	case "BOOLEAN":
		return schema.Boolean
	case "DATE", "TIME", "TIMESTAMP", "SECONDDATE":
		return schema.Timestamp
	case "VARBINARY", "BLOB":
		return schema.ByteArray
	default:
		// VARCHAR, NVARCHAR, CHAR, NCHAR, ALPHANUM, SHORTTEXT, CLOB, NCLOB,
		// TEXT, ST_GEOMETRY, ST_POINT, and all unrecognised types.
		return schema.String
	}
}

// isDecimalType reports whether dataType is a HANA decimal type that needs
// precision/scale-aware mapping.
func isDecimalType(dataType string) bool {
	return dataType == "DECIMAL" || dataType == "NUMERIC"
}

// hanaDecimalToCommon builds a schema.Common entry for a DECIMAL/NUMERIC column.
// Mapping rules:
//   - scale == 0 && 0 < precision <= 18: Int64
//   - known precision and scale: Decimal(precision, scale)
//   - otherwise: BigDecimal (undeclared or out-of-bounds)
func hanaDecimalToCommon(name string, precision, scale sql.NullInt64) schema.Common {
	if precision.Valid && scale.Valid {
		p, s := precision.Int64, scale.Int64
		if s == 0 && p > 0 && p <= 18 {
			return schema.Common{Name: name, Type: schema.Int64, Optional: true}
		}
		if s < 0 {
			s = 0
		}
		if c, err := schema.NewDecimal(name, int32(p), int32(s), true); err == nil {
			return c
		}
	}
	return schema.NewBigDecimal(name, true)
}

// ---------------------------------------------------------------------------
// Schema cache
// ---------------------------------------------------------------------------

// cachedSchema holds the prebuilt schema value and a column-name index used
// for addition-only drift detection.
type cachedSchema struct {
	schemaVal any
	keys      map[string]struct{}
}

// schemaCache fetches and caches per-table Avro-compatible schemas from
// SYS.TABLE_COLUMNS. When an event references a column not present in the
// cached schema, the cache is refreshed (addition-only drift detection).
type schemaCache struct {
	mu      sync.Mutex
	entries map[string]*cachedSchema
	db      *sql.DB
	log     *service.Logger
}

func newSchemaCache(db *sql.DB, log *service.Logger) *schemaCache {
	return &schemaCache{
		entries: make(map[string]*cachedSchema),
		db:      db,
		log:     log,
	}
}

const hanaColumnQuery = `SELECT COLUMN_NAME, DATA_TYPE_NAME, LENGTH, SCALE
FROM SYS.TABLE_COLUMNS
WHERE SCHEMA_NAME = ? AND TABLE_NAME = ?
ORDER BY POSITION`

func fetchHANASchema(ctx context.Context, db *sql.DB, schemaName, tableName string) (*cachedSchema, error) {
	rows, err := db.QueryContext(ctx, hanaColumnQuery, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("querying SYS.TABLE_COLUMNS for %s.%s: %w", schemaName, tableName, err)
	}
	defer rows.Close()

	var (
		children []schema.Common
		keySet   = make(map[string]struct{})
	)

	for rows.Next() {
		var (
			colName  string
			dataType string
			length   sql.NullInt64
			scale    sql.NullInt64
		)
		if err := rows.Scan(&colName, &dataType, &length, &scale); err != nil {
			return nil, fmt.Errorf("scanning column row for %s.%s: %w", schemaName, tableName, err)
		}

		var common schema.Common
		if isDecimalType(dataType) {
			common = hanaDecimalToCommon(colName, length, scale)
		} else {
			common = schema.Common{
				Name:     colName,
				Type:     hanaTypeToCommonType(dataType),
				Optional: true,
			}
		}

		children = append(children, common)
		keySet[colName] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating column rows for %s.%s: %w", schemaName, tableName, err)
	}
	if len(children) == 0 {
		return nil, fmt.Errorf("no columns found for %s.%s in SYS.TABLE_COLUMNS", schemaName, tableName)
	}

	c := schema.Common{
		Name:     tableName,
		Type:     schema.Object,
		Optional: false,
		Children: children,
	}
	return &cachedSchema{schemaVal: c.ToAny(), keys: keySet}, nil
}

// schemaForEvent returns the Avro schema for the given table, refreshing the
// cache when eventKeys contains a column name not in the stored schema.
// Returns nil, nil gracefully when schemaName is empty or on catalog failure.
func (sc *schemaCache) schemaForEvent(ctx context.Context, schemaName, tableName string, eventKeys []string) (any, error) {
	if schemaName == "" {
		return nil, nil
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	tableKey := schemaName + "." + tableName

	if cached, exists := sc.entries[tableKey]; exists {
		allKnown := true
		for _, k := range eventKeys {
			if _, ok := cached.keys[k]; !ok {
				allKnown = false
				break
			}
		}
		if allKnown {
			return cached.schemaVal, nil
		}
		sc.log.Debugf("Schema drift detected for %s — refreshing from SYS.TABLE_COLUMNS", tableKey)
	}

	fresh, err := fetchHANASchema(ctx, sc.db, schemaName, tableName)
	if err != nil {
		if existing, exists := sc.entries[tableKey]; exists {
			sc.log.Warnf("Failed to refresh schema for %s, using cached version: %v", tableKey, err)
			return existing.schemaVal, nil
		}
		sc.log.Warnf("Failed to fetch schema for %s — schema metadata will be omitted: %v", tableKey, err)
		return nil, nil
	}

	sc.entries[tableKey] = fresh
	return fresh.schemaVal, nil
}
