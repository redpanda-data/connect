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
func hanaDecimalToCommon(name string, precision, scale sql.NullInt64, optional bool) schema.Common {
	if precision.Valid && scale.Valid {
		p, s := precision.Int64, scale.Int64
		if s == 0 && p > 0 && p <= 18 {
			return schema.Common{Name: name, Type: schema.Int64, Optional: optional}
		}
		if s < 0 {
			s = 0
		}
		if c, err := schema.NewDecimal(name, int32(p), int32(s), optional); err == nil {
			return c
		}
	}
	return schema.NewBigDecimal(name, optional)
}

// ---------------------------------------------------------------------------
// Schema cache
// ---------------------------------------------------------------------------

// schemaResult bundles everything the schema cache vends per table.
type schemaResult struct {
	Val      any                    // Avro-compatible schema value for the "schema" metadata field
	PKCols   []string               // primary-key column names in key order
	ColTypes map[string]schema.Common // column name → Common, used for decimal canonicalisation
}

// cachedSchema holds the prebuilt schema result and a column-name index used
// for addition-only drift detection.
type cachedSchema struct {
	result *schemaResult
	keys   map[string]struct{}
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

const hanaColumnQuery = `SELECT COLUMN_NAME, DATA_TYPE_NAME, LENGTH, SCALE, IS_NULLABLE
FROM SYS.TABLE_COLUMNS
WHERE SCHEMA_NAME = ? AND TABLE_NAME = ?
ORDER BY POSITION`

const hanaPKQuery = `SELECT ic.COLUMN_NAME
FROM SYS.INDEX_COLUMNS ic
JOIN SYS.INDEXES i
    ON ic.SCHEMA_NAME = i.SCHEMA_NAME
    AND ic.TABLE_NAME = i.TABLE_NAME
    AND ic.INDEX_NAME = i.INDEX_NAME
WHERE i.SCHEMA_NAME = ? AND i.TABLE_NAME = ? AND i.CONSTRAINT = 'PRIMARY KEY'
ORDER BY ic.POSITION`

func fetchHANASchema(ctx context.Context, db *sql.DB, schemaName, tableName string) (*cachedSchema, error) {
	rows, err := db.QueryContext(ctx, hanaColumnQuery, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("querying SYS.TABLE_COLUMNS for %s.%s: %w", schemaName, tableName, err)
	}
	defer rows.Close()

	var (
		children []schema.Common
		keySet   = make(map[string]struct{})
		colTypes = make(map[string]schema.Common)
	)

	for rows.Next() {
		var (
			colName    string
			dataType   string
			length     sql.NullInt64
			scale      sql.NullInt64
			isNullable string
		)
		if err := rows.Scan(&colName, &dataType, &length, &scale, &isNullable); err != nil {
			return nil, fmt.Errorf("scanning column row for %s.%s: %w", schemaName, tableName, err)
		}

		optional := isNullable == "TRUE"

		var common schema.Common
		if isDecimalType(dataType) {
			common = hanaDecimalToCommon(colName, length, scale, optional)
		} else {
			common = schema.Common{
				Name:     colName,
				Type:     hanaTypeToCommonType(dataType),
				Optional: optional,
			}
		}

		children = append(children, common)
		keySet[colName] = struct{}{}
		colTypes[colName] = common
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating column rows for %s.%s: %w", schemaName, tableName, err)
	}
	if len(children) == 0 {
		return nil, fmt.Errorf("no columns found for %s.%s in SYS.TABLE_COLUMNS", schemaName, tableName)
	}

	pkCols, err := fetchHANAPKCols(ctx, db, schemaName, tableName)
	if err != nil {
		// Non-fatal: emit schema without PK info rather than failing.
		pkCols = nil
	}

	c := schema.Common{
		Name:     tableName,
		Type:     schema.Object,
		Optional: false,
		Children: children,
	}
	result := &schemaResult{
		Val:      c.ToAny(),
		PKCols:   pkCols,
		ColTypes: colTypes,
	}
	return &cachedSchema{result: result, keys: keySet}, nil
}

func fetchHANAPKCols(ctx context.Context, db *sql.DB, schemaName, tableName string) ([]string, error) {
	rows, err := db.QueryContext(ctx, hanaPKQuery, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("querying primary keys for %s.%s: %w", schemaName, tableName, err)
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("scanning PK column for %s.%s: %w", schemaName, tableName, err)
		}
		cols = append(cols, col)
	}
	return cols, rows.Err()
}

// schemaForEvent returns the schema result for the given table, refreshing the
// cache when eventKeys contains a column name not in the stored schema.
// Returns nil, nil gracefully when schemaName is empty or on catalog failure.
func (sc *schemaCache) schemaForEvent(ctx context.Context, schemaName, tableName string, eventKeys []string) (*schemaResult, error) {
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
			return cached.result, nil
		}
		sc.log.Debugf("Schema drift detected for %s — refreshing from SYS.TABLE_COLUMNS", tableKey)
	}

	fresh, err := fetchHANASchema(ctx, sc.db, schemaName, tableName)
	if err != nil {
		if existing, exists := sc.entries[tableKey]; exists {
			sc.log.Warnf("Failed to refresh schema for %s, using cached version: %v", tableKey, err)
			return existing.result, nil
		}
		sc.log.Warnf("Failed to fetch schema for %s — schema metadata will be omitted: %v", tableKey, err)
		return nil, nil
	}

	sc.entries[tableKey] = fresh
	return fresh.result, nil
}
