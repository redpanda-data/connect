// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package oracledb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/redpanda-data/benthos/v4/public/schema"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/replication"
)

// oracleTypeToCommonType maps an Oracle DATA_TYPE string to a schema.CommonType.
// For NUMBER columns, callers should use oracleNumberToCommonType which
// considers precision and scale for a more specific mapping.
func oracleTypeToCommonType(dataType string) schema.CommonType {
	switch strings.ToUpper(dataType) {
	case "BINARY_FLOAT":
		return schema.Float32
	case "BINARY_DOUBLE":
		return schema.Float64
	case "RAW", "LONG RAW", "BLOB":
		return schema.ByteArray
	case "DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE":
		return schema.Timestamp
	case "JSON":
		return schema.Any
	default:
		return schema.String
	}
}

// oracleNumberToCommonType maps a NUMBER column to the most specific CommonType
// based on precision and scale. When scale is zero and precision fits in int64
// (<=18 digits), returns Int64. Otherwise returns String to preserve arbitrary
// precision without data loss.
func oracleNumberToCommonType(precision, scale int64, hasDecimalInfo bool) schema.CommonType {
	if !hasDecimalInfo {
		return schema.String
	}
	if scale == 0 && precision > 0 && precision <= 18 {
		return schema.Int64
	}
	return schema.String
}

// isNumberType reports whether dataType is one of Oracle's numeric type names
// that should use precision/scale-aware mapping.
func isNumberType(dataType string) bool {
	switch strings.ToUpper(dataType) {
	case "NUMBER", "INTEGER", "INT", "SMALLINT", "FLOAT":
		return true
	}
	return false
}

// ---------------------------------------------------------------------------
// Schema cache
// ---------------------------------------------------------------------------

// schemaCache holds per-table schema entries and performs addition-only drift
// detection: if an event references a column not in the cached schema, the
// cache is refreshed from ALL_TAB_COLUMNS.
type schemaCache struct {
	mu      sync.Mutex
	schemas map[string]*cachedSchema
	log     *service.Logger
}

type cachedSchema struct {
	schema any                 // serialised schema.Common returned by ToAny()
	keys   map[string]struct{} // column names for O(1) membership checks
}

func newSchemaCache(log *service.Logger) *schemaCache {
	return &schemaCache{
		schemas: make(map[string]*cachedSchema),
		log:     log,
	}
}

// fetchTableSchema queries ALL_TAB_COLUMNS for the given table and returns a
// cachedSchema with the column metadata encoded as a schema.Common.
func fetchTableSchema(ctx context.Context, db *sql.DB, table replication.UserTable) (*cachedSchema, error) {
	const query = `SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE
FROM ALL_TAB_COLUMNS
WHERE OWNER = :1 AND TABLE_NAME = :2
ORDER BY COLUMN_ID`

	rows, err := db.QueryContext(ctx, query, table.Schema, table.Name)
	if err != nil {
		return nil, fmt.Errorf("querying ALL_TAB_COLUMNS for %s.%s: %w", table.Schema, table.Name, err)
	}
	defer rows.Close()

	var (
		children []schema.Common
		keySet   = make(map[string]struct{})
	)
	for rows.Next() {
		var (
			colName   string
			dataType  string
			precision sql.NullInt64
			scale     sql.NullInt64
		)
		if err := rows.Scan(&colName, &dataType, &precision, &scale); err != nil {
			return nil, fmt.Errorf("scanning column metadata: %w", err)
		}

		var ct schema.CommonType
		if isNumberType(dataType) {
			ct = oracleNumberToCommonType(precision.Int64, scale.Int64, precision.Valid && scale.Valid)
		} else {
			ct = oracleTypeToCommonType(dataType)
		}

		children = append(children, schema.Common{
			Name:     colName,
			Type:     ct,
			Optional: true,
		})
		keySet[colName] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating column metadata: %w", err)
	}
	if len(children) == 0 {
		return nil, fmt.Errorf("no columns found for %s.%s in ALL_TAB_COLUMNS", table.Schema, table.Name)
	}

	c := schema.Common{
		Name:     table.Name,
		Type:     schema.Object,
		Optional: false,
		Children: children,
	}
	return &cachedSchema{schema: c.ToAny(), keys: keySet}, nil
}

// schemaForEvent returns the schema for the given table, refreshing the cache
// when eventKeys contains a column name not present in the stored schema.
// If a refresh fails but a prior schema exists, the old schema is returned
// alongside the error so callers can degrade gracefully.
//
// The mutex is held for the full duration including any DB query on drift.
// This is intentional: it avoids TOCTOU races and is acceptable because
// drift is rare (only on column additions). The tradeoff is that a slow
// catalog query during drift will stall all concurrent Publish() calls.
func (sc *schemaCache) schemaForEvent(ctx context.Context, db *sql.DB, table replication.UserTable, eventKeys []string) (any, error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	tableKey := table.Schema + "." + table.Name

	if cached, exists := sc.schemas[tableKey]; exists {
		allKnown := true
		for _, k := range eventKeys {
			if _, ok := cached.keys[k]; !ok {
				allKnown = false
				break
			}
		}
		if allKnown {
			return cached.schema, nil
		}
		sc.log.Debugf("Schema drift detected for %s: refreshing after unknown column in event", tableKey)
	}

	fresh, err := fetchTableSchema(ctx, db, table)
	if err != nil {
		if existing, exists := sc.schemas[tableKey]; exists {
			sc.log.Warnf("Failed to refresh schema for %s, using cached version: %v", tableKey, err)
			return existing.schema, err
		}
		return nil, err
	}

	sc.schemas[tableKey] = fresh
	return fresh.schema, nil
}

// seedFromColumnMeta populates the cache from column metadata collected during
// a snapshot transaction. The snapshot's READ ONLY transaction provides a
// consistent view, so this overrides any pre-fetched entry.
func (sc *schemaCache) seedFromColumnMeta(table replication.UserTable, meta []replication.ColumnMeta) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	tableKey := table.Schema + "." + table.Name

	children := make([]schema.Common, 0, len(meta))
	keySet := make(map[string]struct{}, len(meta))
	for _, m := range meta {
		var ct schema.CommonType
		if isNumberType(m.TypeName) {
			ct = oracleNumberToCommonType(m.Precision, m.Scale, m.HasDecimalSize)
		} else {
			ct = oracleTypeToCommonType(m.TypeName)
		}
		children = append(children, schema.Common{
			Name:     m.Name,
			Type:     ct,
			Optional: true,
		})
		keySet[m.Name] = struct{}{}
	}

	c := schema.Common{
		Name:     table.Name,
		Type:     schema.Object,
		Optional: false,
		Children: children,
	}
	sc.schemas[tableKey] = &cachedSchema{schema: c.ToAny(), keys: keySet}
}
