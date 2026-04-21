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
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"

	"github.com/redpanda-data/benthos/v4/public/schema"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/logminer/sqlredo"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/replication"
)

// oracleTypeToCommonType maps an Oracle DATA_TYPE string to a schema.CommonType.
// For NUMBER columns, callers should use oracleNumberToCommonType which
// considers precision and scale for a more specific mapping.
func oracleTypeToCommonType(dataType string) schema.CommonType {
	switch strings.ToUpper(dataType) {
	case "BINARY_FLOAT", "IBFLOAT", "BFLOAT":
		return schema.Float32
	case "BINARY_DOUBLE", "IBDOUBLE", "BDOUBLE":
		return schema.Float64
	case "RAW", "LONG RAW", "BLOB":
		return schema.ByteArray
	case "DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE",
		"TIMESTAMPTZ", "TIMESTAMPDTY", "TIMESTAMPTZ_DTY", "TIMESTAMPLTZ_DTY", "TIMESTAMPELTZ":
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
	if scale == 0 && precision > 0 && precision <= replication.MaxInt64DecimalPrecision {
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
//
// In CDB mode (pdbName != ""), each cache-miss refresh switches a dedicated
// *sql.Conn to the PDB context (ALTER SESSION SET CONTAINER) before running the
// catalog query, then switches back. Avoids both CDB_* view privilege issues and
// separate-connection login issues.
type schemaCache struct {
	mu      sync.Mutex
	schemas map[string]*cachedSchema
	db      *sql.DB
	pdbName string // non-empty in CDB mode; triggers ALTER SESSION SET CONTAINER per refresh
	log     *service.Logger
}

type cachedSchema struct {
	schema      any                          // serialised schema.Common returned by ToAny()
	keys        map[string]struct{}          // column names for O(1) membership checks
	colTypes    map[string]schema.CommonType // column name → CommonType for value coercion
	numericCols map[string]struct{}          // NUMBER columns that map to String (need json.Number coercion)
}

// newSchemaCache creates a schemaCache. db is used for on-demand cache-miss refreshes.
// pdbName is non-empty when connected to CDB$ROOT and monitoring a specific PDB; the cache
// will switch the session to that PDB for each catalog query.
func newSchemaCache(db *sql.DB, pdbName string, log *service.Logger) *schemaCache {
	return &schemaCache{
		schemas: make(map[string]*cachedSchema),
		db:      db,
		pdbName: pdbName,
		log:     log,
	}
}

// fetchTableSchema queries ALL_TAB_COLUMNS for the given table.
// The caller is responsible for ensuring the db/conn is in the correct container context.
func fetchTableSchema(ctx context.Context, db interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}, table replication.UserTable,
) (*cachedSchema, error) {
	query := `SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE
FROM ALL_TAB_COLUMNS
WHERE OWNER = :1 AND TABLE_NAME = :2
ORDER BY COLUMN_ID`

	rows, err := db.QueryContext(ctx, query, table.Schema, table.Name)
	if err != nil {
		return nil, fmt.Errorf("querying column metadata for %s.%s: %w", table.Schema, table.Name, err)
	}
	defer rows.Close()

	var (
		children    []schema.Common
		keySet      = make(map[string]struct{})
		colTypes    = make(map[string]schema.CommonType)
		numericCols = make(map[string]struct{})
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
		isNum := isNumberType(dataType)
		if isNum {
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
		colTypes[colName] = ct
		if isNum && ct == schema.String {
			numericCols[colName] = struct{}{}
		}
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
	return &cachedSchema{schema: c.ToAny(), keys: keySet, colTypes: colTypes, numericCols: numericCols}, nil
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
// columnTypeInfo holds the type metadata needed for streaming value coercion.
type columnTypeInfo struct {
	colTypes    map[string]schema.CommonType
	numericCols map[string]struct{}
}

func (sc *schemaCache) schemaForEvent(ctx context.Context, table replication.UserTable, eventKeys []string) (any, *columnTypeInfo, error) {
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
			return cached.schema, &columnTypeInfo{cached.colTypes, cached.numericCols}, nil
		}
		sc.log.Debugf("Schema drift detected for %s: refreshing after unknown column in event", tableKey)
	}

	var (
		fresh *cachedSchema
		err   error
	)
	if sc.pdbName != "" {
		// CDB mode: get a dedicated connection and switch to the PDB container
		// before querying ALL_TAB_COLUMNS.
		conn, connErr := sc.db.Conn(ctx)
		if connErr != nil {
			if existing, exists := sc.schemas[tableKey]; exists {
				sc.log.Warnf("Failed to get connection for schema refresh of %s, using cached version: %v", tableKey, connErr)
				return existing.schema, &columnTypeInfo{existing.colTypes, existing.numericCols}, connErr
			}
			return nil, nil, connErr
		}
		defer conn.Close()
		if _, execErr := conn.ExecContext(ctx, "ALTER SESSION SET CONTAINER = "+sc.pdbName); execErr != nil {
			if existing, exists := sc.schemas[tableKey]; exists {
				sc.log.Warnf("Failed to switch to PDB %s for schema refresh of %s, using cached version: %v", sc.pdbName, tableKey, execErr)
				return existing.schema, &columnTypeInfo{existing.colTypes, existing.numericCols}, execErr
			}
			return nil, nil, execErr
		}
		defer func() {
			if _, resetErr := conn.ExecContext(context.Background(), "ALTER SESSION SET CONTAINER = CDB$ROOT"); resetErr != nil {
				sc.log.Errorf("Failed to reset session back to CDB$ROOT after schema refresh: %v", resetErr)
			}
		}()
		fresh, err = fetchTableSchema(ctx, conn, table)
	} else {
		fresh, err = fetchTableSchema(ctx, sc.db, table)
	}
	if err != nil {
		if existing, exists := sc.schemas[tableKey]; exists {
			sc.log.Warnf("Failed to refresh schema for %s, using cached version: %v", tableKey, err)
			return existing.schema, &columnTypeInfo{existing.colTypes, existing.numericCols}, err
		}
		return nil, nil, err
	}

	sc.schemas[tableKey] = fresh
	return fresh.schema, &columnTypeInfo{fresh.colTypes, fresh.numericCols}, nil
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
	colTypes := make(map[string]schema.CommonType, len(meta))
	numericCols := make(map[string]struct{})
	for _, m := range meta {
		var ct schema.CommonType
		isNum := isNumberType(m.TypeName)
		if isNum {
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
		colTypes[m.Name] = ct
		if isNum && ct == schema.String {
			numericCols[m.Name] = struct{}{}
		}
	}

	c := schema.Common{
		Name:     table.Name,
		Type:     schema.Object,
		Optional: false,
		Children: children,
	}
	sc.schemas[tableKey] = &cachedSchema{schema: c.ToAny(), keys: keySet, colTypes: colTypes, numericCols: numericCols}
}

// ---------------------------------------------------------------------------
// Streaming value coercion
// ---------------------------------------------------------------------------

// coerceStreamingValues converts string values from LogMiner SQL_REDO parsing
// to their proper Go types based on schema column metadata. This ensures type
// consistency between snapshot (which returns native Go types via sql.Scan) and
// streaming (which returns strings because LogMiner quotes all INSERT values).
//
// Only unambiguously numeric types are coerced: Int64, Float32, Float64.
// Columns mapped to schema.String (including NUMBER with fractional scale) are
// left as-is because we cannot distinguish them from VARCHAR2 using CommonType alone.
//
// The data map is mutated in place. On parse failure, the original string value
// is preserved and a warning is logged.
func coerceStreamingValues(data map[string]any, info *columnTypeInfo, log *service.Logger) {
	if info == nil {
		return
	}
	for col, val := range data {
		ct, known := info.colTypes[col]
		if !known {
			continue
		}

		// Handle json.Number values produced by ConvertValue for bare float
		// literals (e.g. BINARY_FLOAT/BINARY_DOUBLE). These need to be
		// converted to float64 to match the snapshot path.
		if jn, ok := val.(json.Number); ok {
			switch ct {
			case schema.Float32, schema.Float64:
				if f, err := jn.Float64(); err == nil {
					data[col] = f
				}
			case schema.Int64:
				if n, err := jn.Int64(); err == nil {
					data[col] = n
				}
			}
			continue
		}

		s, ok := val.(string)
		if !ok {
			continue // already typed (nil, int64, time.Time, etc.)
		}
		switch ct {
		case schema.Int64:
			if n, err := strconv.ParseInt(s, 10, 64); err == nil {
				data[col] = n
			} else {
				log.Warnf("coerce %s: cannot parse %q as int64: %v", col, s, err)
			}
		case schema.Float32, schema.Float64:
			if f, err := strconv.ParseFloat(s, 64); err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) {
				data[col] = f
			} else if err != nil {
				log.Warnf("coerce %s: cannot parse %q as float64: %v", col, s, err)
			}
		case schema.String:
			// NUMBER columns with fractional scale map to schema.String, same as
			// VARCHAR2. Use numericCols to distinguish: only NUMBER-as-String
			// columns get wrapped as json.Number to match snapshot behavior.
			if _, isNumeric := info.numericCols[col]; isNumeric {
				data[col] = json.Number(sqlredo.NormalizeJSONNumber(s))
			}
		}
	}
}
