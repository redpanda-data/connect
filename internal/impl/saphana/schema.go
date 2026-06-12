package saphana

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"sync"

	"github.com/redpanda-data/connect/v4/internal/impl/saphana/replication"
)

// HANATypeKind maps a HANA DATA_TYPE_NAME to a canonical Go kind string.
// Used for schema construction and streaming value coercion.
func HANATypeKind(hanaType string) string {
	switch hanaType {
	case "TINYINT", "SMALLINT", "INTEGER", "BIGINT":
		return "int64"
	case "REAL", "DOUBLE":
		return "float64"
	case "DECIMAL", "SMALLDECIMAL":
		return "string" // arbitrary precision — keep as string to avoid loss
	case "BOOLEAN":
		return "bool"
	case "BLOB", "BINARY", "VARBINARY":
		return "bytes"
	default:
		// VARCHAR, NVARCHAR, CHAR, NCHAR, ALPHANUM, SHORTTEXT,
		// DATE, TIME, TIMESTAMP, SECONDDATE, LONGDATE,
		// CLOB, NCLOB, and any unknown type.
		return "string"
	}
}

// SchemaKey returns a compact cache key for schema+table.
func SchemaKey(schema, table string) string {
	return schema + "\x00" + table
}

// ColumnInfo holds per-column metadata from SYS.TABLE_COLUMNS.
type ColumnInfo struct {
	Name     string
	TypeName string
	Position int
	Nullable bool
	IsPK     bool
}

type cachedTable struct {
	columns []ColumnInfo
	pkCols  []string
}

// SchemaCache fetches and caches table column metadata from SYS.TABLE_COLUMNS.
// Thread-safe: multiple goroutines may call ForTable concurrently.
type SchemaCache struct {
	mu      sync.RWMutex
	entries map[string]*cachedTable
	db      *sql.DB
}

// NewSchemaCache creates an empty cache backed by the given DB connection.
func NewSchemaCache(db *sql.DB) *SchemaCache {
	return &SchemaCache{
		entries: make(map[string]*cachedTable),
		db:      db,
	}
}

// schemaColumnsSQL fetches column metadata for a single (schema, table) pair.
// No LIMIT is needed: the query is filtered to one exact table, so the result
// set is bounded by the maximum column count per table in HANA (1000 columns).
const schemaColumnsSQL = `
    SELECT
        c.COLUMN_NAME,
        c.DATA_TYPE_NAME,
        c.POSITION,
        c.IS_NULLABLE,
        CASE WHEN i.COLUMN_NAME IS NOT NULL THEN 'TRUE' ELSE 'FALSE' END AS IS_PK
    FROM SYS.TABLE_COLUMNS c
    LEFT JOIN (
        SELECT ic.COLUMN_NAME
        FROM SYS.INDEX_COLUMNS ic
        JOIN SYS.INDEXES i ON ic.SCHEMA_NAME = i.SCHEMA_NAME
            AND ic.TABLE_NAME = i.TABLE_NAME
            AND ic.INDEX_NAME = i.INDEX_NAME
        WHERE ic.SCHEMA_NAME = ? AND ic.TABLE_NAME = ? AND i.CONSTRAINT = 'PRIMARY KEY'
    ) i ON c.COLUMN_NAME = i.COLUMN_NAME
    WHERE c.SCHEMA_NAME = ? AND c.TABLE_NAME = ?
    ORDER BY c.POSITION`

// ForTable returns the column metadata for schema.table, fetching from HANA if not cached.
func (sc *SchemaCache) ForTable(ctx context.Context, schema, table string) ([]ColumnInfo, []string, error) {
	key := SchemaKey(schema, table)

	sc.mu.RLock()
	if entry, ok := sc.entries[key]; ok {
		sc.mu.RUnlock()
		return entry.columns, entry.pkCols, nil
	}
	sc.mu.RUnlock()

	sc.mu.Lock()
	defer sc.mu.Unlock()
	// Double-checked locking: another goroutine may have populated the entry while we waited.
	if entry, ok := sc.entries[key]; ok {
		return entry.columns, entry.pkCols, nil
	}

	rows, err := sc.db.QueryContext(ctx, schemaColumnsSQL, schema, table, schema, table)
	if err != nil {
		return nil, nil, fmt.Errorf("fetching columns for %s.%s: %w", schema, table, err)
	}
	defer rows.Close()

	// Preallocate with a reasonable initial capacity to avoid repeated growth.
	// 16 covers most tables; the slice grows automatically if needed.
	cols := make([]ColumnInfo, 0, 16)
	pks := make([]string, 0, 4)
	for rows.Next() {
		var c ColumnInfo
		var nullable, isPK string
		if err := rows.Scan(&c.Name, &c.TypeName, &c.Position, &nullable, &isPK); err != nil {
			return nil, nil, fmt.Errorf("scanning column row for %s.%s: %w", schema, table, err)
		}
		c.Nullable = nullable == "TRUE"
		c.IsPK = isPK == "TRUE"
		cols = append(cols, c)
		if c.IsPK {
			pks = append(pks, c.Name)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	sc.entries[key] = &cachedTable{columns: cols, pkCols: pks}
	return cols, pks, nil
}

// Invalidate removes a table's cached metadata (call after DDL changes).
func (sc *SchemaCache) Invalidate(schema, table string) {
	sc.mu.Lock()
	delete(sc.entries, SchemaKey(schema, table))
	sc.mu.Unlock()
}

// CoerceValue coerces a JSON-parsed value to the native Go type for the HANA column type.
func CoerceValue(hanaType string, raw any) any {
	if raw == nil {
		return nil
	}
	switch HANATypeKind(hanaType) {
	case "int64":
		switch v := raw.(type) {
		case float64:
			return int64(v)
		case string:
			i, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return raw // return original on parse failure, don't silently zero
			}
			return i
		}
	case "float64":
		if v, ok := raw.(string); ok {
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return raw
			}
			return f
		}
	case "bool":
		if v, ok := raw.(string); ok {
			return v == "true" || v == "TRUE" || v == "1"
		}
	}
	return raw
}

// ColumnInfoToReplicationMeta converts to replication.ColumnMeta slice.
func ColumnInfoToReplicationMeta(cols []ColumnInfo) []replication.ColumnMeta {
	out := make([]replication.ColumnMeta, len(cols))
	for i, c := range cols {
		out[i] = replication.ColumnMeta{
			Name:     c.Name,
			TypeName: c.TypeName,
			Position: c.Position,
			Nullable: c.Nullable,
		}
	}
	return out
}
