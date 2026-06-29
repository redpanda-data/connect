package replication

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// SnapshotConfig controls initial snapshot behaviour.
type SnapshotConfig struct {
	MaxBatchSize int      // rows per query (default 1024)
	PKColumns    []string // used for ORDER BY and idempotency keys
}

// Snapshot reads all existing rows from a table as OpTypeRead events.
// The change-table watermark is captured BEFORE rows are read, guaranteeing
// no gap between snapshot end and stream start.
type Snapshot struct {
	db  *sql.DB
	cfg SnapshotConfig
}

// NewSnapshot creates a Snapshot reader.
func NewSnapshot(db *sql.DB, cfg SnapshotConfig) *Snapshot {
	if cfg.MaxBatchSize <= 0 {
		cfg.MaxBatchSize = 1024
	}
	return &Snapshot{db: db, cfg: cfg}
}

// Read snapshots schema.table. Returns (events, watermark, error).
// watermark is the _RPCN_CDC.CHANGES.ID high-water mark at snapshot start;
// pass it to Stream.StartFrom to resume streaming with no gap.
//
// Rows are fetched in pages of MaxBatchSize using LIMIT/OFFSET to bound
// memory use regardless of table size.
func (s *Snapshot) Read(ctx context.Context, schema, table string, columns []string) ([]ChangeEvent, LogPos, error) {
	// Capture watermark BEFORE reading any rows.
	var maxID int64
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(ID), 0) FROM _RPCN_CDC.CHANGES`).Scan(&maxID); err != nil {
		return nil, 0, fmt.Errorf("snapshot watermark for %s.%s: %w", schema, table, err)
	}
	watermark := NewLogPos(uint64(maxID))

	// Quote each column name with ANSI double-quotes to handle names with
	// spaces, reserved words, or special characters.
	quotedCols := make([]string, len(columns))
	for i, c := range columns {
		quotedCols[i] = `"` + strings.ReplaceAll(c, `"`, `""`) + `"`
	}
	colList := strings.Join(quotedCols, ", ")
	orderBy := ""
	if len(s.cfg.PKColumns) > 0 {
		quotedPKs := make([]string, len(s.cfg.PKColumns))
		for i, pk := range s.cfg.PKColumns {
			quotedPKs[i] = `"` + strings.ReplaceAll(pk, `"`, `""`) + `"`
		}
		orderBy = " ORDER BY " + strings.Join(quotedPKs, ", ")
	}

	quotedSchema := strings.ReplaceAll(schema, `"`, `""`)
	quotedTable := strings.ReplaceAll(table, `"`, `""`)
	// LIMIT ? OFFSET ? paginates the snapshot so at most MaxBatchSize rows are
	// held in memory per page, preventing OOM on large tables.
	queryTmpl := fmt.Sprintf(`SELECT %s FROM "%s"."%s"%s LIMIT ? OFFSET ?`,
		colList, quotedSchema, quotedTable, orderBy)

	var events []ChangeEvent
	var offset int
	for {
		rows, err := s.db.QueryContext(ctx, queryTmpl, s.cfg.MaxBatchSize, offset)
		if err != nil {
			return nil, 0, fmt.Errorf("snapshot query %s.%s offset %d: %w", schema, table, offset, err)
		}

		colTypes, err := rows.ColumnTypes()
		if err != nil {
			rows.Close()
			return nil, 0, fmt.Errorf("column types for %s.%s: %w", schema, table, err)
		}

		// Preallocate once per page and reuse across rows to avoid per-row malloc pressure.
		vals := make([]any, len(colTypes))
		ptrs := make([]any, len(colTypes))
		for i := range vals {
			ptrs[i] = &vals[i]
		}

		rowCount := 0
		for rows.Next() {
			for i := range vals {
				vals[i] = nil
			}
			if err := rows.Scan(ptrs...); err != nil {
				rows.Close()
				return nil, 0, fmt.Errorf("scan snapshot row %s.%s: %w", schema, table, err)
			}
			row := make(map[string]any, len(colTypes))
			for i, ct := range colTypes {
				row[ct.Name()] = vals[i]
			}
			events = append(events, ChangeEvent{
				Schema:    schema,
				Table:     table,
				Operation: OpTypeRead,
				LogPos:    LogPos(0), // null: snapshot rows have no log position
				Timestamp: time.Now(),
				Data:      row,
				PKColumns: s.cfg.PKColumns,
			})
			rowCount++
		}
		iterErr := rows.Err()
		rows.Close()
		if iterErr != nil {
			return nil, 0, fmt.Errorf("iterate snapshot %s.%s: %w", schema, table, iterErr)
		}

		if rowCount < s.cfg.MaxBatchSize {
			// Last page — no more rows to fetch.
			break
		}
		offset += rowCount
	}
	return events, watermark, nil
}
