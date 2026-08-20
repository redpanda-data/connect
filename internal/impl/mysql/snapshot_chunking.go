// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// chunkBounds defines a half-open PK range [lowerIncl, upperExcl) for snapshot chunking.
// A nil pointer means the bound is open-ended (i.e. no lower/upper constraint).
type chunkBounds struct {
	lowerIncl *int64
	upperExcl *int64
}

// snapshotWorkUnit pairs a table name with optional chunk bounds.
// bounds == nil means read the whole table (no chunking or fallback path).
type snapshotWorkUnit struct {
	table  string
	bounds *chunkBounds
}

// numericPKDataTypes lists the MySQL integer types that support range splitting.
var numericPKDataTypes = map[string]bool{
	"tinyint":   true,
	"smallint":  true,
	"mediumint": true,
	"int":       true,
	"integer":   true,
	"bigint":    true,
}

// planSnapshotWork builds a list of work units for all tables.
// Tables with a numeric first PK column are split into chunksPerTable chunks.
// Tables without a numeric PK fall back to a single whole-table unit.
func planSnapshotWork(ctx context.Context, tx *sql.Tx, tables []string, chunksPerTable int) ([]snapshotWorkUnit, error) {
	var units []snapshotWorkUnit
	for _, table := range tables {
		if chunksPerTable <= 1 {
			units = append(units, snapshotWorkUnit{table: table})
			continue
		}

		pkCol, isNumeric, err := firstNumericPKColumn(ctx, tx, table)
		if err != nil {
			return nil, err
		}
		if !isNumeric {
			units = append(units, snapshotWorkUnit{table: table})
			continue
		}

		lo, hi, empty, err := tableIntBounds(ctx, tx, table, pkCol)
		if err != nil {
			return nil, err
		}
		if empty {
			units = append(units, snapshotWorkUnit{table: table})
			continue
		}

		chunks := splitIntRange(lo, hi, chunksPerTable)
		for _, b := range chunks {
			b := b
			units = append(units, snapshotWorkUnit{table: table, bounds: &b})
		}
	}
	return units, nil
}

// firstNumericPKColumn returns the first PK column name and whether its data type
// is a supported integer type. Returns ("", false, nil) for non-numeric PKs.
func firstNumericPKColumn(ctx context.Context, tx *sql.Tx, table string) (string, bool, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT COLUMN_NAME, LOWER(DATA_TYPE)
		FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
		JOIN INFORMATION_SCHEMA.COLUMNS c USING (TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME)
		WHERE kcu.TABLE_NAME = ? AND kcu.CONSTRAINT_NAME = 'PRIMARY'
		  AND kcu.TABLE_SCHEMA = DATABASE()
		ORDER BY kcu.ORDINAL_POSITION
		LIMIT 1`, table)

	var colName, dataType string
	if err := row.Scan(&colName, &dataType); err != nil {
		if err == sql.ErrNoRows {
			return "", false, nil
		}
		return "", false, fmt.Errorf("querying first PK column for %s: %w", table, err)
	}
	return colName, numericPKDataTypes[dataType], nil
}

// tableIntBounds queries MIN and MAX of pkCol inside the snapshot transaction.
func tableIntBounds(ctx context.Context, tx *sql.Tx, table, pkCol string) (lo, hi int64, empty bool, err error) {
	q := fmt.Sprintf("SELECT MIN(`%s`), MAX(`%s`) FROM `%s`",
		strings.ReplaceAll(pkCol, "`", "``"),
		strings.ReplaceAll(pkCol, "`", "``"),
		strings.ReplaceAll(table, "`", "``"),
	)
	row := tx.QueryRowContext(ctx, q)
	var minVal, maxVal sql.NullInt64
	if err = row.Scan(&minVal, &maxVal); err != nil {
		return 0, 0, false, fmt.Errorf("querying bounds for %s.%s: %w", table, pkCol, err)
	}
	if !minVal.Valid {
		return 0, 0, true, nil
	}
	return minVal.Int64, maxVal.Int64, false, nil
}

// splitIntRange divides [lo, hi] into n half-open chunks using uint64 arithmetic
// to avoid overflow on near-MaxInt64 spans.
// The outermost chunks have nil lower/upper bounds respectively to prevent
// off-by-one row loss at the exact MIN/MAX boundaries.
func splitIntRange(lo, hi int64, n int) []chunkBounds {
	if n <= 1 || hi <= lo {
		return []chunkBounds{{}}
	}

	span := uint64(hi-lo) + 1
	step := span / uint64(n)
	if step < 1 {
		step = 1
	}

	var chunks []chunkBounds
	cur := lo
	for i := 0; i < n; i++ {
		b := chunkBounds{}
		if i > 0 {
			b.lowerIncl = &cur
		}
		if i < n-1 {
			next := cur + int64(step)
			if next > hi {
				next = hi
			}
			b.upperExcl = &next
			cur = next
		}
		chunks = append(chunks, b)
		if i < n-1 && cur >= hi {
			break
		}
	}
	return chunks
}

// buildChunkPredicate returns a WHERE fragment and bind args for the given bounds.
// Returns ("", nil) when bounds is nil or fully open.
func buildChunkPredicate(pkCol string, b *chunkBounds) (string, []any) {
	if b == nil {
		return "", nil
	}
	quoted := "`" + strings.ReplaceAll(pkCol, "`", "``") + "`"
	var parts []string
	var args []any
	if b.lowerIncl != nil {
		parts = append(parts, quoted+" >= ?")
		args = append(args, *b.lowerIncl)
	}
	if b.upperExcl != nil {
		parts = append(parts, quoted+" < ?")
		args = append(args, *b.upperExcl)
	}
	if len(parts) == 0 {
		return "", nil
	}
	return strings.Join(parts, " AND "), args
}
