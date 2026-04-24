// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// chunkBounds is a half-open range [lowerIncl, upperExcl) on the first column
// of a table's primary key. A nil bound means unbounded on that side. Combined
// with the keyset pagination in querySnapshotTable, a chunkBounds partitions
// one table's rows across multiple workers with neither overlap nor gap.
type chunkBounds struct {
	firstPKCol string
	lowerIncl  any
	upperExcl  any
}

// snapshotWorkUnit is one unit of work dispatched to a snapshot worker. Every
// table produces at least one unit: either a whole-table unit (bounds == nil)
// or multiple chunked units covering the table's primary-key space.
type snapshotWorkUnit struct {
	table  string
	bounds *chunkBounds
}

// numericPKDataTypes is the set of MySQL DATA_TYPE tokens for which snapshot
// chunking is supported. Tables whose first PK column is outside this set fall
// back to a whole-table read.
var numericPKDataTypes = map[string]struct{}{
	"tinyint":   {},
	"smallint":  {},
	"mediumint": {},
	"int":       {},
	"integer":   {},
	"bigint":    {},
}

// planSnapshotWork turns a table list into a work-unit list. For each table:
//
//   - chunksPerTable <= 1: emit one whole-table unit (no MIN/MAX query).
//   - First PK column is a supported integer type: query MIN/MAX under the
//     planner's snapshot transaction and split into chunksPerTable ranges.
//   - Otherwise: emit one whole-table unit and log the fallback reason.
//
// For composite PKs only the first column is used for chunking; keyset
// pagination within each chunk continues to respect the full PK ordering.
func planSnapshotWork(ctx context.Context, planner *Snapshot, tables []string, chunksPerTable int) ([]snapshotWorkUnit, error) {
	if chunksPerTable < 1 {
		chunksPerTable = 1
	}

	units := make([]snapshotWorkUnit, 0, len(tables))
	for _, table := range tables {
		if chunksPerTable == 1 {
			units = append(units, snapshotWorkUnit{table: table})
			continue
		}

		pks, err := planner.getTablePrimaryKeys(ctx, table)
		if err != nil {
			return nil, fmt.Errorf("chunk planning for %s: %w", table, err)
		}
		firstPK := pks[0]

		numeric, err := isNumericPKColumn(ctx, planner, table, firstPK)
		if err != nil {
			return nil, fmt.Errorf("inspect PK type for %s.%s: %w", table, firstPK, err)
		}
		if !numeric {
			planner.logger.Infof(
				"Snapshot chunking disabled for table %s: first PK column %s is non-numeric; reading as a single unit",
				table, firstPK)
			units = append(units, snapshotWorkUnit{table: table})
			continue
		}

		lo, hi, empty, err := tableIntBounds(ctx, planner, table, firstPK)
		if err != nil {
			return nil, fmt.Errorf("compute MIN/MAX for %s.%s: %w", table, firstPK, err)
		}
		if empty {
			units = append(units, snapshotWorkUnit{table: table})
			continue
		}

		for _, r := range splitIntRange(lo, hi, chunksPerTable) {
			units = append(units, snapshotWorkUnit{
				table: table,
				bounds: &chunkBounds{
					firstPKCol: firstPK,
					lowerIncl:  r.lo,
					upperExcl:  r.hi,
				},
			})
		}
	}
	return units, nil
}

func isNumericPKColumn(ctx context.Context, s *Snapshot, table, column string) (bool, error) {
	const q = `
SELECT DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?
`
	var dt string
	if err := s.tx.QueryRowContext(ctx, q, table, column).Scan(&dt); err != nil {
		return false, err
	}
	_, ok := numericPKDataTypes[strings.ToLower(dt)]
	return ok, nil
}

// tableIntBounds returns MIN(col), MAX(col) for an integer PK column.
// empty == true when the table has no rows (MIN and MAX return NULL).
func tableIntBounds(ctx context.Context, s *Snapshot, table, column string) (lo, hi int64, empty bool, err error) {
	q := fmt.Sprintf("SELECT MIN(`%s`), MAX(`%s`) FROM `%s`", column, column, table)
	var loN, hiN sql.NullInt64
	if err := s.tx.QueryRowContext(ctx, q).Scan(&loN, &hiN); err != nil {
		return 0, 0, false, err
	}
	if !loN.Valid || !hiN.Valid {
		return 0, 0, true, nil
	}
	return loN.Int64, hiN.Int64, false, nil
}

type intRange struct {
	lo any
	hi any
}

// splitIntRange splits [lo, hi] into n half-open chunks. The outermost chunks
// use nil bounds so rows at the exact MIN/MAX endpoints are never lost and any
// row outside [MIN, MAX] is still read. Every integer in [lo, hi] falls into
// exactly one chunk.
func splitIntRange(lo, hi int64, n int) []intRange {
	if n <= 1 || hi <= lo {
		return []intRange{{lo: nil, hi: nil}}
	}
	span := uint64(hi - lo)
	step := span / uint64(n)
	if step == 0 {
		step = 1
	}

	out := make([]intRange, 0, n)
	for i := range n {
		var loV, hiV any
		if i > 0 {
			loV = lo + int64(step*uint64(i))
		}
		if i < n-1 {
			hiV = lo + int64(step*uint64(i+1))
		}
		out = append(out, intRange{lo: loV, hi: hiV})
	}
	return out
}

// buildChunkPredicate returns a SQL fragment bounding the first PK column and
// the values to bind. Returns ("", nil) when b is nil or both bounds are nil —
// the caller should omit the WHERE clause in that case.
func buildChunkPredicate(b *chunkBounds) (string, []any) {
	if b == nil {
		return "", nil
	}
	var parts []string
	var args []any
	if b.lowerIncl != nil {
		parts = append(parts, fmt.Sprintf("`%s` >= ?", b.firstPKCol))
		args = append(args, b.lowerIncl)
	}
	if b.upperExcl != nil {
		parts = append(parts, fmt.Sprintf("`%s` < ?", b.firstPKCol))
		args = append(args, b.upperExcl)
	}
	if len(parts) == 0 {
		return "", nil
	}
	return strings.Join(parts, " AND "), args
}
