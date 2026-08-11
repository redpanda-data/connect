// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package snapshot

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Masterminds/squirrel"

	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream/sanitize"
)

// rowTuple renders a PrimaryKey as a Postgres ROW(...) constructor with
// placeholder args, e.g. "ROW(?, ?)" with args [v1, v2]. This lets us build
// row-wise (lexicographic) comparisons such as ROW(pk1, pk2) > ROW(?, ?),
// which correctly implements "PK tuple greater than bound tuple" semantics
// for composite primary keys.
type rowTuple struct {
	values []any
}

//nolint:stylecheck // This is implementing the squirrel.Sqlizer interface.
func (t rowTuple) ToSql() (sql string, args []any, err error) {
	placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(t.values)), ", ")
	return "ROW(" + placeholders + ")", t.values, nil
}

var _ squirrel.Sqlizer = rowTuple{}

func quotedColumns(colsUnquoted []string) []string {
	quoted := make([]string, len(colsUnquoted))
	for i, c := range colsUnquoted {
		quoted[i] = sanitize.QuotePostgresIdentifier(c)
	}
	return quoted
}

// quotedRowExpr renders the PK columns as a Postgres ROW(...) constructor,
// e.g. ROW("id", "tenant_id"), for use on the left-hand side of a row-wise
// comparison.
func quotedRowExpr(pkColsUnquoted []string) string {
	return "ROW(" + strings.Join(quotedColumns(pkColsUnquoted), ", ") + ")"
}

func quotedTableName(table TableID) string {
	return sanitize.QuotePostgresIdentifier(table.Schema) + "." + sanitize.QuotePostgresIdentifier(table.Table)
}

// buildChunkQuery builds the paginated chunk SELECT. lower may be nil (first
// chunk of a table - omit the lower-bound predicate). upper is the table's
// fixed max-PK bound (also may be nil only if the table truly has no rows,
// in which case callers should not be calling this - treat nil upper as an
// error).
//
// The SELECT clause is always "*" rather than an explicit column list, since
// this package doesn't know the full column list at this layer; callers are
// responsible for decoding whatever columns come back.
func buildChunkQuery(table TableID, pkColsUnquoted []string, lower, upper PrimaryKey, limit int) (query string, args []any, err error) {
	if len(pkColsUnquoted) == 0 {
		return "", nil, errors.New("buildChunkQuery: no primary key columns provided")
	}
	if upper == nil {
		return "", nil, fmt.Errorf("buildChunkQuery: upper bound must not be nil for table %s", table)
	}

	rowExpr := quotedRowExpr(pkColsUnquoted)

	pred := squirrel.And{}
	if lower != nil {
		pred = append(pred, squirrel.ConcatExpr(rowExpr, " > ", rowTuple{values: lower}))
	}
	pred = append(pred, squirrel.ConcatExpr(rowExpr, " <= ", rowTuple{values: upper}))

	orderBy := make([]string, len(pkColsUnquoted))
	for i, c := range quotedColumns(pkColsUnquoted) {
		orderBy[i] = c + " ASC"
	}

	query, args, err = squirrel.Select("*").
		From(quotedTableName(table)).
		Where(pred).
		OrderBy(orderBy...).
		Limit(uint64(limit)).
		PlaceholderFormat(squirrel.Dollar).
		ToSql()
	if err != nil {
		return "", nil, fmt.Errorf("building chunk query for table %s: %w", table, err)
	}
	return query, args, nil
}

// buildMaxKeyQuery builds a query to fetch the table's current maximum
// primary key (ORDER BY pk DESC LIMIT 1).
func buildMaxKeyQuery(table TableID, pkColsUnquoted []string) (query string, err error) {
	if len(pkColsUnquoted) == 0 {
		return "", errors.New("buildMaxKeyQuery: no primary key columns provided")
	}

	quotedCols := quotedColumns(pkColsUnquoted)
	orderBy := make([]string, len(quotedCols))
	for i, c := range quotedCols {
		orderBy[i] = c + " DESC"
	}

	const maxKeyLimit = 1
	query, _, err = squirrel.Select(quotedCols...).
		From(quotedTableName(table)).
		OrderBy(orderBy...).
		Limit(maxKeyLimit).
		PlaceholderFormat(squirrel.Dollar).
		ToSql()
	if err != nil {
		return "", fmt.Errorf("building max key query for table %s: %w", table, err)
	}
	return query, nil
}
