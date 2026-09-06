// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package incrementalsnapshot

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Masterminds/squirrel"

	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream/sanitize"
	"github.com/redpanda-data/connect/v4/internal/replication/incrementalsnapshot"
)

// rowTuple writes a PrimaryKey as a Postgres ROW constructor, for example
// "ROW(?, ?)". A query can then compare two rows, for example
// ROW(pk1, pk2) > ROW(?, ?). This form supports composite primary keys.
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

// quotedRowExpr writes the primary key columns as a Postgres ROW
// constructor, for example ROW("id", "tenant_id"). A query uses this form on
// the left side of a row comparison.
func quotedRowExpr(pkColsUnquoted []string) string {
	return "ROW(" + strings.Join(quotedColumns(pkColsUnquoted), ", ") + ")"
}

func quotedTableName(table incrementalsnapshot.TableID) string {
	return sanitize.QuotePostgresIdentifier(table.Schema) + "." + sanitize.QuotePostgresIdentifier(table.Table)
}

// BuildChunkQuery makes the SELECT statement for one chunk. It supports
// incrementalsnapshot.Deps.FetchChunk.
//
// lower can be nil for the first chunk of a table, and the query then has no
// lower bound. upper is the largest primary key of the table and must not be
// nil.
//
// The query selects all columns, because this package does not know the
// column names. The caller decodes the result.
func BuildChunkQuery(table incrementalsnapshot.TableID, pkColsUnquoted []string, lower, upper incrementalsnapshot.PrimaryKey, limit int) (query string, args []any, err error) {
	if len(pkColsUnquoted) == 0 {
		return "", nil, errors.New("BuildChunkQuery: no primary key columns provided")
	}
	if upper == nil {
		return "", nil, fmt.Errorf("BuildChunkQuery: upper bound must not be nil for table %s", table)
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

// BuildMaxKeyQuery makes the statement that reads the largest primary key of
// the table. It supports incrementalsnapshot.Deps.ResolveMaxKey.
func BuildMaxKeyQuery(table incrementalsnapshot.TableID, pkColsUnquoted []string) (query string, err error) {
	if len(pkColsUnquoted) == 0 {
		return "", errors.New("BuildMaxKeyQuery: no primary key columns provided")
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
