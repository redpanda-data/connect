// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/Masterminds/squirrel"
	"github.com/jackc/pgtype"

	_ "github.com/lib/pq"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream/sanitize"
	"github.com/redpanda-data/connect/v4/internal/pool"
)

// snapshotter is a structure that allows the creation of a snapshot of a database at a given point in time
// At the time we initialize logical replication - we specify what we want to export the snapshot.
// This snapshot exists until the connection that created the replication slot remains open.
// Therefore snapshotter opens another connection to the database and sets the transaction to the snapshot.
// This allows you to read the data that was in the database at the time of the snapshot creation.
type snapshotter struct {
	connPool     *sql.DB
	logger       *service.Logger
	snapshotName string
	// The TXN for the snapshot phase
	txnPool pool.Capped[*sql.Tx]
}

// newSnapshotter creates a new Snapshotter instance
func newSnapshotter(
	dbDSN string,
	logger *service.Logger,
	snapshotName string,
	maxReaders int,
) (*snapshotter, error) {
	pgConn, err := openPgConnectionFromConfig(dbDSN)
	if err != nil {
		return nil, err
	}
	s := &snapshotter{
		connPool:     pgConn,
		logger:       logger,
		snapshotName: snapshotName,
	}
	s.txnPool = pool.NewCapped(maxReaders, s.openTxn)
	return s, nil
}

func (s *snapshotter) openTxn(ctx context.Context, id int) (*sql.Tx, error) {
	// Use a background context because we explicitly want the Tx to be long lived, we explicitly close it in the close method
	tx, err := s.connPool.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return nil, fmt.Errorf("unable to start reader txn: %w", err)
	}
	sq, err := sanitize.SQLQuery("SET TRANSACTION SNAPSHOT $1", s.snapshotName)
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, sq); err != nil {
		return nil, fmt.Errorf("unable to set txn snapshot to %s: %w", s.snapshotName, err)
	}
	// Oh postgres, pg hackers will tell you the statistics/analyzer just aren't tuned right or up to date,
	// and they are probably right, but this is the easiest way to tell postgres that we actually want to
	// use the index. This is especially import for the key sampling, because otherwise it's likely that
	// postgres will scan the whole table.
	if _, err := tx.ExecContext(ctx, "SET LOCAL enable_seqscan = OFF"); err != nil {
		return nil, fmt.Errorf("unable to deprioritize seqscans for snapshot connection: %w", err)
	}
	return tx, nil
}

func (s *snapshotter) Prepare(ctx context.Context) error {
	var txns []*sql.Tx
	var errs []error
	for range s.txnPool.Cap() {
		tx, err := s.txnPool.Acquire(ctx)
		if err != nil {
			errs = append(errs, err)
		} else {
			txns = append(txns, tx)
		}
	}
	for _, tx := range txns {
		s.txnPool.Release(tx)
	}
	return errors.Join(errs...)
}

type snapshotTxn struct {
	tx     *sql.Tx
	logger *service.Logger
}

func (s *snapshotter) AcquireReaderTxn(ctx context.Context) (*snapshotTxn, error) {
	tx, err := s.txnPool.Acquire(ctx)
	return &snapshotTxn{tx: tx, logger: s.logger}, err
}

func (s *snapshotter) ReleaseReaderTxn(tx *snapshotTxn) {
	s.txnPool.Release(tx.tx)
}

func (s *snapshotter) releaseSnapshot() error {
	var errs []error
	for {
		txn, ok := s.txnPool.TryAcquireExisting()
		if !ok {
			break
		}
		if err := txn.Rollback(); err != nil {
			errs = append(errs, err)
		}
	}
	s.txnPool.Reset()
	return errors.Join(errs...)
}

func (s *snapshotter) closeConn() error {
	if err := s.releaseSnapshot(); err != nil {
		s.logger.Warnf("unable to release snapshot: %v", err)
	}
	s.txnPool.Reset()
	if err := s.connPool.Close(); err != nil {
		return err
	}

	return nil
}

type primaryKey []any

func (s *snapshotTxn) randomlySampleKeyspace(
	ctx context.Context,
	table TableFQN,
	pkColumns []string,
	numSamples int,
) (splits []primaryKey, err error) {
	// ensure each CTE name is prefixed with `_rpcn__` so we don't clash with the user table name.
	query := `
WITH

_rpcn__table_stats AS (
  SELECT
    relpages AS page_count
  FROM
    pg_class
  WHERE
  oid = $1::regclass
),

_rpcn__sampled_pages AS (
  SELECT
    DISTINCT
  ON
    -- Only get distinct pages - I don't know how else to extract only
    -- the page numbers other than string manipulation :(
    (split_part(ctid::text, ',', 1)) ctid
  FROM
    $TABLE
  TABLESAMPLE
    SYSTEM ( (
      SELECT
        LEAST(100.0, GREATEST(0.0001, 100.0 * ($REQUESTED_SAMPLES) / GREATEST(page_count, 1)))
      FROM
        _rpcn__table_stats) )
),
-- Force materialization of this CTE to prevent the query planner from merging this with
-- the output. When merged, the planner will likely choose to scan the entire primary key
-- index which is slow. However we really don't want that, we just want to sample, *then*
-- lookup the primary key as a secondary step in the plan. It's really just the ORDER BY
-- clause on the primary key that causes the planner to do that, so adding the optimization
-- barrier in between prevents it.
_rpcn__sampled_keys AS MATERIALIZED (
  SELECT
    $PRIMARY_KEY_COLUMNS
  FROM
    $TABLE t
  INNER JOIN
    _rpcn__sampled_pages sp
  ON
    t.ctid = sp.ctid 
)
  SELECT *
  FROM _rpcn__sampled_keys t
  ORDER BY
    $PRIMARY_KEY_COLUMNS
`

	pkColumns = slices.Clone(pkColumns)

	for i, col := range pkColumns {
		pkColumns[i] = "t." + col
	}

	query = strings.NewReplacer(
		"$PRIMARY_KEY_COLUMNS", strings.Join(pkColumns, ", "),
		"$TABLE", table.String(),
		"$REQUESTED_SAMPLES", strconv.Itoa(numSamples),
	).Replace(query)

	query, err = sanitize.SQLQuery(query, table.String())
	if err != nil {
		return nil, fmt.Errorf("failed to sanitize query: %w", err)
	}
	rows, err := s.tx.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("unable to execute table sampling query: %w", err)
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to compute column types for key sampling: %w", err)
	}
	scanArgs, valueGetters := prepareScannersAndGetters(columnTypes)
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, fmt.Errorf("unable to scan args for tablesample query: %w", err)
		}
		var data = make(primaryKey, len(valueGetters))
		for i, getter := range valueGetters {
			var val any
			if val, err = getter(scanArgs[i]); err != nil {
				return nil, fmt.Errorf("unable to decode column %s: %w", pkColumns[i], err)
			}
			data[i] = val
		}
		splits = append(splits, data)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("unable to execute sample table query: %w", err)
	}
	return splits, nil
}

type tuple struct {
	elements []any
}

//nolint:stylecheck // This is implementing an interface
func (t *tuple) ToSql() (sql string, args []any, err error) {
	sql = "(" + strings.Join(slices.Repeat([]string{"?"}, len(t.elements)), ", ") + ")"
	args = t.elements
	return
}

var _ squirrel.Sqlizer = &tuple{}

func (s *snapshotTxn) querySnapshotData(ctx context.Context, table TableFQN, minExclusive primaryKey, maxInclusive primaryKey, pkColumns []string, limit int) (rows *sql.Rows, err error) {
	pred := squirrel.And{}
	pkAsTuple := "(" + strings.Join(pkColumns, ", ") + ")"
	if minExclusive != nil {
		pred = append(pred, squirrel.ConcatExpr(pkAsTuple, " > ", &tuple{minExclusive}))
	}
	if maxInclusive != nil {
		pred = append(pred, squirrel.ConcatExpr(pkAsTuple, " <= ", &tuple{maxInclusive}))
	}

	q, args, err := squirrel.Select("*").
		From(table.String()).
		Where(pred).
		OrderBy(pkColumns...).
		Limit(uint64(limit)).
		PlaceholderFormat(squirrel.Dollar).
		ToSql()

	if err != nil {
		return nil, fmt.Errorf("unable to generate SQL query for table scan: %w", err)
	}

	s.logger.Tracef("running snapshot query: %s", q)

	rows, err = s.tx.QueryContext(ctx, q, args...)

	if err != nil {
		return nil, err
	}
	return rows, nil
}

func prepareScannersAndGetters(columnTypes []*sql.ColumnType) ([]any, []func(any) (any, error)) {
	scanArgs := make([]any, len(columnTypes))
	valueGetters := make([]func(any) (any, error), len(columnTypes))

	for i, v := range columnTypes {
		switch v.DatabaseTypeName() {
		case "VARCHAR", "TEXT", "UUID", "TIMESTAMP":
			scanArgs[i] = new(sql.NullString)
			valueGetters[i] = func(v any) (any, error) {
				str := v.(*sql.NullString)
				if !str.Valid {
					return nil, nil
				}
				return str.String, nil
			}
		case "BOOL":
			scanArgs[i] = new(sql.NullBool)
			valueGetters[i] = func(v any) (any, error) {
				val := v.(*sql.NullBool)
				if !val.Valid {
					return nil, nil
				}
				return val.Bool, nil
			}
		case "INT4":
			scanArgs[i] = new(sql.NullInt64)
			valueGetters[i] = func(v any) (any, error) {
				val := v.(*sql.NullInt64)
				if !val.Valid {
					return nil, nil
				}
				return val.Int64, nil
			}
		case "JSONB":
			scanArgs[i] = new(sql.NullString)
			valueGetters[i] = func(v any) (any, error) {
				str := v.(*sql.NullString)
				if !str.Valid {
					return nil, nil
				}
				payload := str.String
				if payload == "" {
					return payload, nil
				}
				var dst any
				if err := json.Unmarshal([]byte(v.(*sql.NullString).String), &dst); err != nil {
					return nil, err
				}

				return dst, nil
			}
		case "INET":
			scanArgs[i] = new(sql.NullString)
			valueGetters[i] = func(v any) (any, error) {
				inet := pgtype.Inet{}
				val := v.(*sql.NullString)
				if !val.Valid {
					return nil, nil
				}
				if err := inet.Scan(val.String); err != nil {
					return nil, err
				}

				return inet.IPNet.String(), nil
			}
		case "TSRANGE":
			scanArgs[i] = new(sql.NullString)
			valueGetters[i] = func(v any) (any, error) {
				newArray := pgtype.Tsrange{}
				val := v.(*sql.NullString)
				if !val.Valid {
					return nil, nil
				}
				if err := newArray.Scan(val.String); err != nil {
					return nil, err
				}

				vv, _ := newArray.Value()
				return vv, nil
			}
		case "_INT4":
			scanArgs[i] = new(sql.NullString)
			valueGetters[i] = func(v any) (any, error) {
				newArray := pgtype.Int4Array{}
				val := v.(*sql.NullString)
				if !val.Valid {
					return nil, nil
				}
				if err := newArray.Scan(val.String); err != nil {
					return nil, err
				}

				return newArray.Elements, nil
			}
		case "_TEXT":
			scanArgs[i] = new(sql.NullString)
			valueGetters[i] = func(v any) (any, error) {
				newArray := pgtype.TextArray{}
				val := v.(*sql.NullString)
				if !val.Valid {
					return nil, nil
				}
				if err := newArray.Scan(val.String); err != nil {
					return nil, err
				}

				return newArray.Elements, nil
			}
		default:
			scanArgs[i] = new(sql.NullString)
			valueGetters[i] = func(v any) (any, error) {
				val := v.(*sql.NullString)
				if !val.Valid {
					return nil, nil
				}
				return val.String, nil
			}
		}
	}

	return scanArgs, valueGetters
}
