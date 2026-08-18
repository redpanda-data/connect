// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"context"
	"fmt"
	"slices"

	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream/sanitize"
	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/snapshot"
	"github.com/redpanda-data/connect/v4/internal/replication/incrementalsnapshot"
)

// setupIncrementalSnapshot wires a snapshot.Coordinator into the stream when
// Config.IncrementalSnapshot is enabled. It is a no-op (leaving both
// s.snapshotCoordinator and s.incrementalDB nil) otherwise.
func (s *Stream) setupIncrementalSnapshot(ctx context.Context, config *Config) error {
	incSnapshotCfg := config.IncrementalSnapshotCfg()
	if !incSnapshotCfg.IsEnabled() {
		return nil
	}

	db, err := openPgConnectionFromConfig(config)
	if err != nil {
		return fmt.Errorf("opening incremental snapshot connection: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return fmt.Errorf("pinging incremental snapshot connection: %w", err)
	}

	tableNames := incSnapshotCfg.Tables
	if len(tableNames) == 0 {
		tableNames = config.DBTables
	}

	tables := make([]incrementalsnapshot.TableID, 0, len(tableNames))
	tableSet := make(map[incrementalsnapshot.TableID]struct{}, len(tableNames))
	for _, name := range tableNames {
		table, err := normalizeTableID(config.DBSchema, name)
		if err != nil {
			_ = db.Close()
			return fmt.Errorf("resolving incremental snapshot table %q: %w", name, err)
		}
		tables = append(tables, table)
		tableSet[table] = struct{}{}
	}

	s.incrementalDB = db
	s.incrementalPKCache = make(map[string][]string)
	s.incrementalSnapshotTables = tableSet

	coordinator, err := snapshot.NewCoordinator(incrementalsnapshot.Config{
		Tables:    tables,
		ChunkSize: incSnapshotCfg.ChunkSize,
		Deps: incrementalsnapshot.Deps{
			ResolvePrimaryKey: s.resolveIncrementalPK,
			ResolveMaxKey:     s.resolveIncrementalMaxKey,
			// Wrapped to satisfy Deps' opaque any signature;
			// resolveIncrementalWatermark itself returns the concrete
			// Postgres snapshot.Watermark.
			ResolveWatermark: func(ctx context.Context) (any, error) {
				return s.resolveIncrementalWatermark(ctx)
			},
			ForceFreshTransaction: s.forceFreshIncrementalTransaction,
			FetchChunk:            s.fetchIncrementalChunk,
		},
	}, incSnapshotCfg.ResumeState)
	if err != nil {
		_ = db.Close()
		s.incrementalDB = nil
		s.incrementalPKCache = nil
		s.incrementalSnapshotTables = nil
		return fmt.Errorf("constructing incremental snapshot coordinator: %w", err)
	}
	s.snapshotCoordinator = coordinator
	return nil
}

// normalizeTableID applies the same normalization NewPgStream uses for
// DBSchema/DBTables, then unquotes: incrementalsnapshot.TableID must hold
// unquoted names to match what postgres reports on replication messages.
func normalizeTableID(schemaRaw, tableRaw string) (incrementalsnapshot.TableID, error) {
	schemaNorm, err := sanitize.NormalizePostgresIdentifier(schemaRaw)
	if err != nil {
		return incrementalsnapshot.TableID{}, fmt.Errorf("invalid schema name %q: %w", schemaRaw, err)
	}
	tableNorm, err := sanitize.NormalizePostgresIdentifier(tableRaw)
	if err != nil {
		return incrementalsnapshot.TableID{}, fmt.Errorf("invalid table name %q: %w", tableRaw, err)
	}
	schema, err := sanitize.UnquotePostgresIdentifier(schemaNorm)
	if err != nil {
		return incrementalsnapshot.TableID{}, fmt.Errorf("unquoting normalized schema name %q: %w", schemaNorm, err)
	}
	table, err := sanitize.UnquotePostgresIdentifier(tableNorm)
	if err != nil {
		return incrementalsnapshot.TableID{}, fmt.Errorf("unquoting normalized table name %q: %w", tableNorm, err)
	}
	return incrementalsnapshot.TableID{Schema: schema, Table: table}, nil
}

// incrementalPKColumns resolves and caches table's unquoted PK columns.
// Backs both ResolvePrimaryKey and incrementalStreamedRowPK, which needs the
// same columns to build a PrimaryKey for OnStreamedRow.
func (s *Stream) incrementalPKColumns(ctx context.Context, table incrementalsnapshot.TableID) ([]string, error) {
	key := table.String()
	if cols, exists := s.incrementalPKCache[key]; exists {
		return cols, nil
	}

	quoted, err := s.getPrimaryKeyColumn(ctx, TableFQN{
		Schema: sanitize.QuotePostgresIdentifier(table.Schema),
		Table:  sanitize.QuotePostgresIdentifier(table.Table),
	})
	if err != nil {
		return nil, err
	}

	cols := make([]string, len(quoted))
	for i, c := range quoted {
		unquoted, err := sanitize.UnquotePostgresIdentifier(c)
		if err != nil {
			return nil, fmt.Errorf("unquoting primary key column %q for table %s: %w", c, table, err)
		}
		cols[i] = unquoted
	}

	s.incrementalPKCache[key] = cols
	return cols, nil
}

// resolveIncrementalPK implements incrementalsnapshot.Deps.ResolvePrimaryKey.
func (s *Stream) resolveIncrementalPK(ctx context.Context, table incrementalsnapshot.TableID) ([]string, error) {
	return s.incrementalPKColumns(ctx, table)
}

// incrementalStreamedRowPK builds a PrimaryKey for a streamed DML row, since
// OnStreamedRow only accepts already-extracted values.
func (s *Stream) incrementalStreamedRowPK(ctx context.Context, table incrementalsnapshot.TableID, data any) (incrementalsnapshot.PrimaryKey, error) {
	pkCols, err := s.incrementalPKColumns(ctx, table)
	if err != nil {
		return nil, err
	}

	values, _ := data.(map[string]any)
	pk := make(incrementalsnapshot.PrimaryKey, len(pkCols))
	for i, col := range pkCols {
		pk[i] = values[col]
	}
	return pk, nil
}

// resolveIncrementalMaxKey implements incrementalsnapshot.Deps.ResolveMaxKey.
func (s *Stream) resolveIncrementalMaxKey(ctx context.Context, table incrementalsnapshot.TableID, pkCols []string, query string) (incrementalsnapshot.PrimaryKey, error) {
	rows, err := s.incrementalDB.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying max key for table %s: %w", table, err)
	}
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("getting column types for table %s max key query: %w", table, err)
	}
	scanArgs, valueGetters := prepareScannersAndGetters(columnTypes)

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("resolving max key for table %s: %w", table, err)
		}
		return nil, fmt.Errorf("table %s has no rows; incremental snapshot cannot resolve a max key for an empty table", table)
	}

	if err := rows.Scan(scanArgs...); err != nil {
		return nil, fmt.Errorf("scanning max key row for table %s: %w", table, err)
	}

	pk := make(incrementalsnapshot.PrimaryKey, len(pkCols))
	for i, getter := range valueGetters {
		val, err := getter(scanArgs[i])
		if err != nil {
			return nil, fmt.Errorf("decoding max key column %s for table %s: %w", pkCols[i], table, err)
		}
		pk[i] = val
	}
	return pk, nil
}

// resolveIncrementalWatermark is the concrete Postgres implementation
// wrapped by setupIncrementalSnapshot to satisfy Deps' opaque any signature.
func (s *Stream) resolveIncrementalWatermark(ctx context.Context) (snapshot.Watermark, error) {
	var raw string
	if err := s.incrementalDB.QueryRowContext(ctx, "SELECT txid_current_snapshot()").Scan(&raw); err != nil {
		return snapshot.Watermark{}, fmt.Errorf("querying txid_current_snapshot: %w", err)
	}
	wm, err := snapshot.ParseSnapshot(raw)
	if err != nil {
		return snapshot.Watermark{}, fmt.Errorf("parsing txid_current_snapshot result %q: %w", raw, err)
	}
	return wm, nil
}

// forceFreshIncrementalTransaction implements incrementalsnapshot.Deps.ForceFreshTransaction.
func (s *Stream) forceFreshIncrementalTransaction(ctx context.Context) error {
	var txid uint64
	if err := s.incrementalDB.QueryRowContext(ctx, "SELECT txid_current()").Scan(&txid); err != nil {
		return fmt.Errorf("querying txid_current: %w", err)
	}
	return nil
}

// fetchIncrementalChunk implements incrementalsnapshot.Deps.FetchChunk.
func (s *Stream) fetchIncrementalChunk(ctx context.Context, table incrementalsnapshot.TableID, pkCols []string, query string, args []any) ([]incrementalsnapshot.Row, error) {
	rows, err := s.incrementalDB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("fetching chunk for table %s: %w", table, err)
	}
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("getting column types for table %s: %w", table, err)
	}
	scanArgs, valueGetters := prepareScannersAndGetters(columnTypes)

	columnNames, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("getting column names for table %s: %w", table, err)
	}
	tableSchema := columnTypesToSchema(table.Table, columnNames, columnTypes)

	pkPositions := make([]int, len(pkCols))
	for i, pkCol := range pkCols {
		pkPositions[i] = slices.Index(columnNames, pkCol)
		if pkPositions[i] == -1 {
			return nil, fmt.Errorf("primary key column %s not found in chunk result for table %s", pkCol, table)
		}
	}

	var result []incrementalsnapshot.Row
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, fmt.Errorf("scanning row for table %s: %w", table, err)
		}

		data := make(map[string]any, len(valueGetters))
		for i, getter := range valueGetters {
			val, err := getter(scanArgs[i])
			if err != nil {
				return nil, fmt.Errorf("decoding column %s for table %s: %w", columnNames[i], table, err)
			}
			data[columnNames[i]] = val
		}

		pk := make(incrementalsnapshot.PrimaryKey, len(pkCols))
		for i, pos := range pkPositions {
			pk[i] = data[columnNames[pos]]
		}

		result = append(result, incrementalsnapshot.Row{
			Table:        table,
			PK:           pk,
			Data:         data,
			ColumnSchema: tableSchema,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating chunk rows for table %s: %w", table, err)
	}
	return result, nil
}

// buildIncrementalSnapshotMessages converts emitted rows into StreamMessages.
// If emitted is empty, a single sentinel checkpoint message carries just the
// state, since state can advance with nothing flushed (e.g. every buffered
// row was deduplicated).
func buildIncrementalSnapshotMessages(emitted []incrementalsnapshot.Row, state []byte) []StreamMessage {
	if len(emitted) == 0 {
		return []StreamMessage{{
			Operation:                IncrementalSnapshotCheckpointOpType,
			IncrementalSnapshotState: state,
		}}
	}

	msgs := make([]StreamMessage, len(emitted))
	for i, row := range emitted {
		msgs[i] = StreamMessage{
			Operation:    ReadOpType,
			Schema:       row.Table.Schema,
			Table:        row.Table.Table,
			Data:         row.Data,
			ColumnSchema: row.ColumnSchema,
		}
	}
	msgs[len(msgs)-1].IncrementalSnapshotState = state
	return msgs
}
