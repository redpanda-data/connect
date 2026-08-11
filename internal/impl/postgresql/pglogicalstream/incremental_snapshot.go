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

	"github.com/google/uuid"

	incsnapshot "github.com/redpanda-data/connect/v4/internal/impl/postgresql/incrementalsnapshot"
	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream/sanitize"
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

	s.incSnapshotConn = db
	s.incSnapshotPKCache = make(map[string][]string)
	s.incSnapshotTables = tableSet

	coordinator, err := incsnapshot.NewCoordinator(incsnapshot.CoordinatorConfig{
		Tables:    tables,
		ChunkSize: incSnapshotCfg.ChunkSize,
		Deps:      incrementalSnapshotDeps{stream: s},
	}, incSnapshotCfg.ResumeState)
	if err != nil {
		_ = db.Close()
		s.incSnapshotConn = nil
		s.incSnapshotPKCache = nil
		s.incSnapshotTables = nil
		return fmt.Errorf("constructing incremental snapshot coordinator: %w", err)
	}
	s.incSnapshotCoordinator = coordinator
	s.logger.Debugf("Incremental snapshot: enabled for %d table(s) %v, chunk_size=%d", len(tables), tables, incSnapshotCfg.ChunkSize)
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
	if cols, exists := s.incSnapshotPKCache[key]; exists {
		return cols, nil
	}

	quoted, err := s.resolveIncrementalPKColumns(ctx, TableFQN{
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

	s.incSnapshotPKCache[key] = cols
	return cols, nil
}

// resolveIncrementalPKColumns resolves table's primary key columns over
// s.incrementalDB rather than s.pgConn. incrementalPKColumns (the sole
// caller) may run at any point during live streaming -- from
// planNextChunk's Deps.ResolvePrimaryKey, or from incrementalStreamedRowPK on
// a live INSERT/UPDATE/DELETE -- so it must never touch s.pgConn, which is
// dedicated to the replication protocol (COPY BOTH) once streaming starts;
// issuing a plain query on it concurrently deadlocks/corrupts the stream.
func (s *Stream) resolveIncrementalPKColumns(ctx context.Context, table TableFQN) ([]string, error) {
	q, err := primaryKeyColumnsQuery(table.String())
	if err != nil {
		return nil, fmt.Errorf("sanitizing query: %w", err)
	}

	rows, err := s.incSnapshotConn.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("querying primary key columns for table %s: %w", table, err)
	}
	defer rows.Close()

	var pkColumns []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("scanning primary key column for table %s: %w", table, err)
		}
		// Postgres gives us back normalized identifiers here - we need to quote them.
		pkColumns = append(pkColumns, sanitize.QuotePostgresIdentifier(col))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating primary key columns for table %s: %w", table, err)
	}

	if len(pkColumns) == 0 {
		return nil, fmt.Errorf("no primary key found for table %s", table)
	}

	return pkColumns, nil
}

// incrementalSnapshotDeps adapts *Stream to satisfy incrementalsnapshot.Deps,
// keeping these generic-sounding method names off Stream's own API.
type incrementalSnapshotDeps struct {
	stream *Stream
}

var _ incsnapshot.Deps = incrementalSnapshotDeps{}

func (d incrementalSnapshotDeps) ResolvePrimaryKey(ctx context.Context, table incrementalsnapshot.TableID) ([]string, error) {
	return d.stream.resolveIncrementalPK(ctx, table)
}

func (d incrementalSnapshotDeps) ResolveMaxKey(ctx context.Context, table incrementalsnapshot.TableID, pkColumnsUnquoted []string) (incrementalsnapshot.PrimaryKey, error) {
	query, err := incsnapshot.BuildMaxKeyQuery(table, pkColumnsUnquoted)
	if err != nil {
		return nil, err
	}
	return d.stream.resolveIncrementalMaxKey(ctx, table, pkColumnsUnquoted, query)
}

func (d incrementalSnapshotDeps) ResolveWatermark(ctx context.Context) (incsnapshot.Watermark, error) {
	return d.stream.resolveIncrementalWatermark(ctx)
}

func (d incrementalSnapshotDeps) ForceFreshTransaction(ctx context.Context) error {
	return d.stream.forceFreshIncrementalTransaction(ctx)
}

func (d incrementalSnapshotDeps) FetchChunk(ctx context.Context, table incrementalsnapshot.TableID, pkColumnsUnquoted []string, lower, upper incrementalsnapshot.PrimaryKey, limit int) ([]incrementalsnapshot.Row, error) {
	query, args, err := incsnapshot.BuildChunkQuery(table, pkColumnsUnquoted, lower, upper, limit)
	if err != nil {
		return nil, err
	}
	return d.stream.fetchIncrementalChunk(ctx, table, pkColumnsUnquoted, query, args)
}

// resolveIncrementalPK backs incrementalSnapshotDeps.ResolvePrimaryKey.
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
		pk[i] = canonicalizePKValue(values[col])
	}
	return pk, nil
}

// canonicalizePKValue normalizes a decoded primary key value into a stable
// representation, so the dedup window's key (built by simply formatting each
// PrimaryKey element, see incrementalsnapshot.newWindowKey) is identical for
// the same underlying value regardless of which decode path produced it.
// This matters because the live streaming path (decodeTextColumnData) and
// the incrementalDB backfill path (prepareScannersAndGetters) don't always
// decode a given Postgres type to the same Go representation -- e.g. a raw
// [16]byte UUID vs. its canonical hyphenated string -- which would otherwise
// silently defeat dedup between a snapshotted row and its streamed
// counterpart.
func canonicalizePKValue(v any) any {
	switch val := v.(type) {
	case [16]byte:
		return uuid.UUID(val).String()
	case []byte:
		return string(val)
	default:
		return val
	}
}

// resolveIncrementalMaxKey backs incrementalSnapshotDeps.ResolveMaxKey.
func (s *Stream) resolveIncrementalMaxKey(ctx context.Context, table incrementalsnapshot.TableID, pkCols []string, query string) (incrementalsnapshot.PrimaryKey, error) {
	rows, err := s.incSnapshotConn.QueryContext(ctx, query)
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
		// An empty table has nothing to backfill; report it as such (nil, nil)
		// rather than erroring, so the coordinator moves on to the next table
		// instead of aborting replication for every table.
		s.logger.Debugf("Incremental snapshot: table %s is empty, skipping", table)
		return nil, nil
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
		pk[i] = canonicalizePKValue(val)
	}
	s.logger.Debugf("Incremental snapshot: table %s upper bound resolved to pk=%v", table, pk)
	return pk, nil
}

// resolveIncrementalWatermark is the concrete Postgres implementation
// wrapped by setupIncrementalSnapshot to satisfy Deps' opaque any signature.
func (s *Stream) resolveIncrementalWatermark(ctx context.Context) (incsnapshot.Watermark, error) {
	var raw string
	if err := s.incSnapshotConn.QueryRowContext(ctx, "SELECT txid_current_snapshot()").Scan(&raw); err != nil {
		return incsnapshot.Watermark{}, fmt.Errorf("querying txid_current_snapshot: %w", err)
	}
	wm, err := incsnapshot.ParseSnapshot(raw)
	if err != nil {
		return incsnapshot.Watermark{}, fmt.Errorf("parsing txid_current_snapshot result %q: %w", raw, err)
	}
	return wm, nil
}

// forceFreshIncrementalTransaction backs incrementalSnapshotDeps.ForceFreshTransaction.
func (s *Stream) forceFreshIncrementalTransaction(ctx context.Context) error {
	var txid uint64
	if err := s.incSnapshotConn.QueryRowContext(ctx, "SELECT txid_current()").Scan(&txid); err != nil {
		return fmt.Errorf("querying txid_current: %w", err)
	}
	return nil
}

// fetchIncrementalChunk backs incrementalSnapshotDeps.FetchChunk.
func (s *Stream) fetchIncrementalChunk(ctx context.Context, table incrementalsnapshot.TableID, pkCols []string, query string, args []any) ([]incrementalsnapshot.Row, error) {
	rows, err := s.incSnapshotConn.QueryContext(ctx, query, args...)
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
			pk[i] = canonicalizePKValue(data[columnNames[pos]])
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
	if len(result) == 0 {
		s.logger.Debugf("Incremental snapshot: fetched 0 rows for table %s, table exhausted", table)
	} else {
		s.logger.Debugf("Incremental snapshot: fetched %d row(s) for table %s (pk %v..%v)", len(result), table, result[0].PK, result[len(result)-1].PK)
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
