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
	"encoding/json"
	"fmt"
	"slices"

	"github.com/google/uuid"

	incsnapshot "github.com/redpanda-data/connect/v4/internal/impl/postgresql/incrementalsnapshot"
	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream/sanitize"
	"github.com/redpanda-data/connect/v4/internal/replication/incrementalsnapshot"
)

// setupIncrementalSnapshot adds a Coordinator to the stream. It does nothing
// when the snapshot is disabled, and leaves the coordinator and the
// connection nil.
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

// normalizeTableID makes a TableID from a schema name and a table name. It
// uses the same rules as NewPgStream, then removes the quotation marks: a
// TableID must hold unquoted names, because Postgres reports them in this
// form in replication messages.
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

// incrementalPKColumns reads the unquoted primary key columns of the table
// and keeps them in a cache. ResolvePrimaryKey and incrementalStreamedRowPK
// both need the same columns to make a PrimaryKey that OnStreamedRow matches.
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

// resolveIncrementalPKColumns reads the primary key columns of the table. It
// must use s.incSnapshotConn and never s.pgConn: after the stream starts,
// s.pgConn is in COPY BOTH for the replication protocol, and a normal query
// on it at the same time stops or damages the stream.
//
// Its only caller, incrementalPKColumns, can run at any time during the
// stream - from Deps.ResolvePrimaryKey when planning a chunk, and from
// incrementalStreamedRowPK on each insert, update or delete.
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
		// Postgres gives the names in normal form, so quote them.
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

// incrementalSnapshotDeps makes *Stream satisfy incrementalsnapshot.Deps,
// keeping these general method names out of the API of Stream.
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

// resolveIncrementalPK backs Deps.ResolvePrimaryKey.
func (s *Stream) resolveIncrementalPK(ctx context.Context, table incrementalsnapshot.TableID) ([]string, error) {
	return s.incrementalPKColumns(ctx, table)
}

// incrementalStreamedRowPK makes the PrimaryKey that OnStreamedRow matches
// from the data of a streamed row.
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

// canonicalizePKValue changes a decoded primary key value to one stable
// form. The window buffer keys rows on the text form of each PrimaryKey
// element - refer to newWindowKey in the shared package - so one value must
// give one key on both decode paths.
//
// The paths disagree: the stream uses decodeTextColumnData and the snapshot
// uses prepareScannersAndGetters, so one Postgres type can arrive as two Go
// types. A UUID can come as 16 bytes or as text. The buffer would then hold
// both rows and remove neither.
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

// resolveIncrementalMaxKey backs Deps.ResolveMaxKey.
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
		// An empty table has no rows to read. Return nil and no error, so
		// the coordinator moves to the next table instead of stopping the
		// replication of every table.
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

func currentSnapshotQuery(pgVersion int) string {
	// pg_current_snapshot replaces the obsolete txid_current_snapshot from
	// PostgreSQL 13. Both give the same text form.
	if pgVersion >= 13 {
		return "SELECT pg_current_snapshot()"
	}
	return "SELECT txid_current_snapshot()"
}

// resolveIncrementalWatermark backs Deps.ResolveWatermark.
func (s *Stream) resolveIncrementalWatermark(ctx context.Context) (incsnapshot.Watermark, error) {
	query := currentSnapshotQuery(s.pgVersion)
	var raw string
	if err := s.incSnapshotConn.QueryRowContext(ctx, query).Scan(&raw); err != nil {
		return incsnapshot.Watermark{}, fmt.Errorf("querying current snapshot with %q: %w", query, err)
	}
	wm, err := incsnapshot.ParseSnapshot(raw)
	if err != nil {
		return incsnapshot.Watermark{}, fmt.Errorf("parsing current snapshot result %q: %w", raw, err)
	}
	return wm, nil
}

// forceFreshIncrementalTransaction backs Deps.ForceFreshTransaction.
func (s *Stream) forceFreshIncrementalTransaction(ctx context.Context) error {
	var txid uint64
	if err := s.incSnapshotConn.QueryRowContext(ctx, "SELECT txid_current()").Scan(&txid); err != nil {
		return fmt.Errorf("querying txid_current: %w", err)
	}
	return nil
}

// fetchIncrementalChunk backs Deps.FetchChunk.
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

// buildIncrementalSnapshotMessages makes StreamMessage values from the rows
// and puts the state on the last one. With no rows it makes one message that
// holds the state only, because the state can move forward with no rows - for
// example when the stream has already delivered each buffered row.
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

// advanceIncrementalSnapshot reports a committed transaction to the snapshot,
// sending whatever it releases before the commit reaches the consumer, so the
// consumer never sees progress past changes it cannot yet read.
//
// A no-op when the snapshot is disabled, or when xid is zero: no BEGIN
// supplied one, and zero sorts below every watermark, so it would open or
// close the window spuriously.
func (s *Stream) advanceIncrementalSnapshot(ctx context.Context, xid uint32) error {
	if s.incSnapshotCoordinator == nil || xid == 0 {
		return nil
	}

	// One commit can release several chunks, when the database is quiet
	// enough to need no deduplication. emit runs once per chunk, and its send
	// to s.messages paces the drain.
	emit := func(rows []incrementalsnapshot.Row) error {
		if len(rows) > 0 {
			s.logger.Debugf("Incremental snapshot: flushed %d row(s) for table %s", len(rows), rows[0].Table)
			s.monitor.UpdateSnapshotProgressForTable(tableFQN(rows[0].Table), len(rows))
		} else {
			s.logger.Debugf("Incremental snapshot: checkpoint advanced with no rows to flush (fully deduplicated)")
		}
		checkpoint := s.incSnapshotCoordinator.State()
		s.reportTableTransition(checkpoint.CurrentTable)

		state, err := json.Marshal(checkpoint)
		if err != nil {
			return fmt.Errorf("serializing incremental snapshot state: %w", err)
		}
		select {
		case s.messages <- buildIncrementalSnapshotMessages(rows, state):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	changed, err := s.incSnapshotCoordinator.OnCommit(ctx, xid, emit)
	if err != nil {
		return fmt.Errorf("advancing incremental snapshot: %w", err)
	}
	if changed && s.incSnapshotCoordinator.Done() {
		// The last table never sees a following checkpoint, so report it here.
		s.reportTableTransition(nil)
		s.logger.Info("Incremental snapshot: complete")
		for table := range s.incSnapshotTables {
			s.monitor.MarkSnapshotComplete(tableFQN(table))
		}
	}
	return nil
}

func (s *Stream) reportTableTransition(current *incrementalsnapshot.TableID) {
	previous := s.incSnapshotLastTable
	s.incSnapshotLastTable = current
	if sameTable(previous, current) {
		return
	}
	if previous != nil {
		s.logger.Infof("Incremental snapshot: finished table %s", *previous)
		s.monitor.MarkSnapshotComplete(tableFQN(*previous))
	}
	if current != nil {
		s.logger.Infof("Incremental snapshot: starting table %s", *current)
	}
}

func sameTable(a, b *incrementalsnapshot.TableID) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func tableFQN(table incrementalsnapshot.TableID) TableFQN {
	return TableFQN{
		Schema: sanitize.QuotePostgresIdentifier(table.Schema),
		Table:  sanitize.QuotePostgresIdentifier(table.Table),
	}
}

// deduplicateStreamedRow tells the coordinator that the stream carries this
// row, which drops any copy the window buffer holds for the same key. That
// copy is stale or redundant, and this message gives the current value.
//
// A no-op unless the snapshot runs and the table is one it snapshots.
func (s *Stream) deduplicateStreamedRow(ctx context.Context, message *StreamMessage) error {
	if s.incSnapshotCoordinator == nil {
		return nil
	}
	switch message.Operation {
	case InsertOpType, UpdateOpType, DeleteOpType:
	default:
		return nil
	}

	table := incrementalsnapshot.TableID{Schema: message.Schema, Table: message.Table}
	if _, tracked := s.incSnapshotTables[table]; !tracked {
		return nil
	}

	pk, err := s.incrementalStreamedRowPK(ctx, table, message.Data)
	if err != nil {
		return fmt.Errorf("resolving primary key for incremental snapshot deduplication on table %s: %w", table, err)
	}
	if s.incSnapshotCoordinator.OnStreamedRow(table, pk) {
		s.logger.Debugf("Incremental snapshot: deduplicated live row for table %s pk=%v", table, pk)
	}
	return nil
}

// reportResumeReconciliation logs what Start made of a resumed checkpoint
// whose table set no longer matches the config. It logs nothing when the two
// agree, which is the normal case.
func (s *Stream) reportResumeReconciliation() {
	if added := s.incSnapshotCoordinator.AddedOnResume(); len(added) > 0 {
		s.logger.Infof("Incremental snapshot: %d table(s) added to the config since the checkpoint, queued for backfill: %v", len(added), added)
	}
	if removed := s.incSnapshotCoordinator.RemovedOnResume(); len(removed) > 0 {
		s.logger.Warnf(
			"Incremental snapshot: dropped %d table(s) the checkpoint covers but the config no longer lists: %v. A table that was part-read stops there, so its remaining rows are not backfilled",
			len(removed), removed,
		)
	}
}
