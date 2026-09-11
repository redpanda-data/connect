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
	"errors"
	"fmt"
	"slices"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	incsnapshot "github.com/redpanda-data/connect/v4/internal/impl/postgresql/incrementalsnapshot"
	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream/sanitize"
	"github.com/redpanda-data/connect/v4/internal/replication"
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

	// A signal may only ask for a replicated table. An empty DBTables means
	// the publication is FOR ALL TABLES, so leave the set nil to accept any.
	if len(config.DBTables) > 0 {
		s.incSnapshotReplicated = make(map[incrementalsnapshot.TableID]struct{}, len(config.DBTables))
		for _, name := range config.DBTables {
			table, err := normalizeTableID(config.DBSchema, name)
			if err != nil {
				_ = db.Close()
				return fmt.Errorf("resolving replicated table %q: %w", name, err)
			}
			s.incSnapshotReplicated[table] = struct{}{}
		}
	}

	s.incSnapshotConn = db
	s.incSnapshotPKCache = make(map[string][]string)

	// Nothing is queued at first: tables are requested by signal. A resumed
	// checkpoint brings back what the last run covered.
	coordinator, err := incsnapshot.NewCoordinator(incsnapshot.CoordinatorConfig{
		ChunkSize: incSnapshotCfg.ChunkSize,
		Deps:      incrementalSnapshotDeps{stream: s},
		OnTableDropped: func(table incrementalsnapshot.TableID, err error) {
			// Accepted by checkBackfillable, then dropped or its key
			// removed before it was planned.
			s.logger.Warnf("Incremental snapshot: dropped table %s from the queue, it can no longer be backfilled: %s", table, err)
		},
	}, incSnapshotCfg.ResumeState)
	if err != nil {
		_ = db.Close()
		s.incSnapshotConn = nil
		s.incSnapshotPKCache = nil
		return fmt.Errorf("constructing incremental snapshot coordinator: %w", err)
	}
	s.incSnapshotCoordinator = coordinator
	var resuming int
	if resume := incSnapshotCfg.ResumeState; resume != nil {
		resuming = len(resume.Tables)
	}
	s.logger.Debugf("Incremental snapshot: enabled with chunk_size=%d, resuming %d table(s)", incSnapshotCfg.ChunkSize, resuming)
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
		if errIsPermanent(err) {
			return nil, fmt.Errorf("%w: reading primary key columns for table %s: %w", incrementalsnapshot.ErrTableUnusable, table, err)
		}
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
		// Unusable, not a failure: the backfill pages by key and no retry
		// will produce one. REPLICA IDENTITY FULL replicates a table
		// without one, so this is reachable.
		return nil, fmt.Errorf("%w: no primary key found for table %s", incrementalsnapshot.ErrTableUnusable, table)
	}

	return pkColumns, nil
}

const (
	pgErrUndefinedTable        = "42P01"
	pgErrInvalidSchemaName     = "3F000"
	pgErrInsufficientPrivilege = "42501"
)

func errIsPermanent(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	switch pgErr.Code {
	case pgErrUndefinedTable, pgErrInvalidSchemaName, pgErrInsufficientPrivilege:
		return true
	}
	return false
}

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
	// The heartbeat only owes transaction ids while there is something to
	// read. Mirrored on every commit, not on the transition: OnCommit
	// reports no change once idle, so a transition-only update could never
	// clear this.
	s.incSnapshotBackfilling.Store(!s.incSnapshotCoordinator.Idle())

	if changed && s.incSnapshotCoordinator.Idle() {
		// The last table sees no following checkpoint, so report it here.
		// More may arrive by signal, so this is not a completion.
		s.reportTableTransition(nil)
		s.logger.Info("Incremental snapshot: queue empty, waiting for a snapshot signal")
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

	// Only the table being read can have a buffered row to supersede. Every
	// other one -- finished, or queued behind this one -- would cost a key
	// lookup whose result OnStreamedRow discards, and a failed lookup would
	// stop replication for a table the snapshot is not even touching.
	table := incrementalsnapshot.TableID{Schema: message.Schema, Table: message.Table}
	if !s.incSnapshotCoordinator.Snapshotting(table) {
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

// errSignalRejected marks a signal the connector will never honour, as
// against a failure to judge one.
//
// Only a rejection may be logged and skipped. The signal row is forwarded
// and its position acknowledged, so a request dropped on a failed check is
// dropped for good -- an acknowledged row never streams again. Anything else
// must reach the caller, which restarts the stream and redelivers the row.
var errSignalRejected = errors.New("rejected")

// checkBackfillable rejects a table the snapshot could not read, so a bad
// request fails where the user can see it rather than once it is queued and
// checkpointed. Refer to incrementalsnapshot.ErrTableUnusable.
//
// A rejection wraps errSignalRejected; a failure to check does not.
func (s *Stream) checkBackfillable(ctx context.Context, table incrementalsnapshot.TableID) error {
	if err := s.checkReplicated(table); err != nil {
		return fmt.Errorf("%w: %w", errSignalRejected, err)
	}
	// The backfill pages by key, so a table without one can never be read.
	if _, err := s.incrementalPKColumns(ctx, table); err != nil {
		if errors.Is(err, incrementalsnapshot.ErrTableUnusable) {
			return fmt.Errorf("%w: %w", errSignalRejected, err)
		}
		// The query itself failed, so whether the table is usable is still
		// unknown. Propagate, and the redelivered row asks again.
		return err
	}
	return nil
}

// checkReplicated rejects a table the publication does not carry.
//
// Its backfill would have no live changes to deduplicate against, so a write
// landing after its chunk is read would be lost: the stale snapshot row
// would be the last thing delivered for that key, with nothing following to
// correct it.
func (s *Stream) checkReplicated(table incrementalsnapshot.TableID) error {
	if s.incSnapshotReplicated == nil {
		return nil // FOR ALL TABLES
	}
	if _, replicated := s.incSnapshotReplicated[table]; replicated {
		return nil
	}
	return fmt.Errorf(
		"table %s is not replicated, so a write during its backfill could not be deduplicated and would be lost: add it to the input's tables",
		table,
	)
}

// snapshotSignalTables reads a snapshot signal's table list, or nil when the
// row is not one. A malformed payload is an error, not a row to ignore: the
// request came from a user, who would otherwise wait for a backfill that
// never starts.
func (s *Stream) snapshotSignalTables(ctx context.Context, message *StreamMessage) ([]incrementalsnapshot.TableID, error) {
	if s.incSnapshotCoordinator == nil || message.Operation != InsertOpType {
		return nil, nil
	}
	if s.signalTable == nil || message.Schema != s.signalTable.Schema || message.Table != s.signalTable.Table {
		return nil, nil
	}

	row, isMap := message.Data.(map[string]any)
	if !isMap {
		return nil, fmt.Errorf("signal row: %w: expected map data, got %T", errSignalRejected, message.Data)
	}
	if signalType, _ := row["type"].(string); signalType != replication.SnapshotSignalType {
		return nil, nil
	}

	payload, isText := row["data"].(string)
	if !isText {
		return nil, fmt.Errorf("signal row: %w: expected string data column, got %T", errSignalRejected, row["data"])
	}
	var signal replication.SnapshotSignal
	if err := json.Unmarshal([]byte(payload), &signal); err != nil {
		return nil, fmt.Errorf("signal row: %w: parsing %s payload: %w", errSignalRejected, replication.SnapshotSignalType, err)
	}
	if len(signal.Tables) == 0 {
		return nil, fmt.Errorf("signal row: %w: %s payload lists no tables", errSignalRejected, replication.SnapshotSignalType)
	}

	tables := make([]incrementalsnapshot.TableID, 0, len(signal.Tables))
	for _, name := range signal.Tables {
		table, err := normalizeTableID(s.snapshotSchema, name)
		if err != nil {
			return nil, fmt.Errorf("signal row: %w: resolving table %q: %w", errSignalRejected, name, err)
		}
		if err := s.checkBackfillable(ctx, table); err != nil {
			// Reject the whole request rather than part of it: a caller who
			// asked for three tables and got two would have no way to tell.
			return nil, fmt.Errorf("signal row: %w", err)
		}
		tables = append(tables, table)
	}
	return tables, nil
}

func (s *Stream) dispatchSnapshotSignal(ctx context.Context, message *StreamMessage) error {
	tables, err := s.snapshotSignalTables(ctx, message)
	if err != nil {
		if errors.Is(err, errSignalRejected) {
			// The connector will never honour it, so log and carry on: the
			// row still reaches the consumer for inspection.
			s.logger.Errorf("Incremental snapshot: %s", err)
			return nil
		}
		// The signal could not be judged, so returning is the only way to
		// keep the request -- refer to errSignalRejected.
		return err
	}
	if len(tables) == 0 {
		return nil
	}

	added := s.incSnapshotCoordinator.AddTables(tables)
	if len(added) > 0 {
		// The heartbeat must carry transaction ids again: on a quiet table
		// it is the only thing that advances the backfill.
		s.incSnapshotBackfilling.Store(true)
	}
	if len(added) == 0 {
		s.logger.Warnf("Incremental snapshot: signal asked for %v, all of which this run already covers, so nothing was queued", tables)
		return nil
	}
	s.logger.Infof("Incremental snapshot: signal queued %d table(s) for backfill: %v", len(added), added)
	if len(added) < len(tables) {
		s.logger.Warnf("Incremental snapshot: signal asked for %v but this run already covers some of them, so only %v was queued", tables, added)
	}
	return nil
}
