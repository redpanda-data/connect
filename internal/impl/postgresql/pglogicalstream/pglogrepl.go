// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

// Package pglogrepl implements PostgreSQL logical replication client functionality.
//
// pglogrepl uses package github.com/jackc/pgconn as its underlying PostgreSQL connection.
// Use pgconn to establish a connection to PostgreSQL and then use the pglogrepl functions
// on that connection.
//
// Proper use of this package requires understanding the underlying PostgreSQL concepts.
// See https://www.postgresql.org/docs/current/protocol-replication.html.

import (
	"context"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgio"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream/sanitize"
)

const (
	// XLogDataByteID is the byte ID for XLogData messages.
	XLogDataByteID = 'w'
	// PrimaryKeepaliveMessageByteID is the byte ID for PrimaryKeepaliveMessage messages.
	PrimaryKeepaliveMessageByteID = 'k'
	// StandbyStatusUpdateByteID is the byte ID for StandbyStatusUpdate messages.
	StandbyStatusUpdateByteID = 'r'
)

// LSN is a PostgreSQL Log Sequence Number. See https://www.postgresql.org/docs/current/datatype-pg-lsn.html.
type LSN uint64

// String formats the LSN value into the XXX/XXX format which is the text format used by PostgreSQL.
func (lsn LSN) String() string {
	return fmt.Sprintf("%08X/%08X", uint32(lsn>>32), uint32(lsn))
}

func (lsn *LSN) decodeText(src string) error {
	lsnValue, err := ParseLSN(src)
	if err != nil {
		return err
	}
	*lsn = lsnValue

	return nil
}

// Scan implements the Scanner interface.
func (lsn *LSN) Scan(src any) error {
	if lsn == nil {
		return nil
	}

	switch v := src.(type) {
	case uint64:
		*lsn = LSN(v)
	case string:
		if err := lsn.decodeText(v); err != nil {
			return err
		}
	case []byte:
		if err := lsn.decodeText(string(v)); err != nil {
			return err
		}
	default:
		return fmt.Errorf("can not scan %T to LSN", src)
	}

	return nil
}

// Value implements the Valuer interface.
func (lsn LSN) Value() (driver.Value, error) {
	return driver.Value(lsn.String()), nil
}

// ParseLSN parses the given XXX/XXX text format LSN used by PostgreSQL.
func ParseLSN(s string) (LSN, error) {
	var upperHalf uint64
	var lowerHalf uint64
	var nparsed int
	nparsed, err := fmt.Sscanf(s, "%X/%X", &upperHalf, &lowerHalf)
	if err != nil {
		return 0, fmt.Errorf("parsing LSN: %w", err)
	}

	if nparsed != 2 {
		return 0, fmt.Errorf("parsing LSN: %s", s)
	}

	return LSN((upperHalf << 32) + lowerHalf), nil
}

// IdentifySystemResult is the parsed result of the IDENTIFY_SYSTEM command.
type IdentifySystemResult struct {
	SystemID string
	Timeline int32
	XLogPos  LSN
	DBName   string
}

// IdentifySystem executes the IDENTIFY_SYSTEM command.
func IdentifySystem(ctx context.Context, conn *pgconn.PgConn) (IdentifySystemResult, error) {
	return ParseIdentifySystem(conn.Exec(ctx, "IDENTIFY_SYSTEM"))
}

// ParseIdentifySystem parses the result of the IDENTIFY_SYSTEM command.
func ParseIdentifySystem(mrr *pgconn.MultiResultReader) (IdentifySystemResult, error) {
	var isr IdentifySystemResult
	results, err := mrr.ReadAll()
	if err != nil {
		return isr, err
	}

	if len(results) != 1 {
		return isr, fmt.Errorf("expected 1 result set, got %d", len(results))
	}

	result := results[0]
	if len(result.Rows) != 1 {
		return isr, fmt.Errorf("expected 1 result row, got %d", len(result.Rows))
	}

	row := result.Rows[0]
	if len(row) != 4 {
		return isr, fmt.Errorf("expected 4 result columns, got %d", len(row))
	}

	isr.SystemID = string(row[0])
	timeline, err := strconv.ParseInt(string(row[1]), 10, 32)
	if err != nil {
		return isr, fmt.Errorf("parsing timeline: %w", err)
	}
	isr.Timeline = int32(timeline)

	isr.XLogPos, err = ParseLSN(string(row[2]))
	if err != nil {
		return isr, fmt.Errorf("parsing xlogpos as LSN: %w", err)
	}

	isr.DBName = string(row[3])

	return isr, nil
}

// TimelineHistoryResult is the parsed result of the TIMELINE_HISTORY command.
type TimelineHistoryResult struct {
	FileName string
	Content  []byte
}

// TimelineHistory executes the TIMELINE_HISTORY command.
func TimelineHistory(ctx context.Context, conn *pgconn.PgConn, timeline int32) (TimelineHistoryResult, error) {
	sql := fmt.Sprintf("TIMELINE_HISTORY %d", timeline)
	return ParseTimelineHistory(conn.Exec(ctx, sql))
}

// ParseTimelineHistory parses the result of the TIMELINE_HISTORY command.
func ParseTimelineHistory(mrr *pgconn.MultiResultReader) (TimelineHistoryResult, error) {
	var thr TimelineHistoryResult
	results, err := mrr.ReadAll()
	if err != nil {
		return thr, err
	}

	if len(results) != 1 {
		return thr, fmt.Errorf("expected 1 result set, got %d", len(results))
	}

	result := results[0]
	if len(result.Rows) != 1 {
		return thr, fmt.Errorf("expected 1 result row, got %d", len(result.Rows))
	}

	row := result.Rows[0]
	if len(row) != 2 {
		return thr, fmt.Errorf("expected 2 result columns, got %d", len(row))
	}

	thr.FileName = string(row[0])
	thr.Content = row[1]
	return thr, nil
}

// CreateReplicationSlotOptions are the options for the CREATE_REPLICATION_SLOT command.
type CreateReplicationSlotOptions struct {
	Temporary      bool
	SnapshotAction string
}

// CreateReplicationSlot creates a logical replication slot.
func CreateReplicationSlot(
	ctx context.Context,
	conn *pgconn.PgConn,
	slotName string,
	outputPlugin string,
	options CreateReplicationSlotOptions,
) (lsn LSN, snapshotName string, err error) {
	var temporaryString string
	if options.Temporary {
		temporaryString = "TEMPORARY"
	}
	// NOTE: All strings passed into here have been validated and are not prone to SQL injection.
	cmd := fmt.Sprintf("CREATE_REPLICATION_SLOT %s %s LOGICAL %s %s", slotName, temporaryString, outputPlugin, options.SnapshotAction)
	results, err := conn.Exec(ctx, cmd).ReadAll()
	if err != nil {
		return 0, "", err
	}
	if len(results) != 1 || len(results[0].Rows) != 1 || len(results[0].Rows[0]) != 4 {
		return 0, "", errors.New("unexpected result from CREATE_REPLICATION_SLOT")
	}
	lsn, err = ParseLSN(string(results[0].Rows[0][1]))
	if err != nil {
		return 0, "", fmt.Errorf("invalid lsn from CREATE_REPLICATION_SLOT: %w", err)
	}
	return lsn, string(results[0].Rows[0][2]), nil
}

// CopyReplicationSlot copies a replication slot, requires PG >= 12.
func CopyReplicationSlot(ctx context.Context, conn *pgconn.PgConn, oldSlot, newSlot string, temporary bool) (LSN, error) {
	cmd := fmt.Sprintf("select pg_copy_logical_replication_slot('%s', '%s', %v)", oldSlot, newSlot, temporary)
	results, err := conn.Exec(ctx, cmd).ReadAll()
	if err != nil {
		return 0, err
	}
	if len(results) != 1 || len(results[0].Rows) != 1 || len(results[0].Rows[0]) != 1 {
		return 0, errors.New("unexpected result from pg_copy_logical_replication_slot")
	}
	result := string(results[0].Rows[0][0])
	if !strings.HasPrefix(result, "(") || !strings.HasSuffix(result, ")") {
		return 0, fmt.Errorf("unexpected result from pg_copy_logical_replication_slot: %q", result)
	}
	result = result[1 : len(result)-1]
	result, ok := strings.CutPrefix(result, newSlot)
	if !ok {
		return 0, fmt.Errorf("unexpected slot name from pg_copy_logical_replication_slot: %q", result)
	}
	result, ok = strings.CutPrefix(result, ",")
	if !ok {
		return 0, fmt.Errorf("unexpected delimiter from pg_copy_logical_replication_slot: %q", result)
	}
	return ParseLSN(result)
}

// DropReplicationSlotOptions are options for the DROP_REPLICATION_SLOT command.
type DropReplicationSlotOptions struct {
	Wait bool
}

// DropReplicationSlot drops a logical replication slot.
func DropReplicationSlot(ctx context.Context, conn *pgconn.PgConn, slotName string, options DropReplicationSlotOptions) error {
	var waitString string
	if options.Wait {
		waitString = "WAIT"
	}
	sql := fmt.Sprintf("DROP_REPLICATION_SLOT %s %s", slotName, waitString)
	_, err := conn.Exec(ctx, sql).ReadAll()
	return err
}

// CreatePublication makes the publication with the given name publish exactly the given tables. An empty list means
// FOR ALL TABLES. Each case runs at most one statement, so a failure never leaves the publication half changed.
func CreatePublication(ctx context.Context, conn *pgconn.PgConn, publicationName string, tables []TableFQN) error {
	// puballtables must be read from pg_publication: pg_publication_tables
	// lists every table for a FOR ALL TABLES publication and nothing for a
	// FOR TABLE publication that has had all of its tables dropped, so the
	// table list alone cannot tell them apart. The options are read too, so
	// that a recreated publication keeps them. They go through to_jsonb
	// because the newer columns are missing on older servers, and naming a
	// missing column fails the whole query.
	pubQuery, err := sanitize.SQLQuery(`
			SELECT puballtables,
				to_jsonb(p) ->> 'pubinsert',
				to_jsonb(p) ->> 'pubupdate',
				to_jsonb(p) ->> 'pubdelete',
				to_jsonb(p) ->> 'pubtruncate',
				to_jsonb(p) ->> 'pubviaroot',
				to_jsonb(p) ->> 'pubgencols'
			FROM pg_publication p
			WHERE pubname = $1;
		`, publicationName)
	if err != nil {
		return fmt.Errorf("sanitizing publication query: %w", err)
	}
	rows, err := conn.Exec(ctx, pubQuery).ReadAll()
	if err != nil {
		return fmt.Errorf("checking publication existence: %w", err)
	}
	exists := len(rows) > 0 && len(rows[0].Rows) > 0
	// The simple query protocol returns booleans as text: "t" or "f".
	allTables := exists && string(rows[0].Rows[0][0]) == "t"

	tableNames := make([]string, 0, len(tables))
	for _, table := range tables {
		tableNames = append(tableNames, table.String())
	}
	// The table names are sanitized, so we can safely interpolate them into the query.
	tableClause := "TABLE " + strings.Join(tableNames, ", ")

	var stmt string
	switch {
	case !exists && len(tables) == 0:
		stmt = fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES;", publicationName)
	case !exists:
		stmt = fmt.Sprintf("CREATE PUBLICATION %s FOR %s;", publicationName, tableClause)
	case len(tables) == 0 && allTables:
		return nil
	case len(tables) == 0:
		// A FOR TABLE publication cannot be widened with ALTER PUBLICATION,
		// and dropping its members would only leave an empty publication that
		// streams nothing, so recreate it with the same options. Both
		// statements run in one implicit transaction, so the publication never
		// goes missing for a concurrent reader, and a failed CREATE leaves it
		// as it was. The WITH clause is built from fixed keywords only, so we
		// can safely interpolate it into the query.
		stmt = fmt.Sprintf("DROP PUBLICATION %s; CREATE PUBLICATION %s FOR ALL TABLES WITH (%s);", publicationName, publicationName, publicationWithClause(rows[0].Rows[0][1:]))
	default:
		pubTables, err := GetPublicationTables(ctx, conn, publicationName)
		if err != nil {
			return fmt.Errorf("getting publication tables: %w", err)
		}
		// Only the owner may run ALTER PUBLICATION, so skip it when nothing
		// changes. A publication that another role created ahead of time then
		// keeps working.
		if len(pubTables) == len(tables) && !slices.ContainsFunc(tables, func(table TableFQN) bool {
			return !slices.Contains(pubTables, table)
		}) {
			return nil
		}
		// SET TABLE replaces the member list and keeps the publication
		// options. It fails on a FOR ALL TABLES publication, because
		// PostgreSQL cannot narrow one.
		stmt = fmt.Sprintf("ALTER PUBLICATION %s SET %s;", publicationName, tableClause)
	}

	sq, err := sanitize.SQLQuery(stmt)
	if err != nil {
		return fmt.Errorf("sanitizing publication statement: %w", err)
	}
	if _, err := conn.Exec(ctx, sq).ReadAll(); err != nil {
		if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.Code == "42501" && exists && len(tables) == 0 {
			return fmt.Errorf("recreating publication %s FOR ALL TABLES needs a superuser: recreate it FOR ALL TABLES as a superuser, or list the tables to replicate in tables: %w", publicationName, err)
		}
		return fmt.Errorf("updating publication %s: %w", publicationName, err)
	}
	return nil
}

// publicationWithClause turns the pg_publication option columns read by
// CreatePublication (pubinsert, pubupdate, pubdelete, pubtruncate, pubviaroot,
// pubgencols, each NULL when the server predates it) into the body of a
// CREATE PUBLICATION ... WITH (...) clause that restores them.
func publicationWithClause(opts [][]byte) string {
	var publish []string
	for i, op := range []string{"insert", "update", "delete", "truncate"} {
		if string(opts[i]) == "true" {
			publish = append(publish, op)
		}
	}
	clause := fmt.Sprintf("publish = '%s'", strings.Join(publish, ", "))
	switch string(opts[4]) {
	case "true":
		clause += ", publish_via_partition_root = true"
	case "false":
		clause += ", publish_via_partition_root = false"
	}
	switch string(opts[5]) {
	case "n":
		clause += ", publish_generated_columns = none"
	case "s":
		clause += ", publish_generated_columns = stored"
	}
	return clause
}

// GetPublicationTables returns the tables currently in the publication. A FOR
// ALL TABLES publication lists every table, and a FOR TABLE publication with no
// members lists none: read pg_publication.puballtables to tell those apart.
func GetPublicationTables(ctx context.Context, conn *pgconn.PgConn, publicationName string) ([]TableFQN, error) {
	query, err := sanitize.SQLQuery(`
		SELECT DISTINCT
		tablename as table_name,
		schemaname as schema_name
		FROM pg_publication_tables
		WHERE pubname = $1
		ORDER BY schema_name, table_name;
	`, publicationName)
	if err != nil {
		return nil, fmt.Errorf("getting publication tables: %w", err)
	}

	// Get specific tables in the publication
	result := conn.Exec(ctx, query)

	rows, err := result.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("getting publication tables: %w", err)
	}

	if len(rows) == 0 || len(rows[0].Rows) == 0 {
		return nil, nil
	}

	tables := make([]TableFQN, 0, len(rows))
	for _, row := range rows[0].Rows {
		// These come from postgres so they are valid, but we have to quote them
		// to prevent normalization
		table := sanitize.QuotePostgresIdentifier(string(row[0]))
		schema := sanitize.QuotePostgresIdentifier(string(row[1]))
		tables = append(tables, TableFQN{Table: table, Schema: schema})
	}

	return tables, nil
}

// StartReplicationOptions are the options for the START_REPLICATION command.
// The Timeline field is optional and defaults to 0, which means the current server timeline.
// The Mode field is required and must be either PhysicalReplication or LogicalReplication. ## PhysicalReplication is not supporter by this plugin, but still can be implemented
// The PluginArgs field is optional and only used for LogicalReplication.
type StartReplicationOptions struct {
	PluginArgs []string
}

// StartReplication begins the replication process by executing the START_REPLICATION command.
func StartReplication(ctx context.Context, conn *pgconn.PgConn, slotName string, startLSN LSN, options StartReplicationOptions) error {
	sql := fmt.Sprintf("START_REPLICATION SLOT %s LOGICAL %s ", slotName, startLSN)
	if len(options.PluginArgs) > 0 {
		sql += fmt.Sprintf("(%s)", strings.Join(options.PluginArgs, ", "))
	}

	conn.Frontend().SendQuery(&pgproto3.Query{String: sql})
	err := conn.Frontend().Flush()
	if err != nil {
		return fmt.Errorf("sending START_REPLICATION: %w", err)
	}

	for {
		msg, err := conn.ReceiveMessage(ctx)
		if err != nil {
			return fmt.Errorf("receiving message: %w", err)
		}

		switch msg := msg.(type) {
		case *pgproto3.NoticeResponse:
		case *pgproto3.ErrorResponse:
			return pgconn.ErrorResponseToPgError(msg)
		case *pgproto3.CopyBothResponse:
			// This signals the start of the replication stream.
			return nil
		default:
			return fmt.Errorf("unexpected response type: %T", msg)
		}
	}
}

// PrimaryKeepaliveMessage is a message sent by the primary server to the replica server to keep the connection alive.
type PrimaryKeepaliveMessage struct {
	ServerWALEnd   LSN
	ServerTime     time.Time
	ReplyRequested bool
}

// ParsePrimaryKeepaliveMessage parses a Primary keepalive message from the server.
func ParsePrimaryKeepaliveMessage(buf []byte) (PrimaryKeepaliveMessage, error) {
	var pkm PrimaryKeepaliveMessage
	if len(buf) != 17 {
		return pkm, fmt.Errorf("PrimaryKeepaliveMessage must be 17 bytes, got %d", len(buf))
	}

	pkm.ServerWALEnd = LSN(binary.BigEndian.Uint64(buf))
	pkm.ServerTime = pgTimeToTime(int64(binary.BigEndian.Uint64(buf[8:])))
	pkm.ReplyRequested = buf[16] != 0

	return pkm, nil
}

// XLogData is a message sent by the primary server to the replica server containing WAL data.
type XLogData struct {
	WALStart     LSN
	ServerWALEnd LSN
	ServerTime   time.Time
	WALData      []byte
}

// ParseXLogData parses a XLogData message from the server.
func ParseXLogData(buf []byte) (XLogData, error) {
	var xld XLogData
	if len(buf) < 24 {
		return xld, fmt.Errorf("XLogData must be at least 24 bytes, got %d", len(buf))
	}

	xld.WALStart = LSN(binary.BigEndian.Uint64(buf))
	xld.ServerWALEnd = LSN(binary.BigEndian.Uint64(buf[8:]))
	xld.ServerTime = pgTimeToTime(int64(binary.BigEndian.Uint64(buf[16:])))
	xld.WALData = buf[24:]

	return xld, nil
}

// StandbyStatusUpdate is a message sent from the client that acknowledges receipt of WAL records.
type StandbyStatusUpdate struct {
	WALWritePosition LSN       // The WAL position that's been locally written
	WALFlushPosition LSN       // The WAL position that's been locally flushed
	WALApplyPosition LSN       // The WAL position that's been locally applied
	ClientTime       time.Time // Client system clock time
	ReplyRequested   bool      // Request server to reply immediately.
}

// SendStandbyStatusUpdate sends a StandbyStatusUpdate to the PostgreSQL server.
//
// The only required field in ssu is WALWritePosition. If WALFlushPosition is 0 then WALWritePosition will be assigned
// to it. If WALApplyPosition is 0 then WALWritePosition will be assigned to it. If ClientTime is the zero value then
// the current time will be assigned to it.
func SendStandbyStatusUpdate(_ context.Context, conn *pgconn.PgConn, ssu StandbyStatusUpdate) error {
	if ssu.WALFlushPosition == 0 {
		ssu.WALFlushPosition = ssu.WALWritePosition
	}
	if ssu.WALApplyPosition == 0 {
		ssu.WALApplyPosition = ssu.WALWritePosition
	}
	if ssu.ClientTime.IsZero() {
		ssu.ClientTime = time.Now()
	}

	data := make([]byte, 0, 34)
	data = append(data, StandbyStatusUpdateByteID)
	data = pgio.AppendUint64(data, uint64(ssu.WALWritePosition))
	data = pgio.AppendUint64(data, uint64(ssu.WALFlushPosition))
	data = pgio.AppendUint64(data, uint64(ssu.WALApplyPosition))
	data = pgio.AppendInt64(data, timeToPgTime(ssu.ClientTime))
	if ssu.ReplyRequested {
		data = append(data, 1)
	} else {
		data = append(data, 0)
	}

	cd := &pgproto3.CopyData{Data: data}
	buf, err := cd.Encode(nil)
	if err != nil {
		return err
	}

	return conn.Frontend().SendUnbufferedEncodedCopyData(buf)
}

// CopyDoneResult is the parsed result as returned by the server after the client
// sends a CopyDone to the server to confirm ending the copy-both mode.
type CopyDoneResult struct {
	Timeline int32
	LSN      LSN
}

// SendStandbyCopyDone sends a StandbyCopyDone to the PostgreSQL server
// to confirm ending the copy-both mode.
func SendStandbyCopyDone(_ context.Context, conn *pgconn.PgConn) (cdr *CopyDoneResult, err error) {
	// I am suspicious that this is wildly wrong, but I'm pretty sure the previous
	// code was wildly wrong too -- wttw <steve@blighty.com>
	conn.Frontend().Send(&pgproto3.CopyDone{})
	err = conn.Frontend().Flush()
	if err != nil {
		return cdr, err
	}

	for {
		var msg pgproto3.BackendMessage
		msg, err = conn.Frontend().Receive()
		if err != nil {
			return cdr, err
		}

		switch m := msg.(type) {
		case *pgproto3.CopyDone:
		case *pgproto3.ParameterStatus, *pgproto3.NoticeResponse:
		case *pgproto3.CommandComplete:
		case *pgproto3.RowDescription:
		case *pgproto3.DataRow:
			// We are expecting just one row returned, with two columns timeline and LSN
			// We should pay attention to RowDescription, but we'll take it on trust.
			if len(m.Values) == 2 {
				timeline, lerr := strconv.Atoi(string(m.Values[0]))
				if lerr == nil {
					lsn, lerr := ParseLSN(string(m.Values[1]))
					if lerr == nil {
						cdr = new(CopyDoneResult)
						cdr.Timeline = int32(timeline)
						cdr.LSN = lsn
					}
				}
			}
		case *pgproto3.EmptyQueryResponse:
		case *pgproto3.ErrorResponse:
			return cdr, pgconn.ErrorResponseToPgError(m)
		case *pgproto3.ReadyForQuery:
			// Should we eat the ReadyForQuery here, or not?
			return cdr, err
		}
	}
}

const microsecFromUnixEpochToY2K = 946684800 * 1000000

func pgTimeToTime(microsecSinceY2K int64) time.Time {
	microsecSinceUnixEpoch := microsecFromUnixEpochToY2K + microsecSinceY2K
	return time.Unix(0, microsecSinceUnixEpoch*1000)
}

func timeToPgTime(t time.Time) int64 {
	microsecSinceUnixEpoch := t.Unix()*1000000 + int64(t.Nanosecond())/1000
	return microsecSinceUnixEpoch - microsecFromUnixEpochToY2K
}
