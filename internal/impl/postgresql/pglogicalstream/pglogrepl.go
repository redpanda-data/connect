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

// ReplicationMode is the mode of replication to use.
type ReplicationMode int

const (
	// LogicalReplication is the only replication mode supported by this plugin
	LogicalReplication ReplicationMode = iota
)

// String formats the mode into a postgres valid string
func (mode ReplicationMode) String() string {
	if mode == LogicalReplication {
		return "LOGICAL"
	} else {
		return "PHYSICAL"
	}
}

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
func (lsn *LSN) Scan(src interface{}) error {
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
		return 0, fmt.Errorf("failed to parse LSN: %w", err)
	}

	if nparsed != 2 {
		return 0, fmt.Errorf("failed to parsed LSN: %s", s)
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
		return isr, fmt.Errorf("failed to parse timeline: %w", err)
	}
	isr.Timeline = int32(timeline)

	isr.XLogPos, err = ParseLSN(string(row[2]))
	if err != nil {
		return isr, fmt.Errorf("failed to parse xlogpos as LSN: %w", err)
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

// CreateReplicationSlotOptions are the options for the CREATE_REPLICATION_SLOT command. Including Mode, Temporary, and SnapshotAction.
type CreateReplicationSlotOptions struct {
	Temporary      bool
	SnapshotAction string
	Mode           ReplicationMode
}

// CreateReplicationSlotResult is the parsed results the CREATE_REPLICATION_SLOT command.
type CreateReplicationSlotResult struct {
	SlotName        string
	ConsistentPoint string
	SnapshotName    string
	OutputPlugin    string
}

// CreateReplicationSlot creates a logical replication slot.
func CreateReplicationSlot(
	ctx context.Context,
	conn *pgconn.PgConn,
	slotName string,
	outputPlugin string,
	options CreateReplicationSlotOptions,
	version int,
	snapshotter *Snapshotter,
) (CreateReplicationSlotResult, error) {
	var temporaryString string
	if options.Temporary {
		temporaryString = "TEMPORARY"
	}
	var snapshotString string
	if options.SnapshotAction == "export" {
		snapshotString = "(SNAPSHOT export)"
	} else {
		snapshotString = options.SnapshotAction
	}

	// NOTE: All strings passed into here have been validated and are not prone to SQL injection.
	newPgCreateSlotCommand := fmt.Sprintf("CREATE_REPLICATION_SLOT %s %s %s %s %s", slotName, temporaryString, options.Mode, outputPlugin, snapshotString)
	oldPgCreateSlotCommand := fmt.Sprintf("SELECT * FROM pg_create_logical_replication_slot('%s', '%s', %v);", slotName, outputPlugin, temporaryString == "TEMPORARY")

	var snapshotName string
	if version > 14 {
		result, err := ParseCreateReplicationSlot(conn.Exec(ctx, newPgCreateSlotCommand), version, snapshotName)
		if err != nil {
			return CreateReplicationSlotResult{}, err
		}
		if snapshotter != nil {
			snapshotter.setTransactionSnapshotName(result.SnapshotName)
		}

		return result, nil
	}

	var snapshotResponse SnapshotCreationResponse
	if options.SnapshotAction == "export" {
		var err error
		snapshotResponse, err = snapshotter.initSnapshotTransaction()
		if err != nil {
			return CreateReplicationSlotResult{}, err
		}
		snapshotter.setTransactionSnapshotName(snapshotResponse.ExportedSnapshotName)
	}

	replicationSlotCreationResponse := conn.Exec(ctx, oldPgCreateSlotCommand)
	_, err := replicationSlotCreationResponse.ReadAll()
	if err != nil {
		return CreateReplicationSlotResult{}, err
	}

	return CreateReplicationSlotResult{
		SnapshotName: snapshotResponse.ExportedSnapshotName,
	}, nil
}

// ParseCreateReplicationSlot parses the result of the CREATE_REPLICATION_SLOT command.
func ParseCreateReplicationSlot(mrr *pgconn.MultiResultReader, version int, snapshotName string) (CreateReplicationSlotResult, error) {
	var crsr CreateReplicationSlotResult
	results, err := mrr.ReadAll()
	if err != nil {
		return crsr, err
	}

	if len(results) != 1 {
		return crsr, fmt.Errorf("expected 1 result set, got %d", len(results))
	}

	result := results[0]
	if len(result.Rows) != 1 {
		return crsr, fmt.Errorf("expected 1 result row, got %d", len(result.Rows))
	}

	row := result.Rows[0]
	if version > 14 {
		if len(row) != 4 {
			return crsr, fmt.Errorf("expected 4 result columns, got %d", len(row))
		}
	}

	crsr.SlotName = string(row[0])
	crsr.ConsistentPoint = string(row[1])

	if version > 14 {
		crsr.SnapshotName = string(row[2])
	} else {
		crsr.SnapshotName = snapshotName
	}

	return crsr, nil
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

// CreatePublication creates a new PostgreSQL publication with the given name for a list of tables and drop if exists flag
func CreatePublication(ctx context.Context, conn *pgconn.PgConn, publicationName string, tables []TableFQN) error {
	// Check if publication exists
	pubQuery, err := sanitize.SQLQuery(`
			SELECT pubname, puballtables
			FROM pg_publication
			WHERE pubname = $1;
		`, publicationName)
	if err != nil {
		return fmt.Errorf("failed to sanitize publication query: %w", err)
	}

	result := conn.Exec(ctx, pubQuery)

	rows, err := result.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to check publication existence: %w", err)
	}

	tablesClause := "FOR ALL TABLES"
	if len(tables) > 0 {
		tablesClause = "FOR TABLE "
		for i, table := range tables {
			if i > 0 {
				tablesClause += ", "
			}
			tablesClause += table.String()
		}
	}

	if len(rows) == 0 || len(rows[0].Rows) == 0 {
		// tablesClause is sanitized, so we can safely interpolate it into the query
		sq, err := sanitize.SQLQuery(fmt.Sprintf("CREATE PUBLICATION %s %s;", publicationName, tablesClause))
		if err != nil {
			return fmt.Errorf("failed to sanitize publication creation query: %w", err)
		}
		// Publication doesn't exist, create new one
		result = conn.Exec(ctx, sq)
		if _, err := result.ReadAll(); err != nil {
			return fmt.Errorf("failed to create publication: %w", err)
		}

		return nil
	}

	// assuming publication already exists
	// get a list of tables in the publication
	pubTables, forAllTables, err := GetPublicationTables(ctx, conn, publicationName)
	if err != nil {
		return fmt.Errorf("failed to get publication tables: %w", err)
	}

	// list of tables to publish is empty and publication is for all tables
	// no update is needed
	if forAllTables && len(pubTables) == 0 {
		return nil
	}

	var tablesToRemoveFromPublication = []TableFQN{}
	var tablesToAddToPublication = []TableFQN{}
	for _, table := range tables {
		if !slices.Contains(pubTables, table) {
			tablesToAddToPublication = append(tablesToAddToPublication, table)
		}
	}

	for _, table := range pubTables {
		if !slices.Contains(tables, table) {
			tablesToRemoveFromPublication = append(tablesToRemoveFromPublication, table)
		}
	}

	// remove tables from publication
	for _, dropTable := range tablesToRemoveFromPublication {
		sq, err := sanitize.SQLQuery(fmt.Sprintf(`ALTER PUBLICATION %s DROP TABLE %s;`, publicationName, dropTable.String()))
		if err != nil {
			return fmt.Errorf("failed to sanitize drop table query: %w", err)
		}
		result = conn.Exec(ctx, sq)
		if _, err := result.ReadAll(); err != nil {
			return fmt.Errorf("failed to remove table from publication: %w", err)
		}
	}

	// add tables to publication
	for _, addTable := range tablesToAddToPublication {
		sq, err := sanitize.SQLQuery(fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s;", publicationName, addTable.String()))
		if err != nil {
			return fmt.Errorf("failed to sanitize add table query: %w", err)
		}
		result = conn.Exec(ctx, sq)
		if _, err := result.ReadAll(); err != nil {
			return fmt.Errorf("failed to add table to publication: %w", err)
		}
	}

	return nil
}

// GetPublicationTables returns a list of tables currently in the publication
// Arguments, in order: list of the tables, exist for all tables, errror
func GetPublicationTables(ctx context.Context, conn *pgconn.PgConn, publicationName string) ([]TableFQN, bool, error) {
	query, err := sanitize.SQLQuery(`
		SELECT DISTINCT
			tablename as table_name,
      schemaname as schema_name
		FROM pg_publication_tables
		WHERE pubname = $1
		ORDER BY schema_name, table_name;
	`, publicationName)
	if err != nil {
		return nil, false, fmt.Errorf("failed to get publication tables: %w", err)
	}

	// Get specific tables in the publication
	result := conn.Exec(ctx, query)

	rows, err := result.ReadAll()
	if err != nil {
		return nil, false, fmt.Errorf("failed to get publication tables: %w", err)
	}

	if len(rows) == 0 || len(rows[0].Rows) == 0 {
		return nil, true, nil // Publication exists and is for all tables
	}

	tables := make([]TableFQN, 0, len(rows))
	for _, row := range rows[0].Rows {
		// These come from postgres so they are valid, but we have to quote them
		// to prevent normalization
		table := sanitize.QuotePostgresIdentifier(string(row[0]))
		schema := sanitize.QuotePostgresIdentifier(string(row[1]))
		tables = append(tables, TableFQN{Table: table, Schema: schema})
	}

	return tables, false, nil
}

// StartReplicationOptions are the options for the START_REPLICATION command.
// The Timeline field is optional and defaults to 0, which means the current server timeline.
// The Mode field is required and must be either PhysicalReplication or LogicalReplication. ## PhysicalReplication is not supporter by this plugin, but still can be implemented
// The PluginArgs field is optional and only used for LogicalReplication.
type StartReplicationOptions struct {
	Timeline   int32 // 0 means current server timeline
	Mode       ReplicationMode
	PluginArgs []string
}

// StartReplication begins the replication process by executing the START_REPLICATION command.
func StartReplication(ctx context.Context, conn *pgconn.PgConn, slotName string, startLSN LSN, options StartReplicationOptions) error {
	var timelineString string
	if options.Timeline > 0 {
		timelineString = fmt.Sprintf("TIMELINE %d", options.Timeline)
		options.PluginArgs = append(options.PluginArgs, timelineString)
	}

	sql := fmt.Sprintf("START_REPLICATION SLOT %s %s %s ", slotName, options.Mode, startLSN)
	if options.Mode == LogicalReplication {
		if len(options.PluginArgs) > 0 {
			sql += fmt.Sprintf("(%s)", strings.Join(options.PluginArgs, ", "))
		}
	} else {
		sql += timelineString
	}

	conn.Frontend().SendQuery(&pgproto3.Query{String: sql})
	err := conn.Frontend().Flush()
	if err != nil {
		return fmt.Errorf("failed to send START_REPLICATION: %w", err)
	}

	for {
		msg, err := conn.ReceiveMessage(ctx)
		if err != nil {
			return fmt.Errorf("failed to receive message: %w", err)
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
	if ssu.ClientTime == (time.Time{}) {
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
		return
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
