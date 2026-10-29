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

// publicationInfo describes an existing publication.
type publicationInfo struct {
	exists bool
	// allTables is pg_publication.puballtables. It must be read from there:
	// pg_publication_tables lists every table for a FOR ALL TABLES
	// publication, and nothing for a FOR TABLE publication that has had all of
	// its tables dropped, so the table list alone cannot tell them apart.
	allTables bool
}

func getPublication(ctx context.Context, conn *pgconn.PgConn, publicationName string) (publicationInfo, error) {
	pubQuery, err := sanitize.SQLQuery(`
			SELECT puballtables
			FROM pg_publication
			WHERE pubname = $1;
		`, publicationName)
	if err != nil {
		return publicationInfo{}, fmt.Errorf("sanitizing publication query: %w", err)
	}

	rows, err := conn.Exec(ctx, pubQuery).ReadAll()
	if err != nil {
		return publicationInfo{}, fmt.Errorf("checking publication existence: %w", err)
	}
	if len(rows) == 0 || len(rows[0].Rows) == 0 {
		return publicationInfo{}, nil
	}
	row := rows[0].Rows[0]
	return publicationInfo{
		exists:    true,
		allTables: string(row[0]) == "t",
	}, nil
}

// errAllTablesPublication is returned when the publication is FOR ALL TABLES
// but the input is configured with a table list.
var errAllTablesPublication = errors.New("publication is FOR ALL TABLES")

// publicationSyncPlan returns the tables to add to and drop from an existing
// publication so that it lists exactly tables. A FOR ALL TABLES publication
// already covers every table when tables is empty, so it is left alone. With a
// table list it cannot be narrowed, since PostgreSQL rejects ADD TABLE and DROP
// TABLE on it, and keeping it would stream every table in the database, so an
// error is returned instead.
func publicationSyncPlan(publicationName string, pub publicationInfo, pubTables, tables []TableFQN) (add, drop []TableFQN, err error) {
	if pub.allTables {
		if len(tables) > 0 {
			return nil, nil, fmt.Errorf("%w: %s replicates every table and cannot be narrowed to the configured tables: drop it so it can be recreated for the configured tables, or use a different publication", errAllTablesPublication, publicationName)
		}
		return nil, nil, nil
	}
	for _, table := range tables {
		if !slices.Contains(pubTables, table) {
			add = append(add, table)
		}
	}
	for _, table := range pubTables {
		if !slices.Contains(tables, table) {
			drop = append(drop, table)
		}
	}
	return add, drop, nil
}

// CreatePublication creates a new PostgreSQL publication with the given name for a list of tables, or alters an
// existing one so that it lists exactly those tables. An empty list creates a FOR ALL TABLES publication.
func CreatePublication(ctx context.Context, conn *pgconn.PgConn, publicationName string, tables []TableFQN) error {
	pub, err := getPublication(ctx, conn, publicationName)
	if err != nil {
		return err
	}

	tablesClause := "FOR ALL TABLES"
	if len(tables) > 0 {
		var sb strings.Builder
		sb.WriteString("FOR TABLE ")
		for i, table := range tables {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(table.String())
		}
		tablesClause = sb.String()
	}

	if !pub.exists {
		// tablesClause is sanitized, so we can safely interpolate it into the query
		sq, err := sanitize.SQLQuery(fmt.Sprintf("CREATE PUBLICATION %s %s;", publicationName, tablesClause))
		if err != nil {
			return fmt.Errorf("sanitizing publication creation query: %w", err)
		}
		// Publication doesn't exist, create new one
		if _, err := conn.Exec(ctx, sq).ReadAll(); err != nil {
			return fmt.Errorf("creating publication: %w", err)
		}

		return nil
	}

	// Diffed against the tables the publication lists itself, not
	// pg_publication_tables: without publish_via_partition_root that view
	// lists a partitioned table's partitions instead of the table, so it
	// would plan to drop partitions that are not members and to add the
	// table that already is.
	var pubTables []TableFQN
	if !pub.allTables {
		if pubTables, err = getPublicationMembers(ctx, conn, publicationName); err != nil {
			return fmt.Errorf("getting publication tables: %w", err)
		}
	}

	tablesToAddToPublication, tablesToRemoveFromPublication, err := publicationSyncPlan(publicationName, pub, pubTables, tables)
	if err != nil {
		return err
	}

	// remove tables from publication
	for _, dropTable := range tablesToRemoveFromPublication {
		sq, err := sanitize.SQLQuery(fmt.Sprintf(`ALTER PUBLICATION %s DROP TABLE %s;`, publicationName, dropTable.String()))
		if err != nil {
			return fmt.Errorf("sanitizing drop table query: %w", err)
		}
		if _, err := conn.Exec(ctx, sq).ReadAll(); err != nil {
			return fmt.Errorf("removing table from publication: %w", err)
		}
	}

	// add tables to publication
	for _, addTable := range tablesToAddToPublication {
		sq, err := sanitize.SQLQuery(fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s;", publicationName, addTable.String()))
		if err != nil {
			return fmt.Errorf("sanitizing add table query: %w", err)
		}
		if _, err := conn.Exec(ctx, sq).ReadAll(); err != nil {
			return fmt.Errorf("adding table to publication: %w", err)
		}
	}

	return nil
}

// getPublicationMembers returns the tables a publication lists itself, from
// pg_publication_rel. Unlike GetPublicationTables, a partitioned table is
// returned as itself rather than as its partitions.
func getPublicationMembers(ctx context.Context, conn *pgconn.PgConn, publicationName string) ([]TableFQN, error) {
	query, err := sanitize.SQLQuery(`
		SELECT c.relname, n.nspname
		FROM   pg_publication_rel pr
		JOIN   pg_class c ON c.oid = pr.prrelid
		JOIN   pg_namespace n ON n.oid = c.relnamespace
		WHERE  pr.prpubid = (SELECT oid FROM pg_publication WHERE pubname = $1)
		ORDER BY n.nspname, c.relname;
	`, publicationName)
	if err != nil {
		return nil, fmt.Errorf("sanitizing publication members query: %w", err)
	}
	rows, err := conn.Exec(ctx, query).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("getting publication members: %w", err)
	}
	var tables []TableFQN
	if len(rows) > 0 {
		for _, row := range rows[0].Rows {
			tables = append(tables, TableFQN{
				Table:  sanitize.QuotePostgresIdentifier(string(row[0])),
				Schema: sanitize.QuotePostgresIdentifier(string(row[1])),
			})
		}
	}
	return tables, nil
}

// publicationRowFilterExpr and publicationColumnListExpr read the row filter
// and column list of a pg_publication_tables row aliased pt. They go through
// to_jsonb because those columns only exist from PostgreSQL 15, and naming
// them directly would fail on earlier versions; there they read NULL, which
// is right, since earlier versions have neither feature.
const (
	publicationRowFilterExpr  = `to_jsonb(pt) ->> 'rowfilter'`
	publicationColumnListExpr = `to_jsonb(pt) -> 'attnames'`
)

// publicationFilteredExpr is true for a published row p whose row filter is
// set, or whose column list leaves out a column of its relation p.oid. With
// no column list PostgreSQL reports every column, and generated columns are
// left out of the comparison since they are only published on request.
//
// The column list comparison is only built when columnLists is set: it names
// pg_attribute.attgenerated, which PostgreSQL 10 and 11 lack, and a query
// naming a missing column fails to parse even when that branch never runs.
// Column lists only exist from PostgreSQL 15, so before that there is nothing
// to compare.
func publicationFilteredExpr(columnLists bool) string {
	if !columnLists {
		return `(p.rowfilter IS NOT NULL)`
	}
	return `(p.rowfilter IS NOT NULL
               OR (p.attnames IS NOT NULL AND EXISTS (
                   SELECT 1
                   FROM   pg_attribute a
                   WHERE  a.attrelid = p.oid
                   AND    a.attnum > 0
                   AND    NOT a.attisdropped
                   AND    a.attgenerated = ''
                   AND    NOT EXISTS (SELECT 1 FROM jsonb_array_elements_text(p.attnames) e WHERE e.value = a.attname)
               )))`
}

// publishedAsItselfQuery reports whether a publication streams a table's
// changes under the table's own name, and whether it streams them with a row
// filter or a column list. pg_publication_tables lists each table under the
// name its changes are published with: a partitioned table's partitions
// unless publish_via_partition_root is set, and the topmost published
// ancestor when it is. So a partitioned table published through its
// partitions, and a partition published through its root, are not listed as
// themselves. pgVersion is the server's major version, which decides whether
// column lists are compared.
func publishedAsItselfQuery(pgVersion int, publication, schema, table, fqn string) (string, error) {
	return sanitize.SQLQuery(`
        WITH published AS (
            SELECT to_regclass($4)::oid AS oid,
                   `+publicationRowFilterExpr+` AS rowfilter,
                   `+publicationColumnListExpr+` AS attnames
            FROM   pg_publication_tables pt
            WHERE  pt.pubname = $1
            AND    pt.schemaname = $2
            AND    pt.tablename = $3
        )
        SELECT EXISTS (SELECT 1 FROM published),
               EXISTS (SELECT 1 FROM published p WHERE `+publicationFilteredExpr(pgVersion >= 15)+`);
    `, publication, schema, table, fqn)
}

// GetPublicationTables returns the tables a publication replicates, and
// whether it is FOR ALL TABLES. For a FOR ALL TABLES publication the list is
// every table in the database.
// Arguments, in order: list of the tables, exist for all tables, error.
func GetPublicationTables(ctx context.Context, conn *pgconn.PgConn, publicationName string) ([]TableFQN, bool, error) {
	pub, err := getPublication(ctx, conn, publicationName)
	if err != nil {
		return nil, false, err
	}
	tables, err := publicationTables(ctx, conn, publicationName)
	if err != nil {
		return nil, false, err
	}
	return tables, pub.allTables, nil
}

// publicationTables returns the tables pg_publication_tables lists for a
// publication, without reading the publication itself.
func publicationTables(ctx context.Context, conn *pgconn.PgConn, publicationName string) ([]TableFQN, error) {
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
	rows, err := conn.Exec(ctx, query).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("getting publication tables: %w", err)
	}

	var tables []TableFQN
	if len(rows) > 0 {
		tables = make([]TableFQN, 0, len(rows[0].Rows))
		for _, row := range rows[0].Rows {
			// These come from postgres so they are valid, but we have to quote them
			// to prevent normalization
			table := sanitize.QuotePostgresIdentifier(string(row[0]))
			schema := sanitize.QuotePostgresIdentifier(string(row[1]))
			tables = append(tables, TableFQN{Table: table, Schema: schema})
		}
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
