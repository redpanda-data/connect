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
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/asyncroutine"
	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream/sanitize"
)

// discoverer periodically polls the configured schema for new tables and adds
// them to the live replication set without restarting the main replication
// slot.
//
// Per discovery tick, for each newly-found table T:
//  1. ALTER PUBLICATION ... ADD TABLE T              (LSN advances past L_add)
//  2. CREATE_REPLICATION_SLOT tmp_T ... EXPORT_SNAPSHOT
//     -> returns (L_snap, snapshot_name)
//  3. snapshot T at snapshot_name (single-worker sequential scan)
//  4. record tableWatermark[T] = L_snap on the parent stream
//  5. DROP_REPLICATION_SLOT tmp_T
//  6. register T with the monitor
//  7. add T to the parent stream's live tables set
//
// WAL events for T with LSN <= L_snap are suppressed by the parent stream's
// processChange, since the snapshot at L_snap already contains them. Events
// with LSN > L_snap flow through normally.
type discoverer struct {
	stream  *Stream
	config  *Config
	pubName string
	logger  *service.Logger

	// pollConn is a regular SQL connection used for: polling pg_tables,
	// running ALTER PUBLICATION, and querying primary key info before
	// snapshotting. ALTER PUBLICATION cannot run on a replication-mode
	// connection on all PG versions, so we keep this separate from the
	// replication conn used for slot creation.
	pollConn *sql.DB

	loop *asyncroutine.Periodic
}

func newDiscoverer(ctx context.Context, stream *Stream, config *Config, pubName string) (*discoverer, error) {
	conn, err := openPgConnectionFromConfig(config)
	if err != nil {
		return nil, fmt.Errorf("opening discovery connection: %w", err)
	}
	if err := conn.PingContext(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("pinging discovery connection: %w", err)
	}
	d := &discoverer{
		stream:   stream,
		config:   config,
		pubName:  pubName,
		logger:   config.Logger,
		pollConn: conn,
	}
	d.loop = asyncroutine.NewPeriodicWithContext(config.DiscoveryInterval, d.tick)
	return d, nil
}

// Start begins the discovery polling loop.
func (d *discoverer) Start() {
	d.loop.Start()
}

// Stop halts the discovery loop and closes the connection.
func (d *discoverer) Stop() error {
	d.loop.Stop()
	return d.pollConn.Close()
}

func (d *discoverer) tick(ctx context.Context) {
	// One discovery cycle. Logs and continues on per-table errors so that a
	// single bad table doesn't stall discovery for the rest of the schema.
	candidates, err := d.listSchemaTables(ctx)
	if err != nil {
		d.logger.Warnf("table discovery: listing schema tables: %v", err)
		return
	}
	known := d.stream.knownTableNames()
	for _, t := range candidates {
		if ctx.Err() != nil {
			return
		}
		if _, seen := known[t.String()]; seen {
			continue
		}
		if err := d.addTable(ctx, t); err != nil {
			d.logger.Warnf("table discovery: adding %s: %v", t, err)
			continue
		}
		d.logger.Infof("table discovery: added %s", t)
	}
}

// listSchemaTables returns all base tables in the configured schema, with
// schema and table name normalized to TableFQN form (quoted as needed).
func (d *discoverer) listSchemaTables(ctx context.Context) ([]TableFQN, error) {
	unquotedSchema, err := sanitize.UnquotePostgresIdentifier(d.stream.schema)
	if err != nil {
		return nil, fmt.Errorf("unquoting schema: %w", err)
	}
	rows, err := d.pollConn.QueryContext(ctx, `
		SELECT tablename
		FROM pg_tables
		WHERE schemaname = $1
		ORDER BY tablename
	`, unquotedSchema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []TableFQN
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		normalized, err := sanitize.NormalizePostgresIdentifier(name)
		if err != nil {
			d.logger.Warnf("table discovery: skipping table with invalid identifier %q: %v", name, err)
			continue
		}
		out = append(out, TableFQN{Schema: d.stream.schema, Table: normalized})
	}
	return out, rows.Err()
}

// addTable runs the full discovery protocol for a single table.
func (d *discoverer) addTable(ctx context.Context, table TableFQN) error {
	// Step 1: ALTER PUBLICATION ADD TABLE.
	if err := d.alterPublicationAddTable(ctx, table); err != nil {
		// undefined_table = 42P01: table was dropped between listSchemaTables
		// and now. Treat as benign; next tick will skip it.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42P01" {
			return nil
		}
		return fmt.Errorf("ALTER PUBLICATION ADD TABLE: %w", err)
	}

	// Step 2: open a replication connection and create a temporary slot
	// with EXPORT_SNAPSHOT to anchor the snapshot at a known LSN.
	repConn, err := pgconn.ConnectConfig(ctx, d.config.DBConfig.Copy())
	if err != nil {
		return fmt.Errorf("opening replication connection: %w", err)
	}
	defer func() {
		if cerr := repConn.Close(ctx); cerr != nil {
			d.logger.Warnf("table discovery: closing replication conn for %s: %v", table, cerr)
		}
	}()

	tmpSlot, err := tempSlotName(d.stream.slotName)
	if err != nil {
		return fmt.Errorf("generating temp slot name: %w", err)
	}
	snapLSN, snapName, err := CreateReplicationSlot(
		ctx,
		repConn,
		tmpSlot,
		decodingPlugin,
		CreateReplicationSlotOptions{Temporary: true, SnapshotAction: "EXPORT_SNAPSHOT"},
	)
	if err != nil {
		return fmt.Errorf("creating temporary slot: %w", err)
	}
	// Slot is temporary and tied to repConn; closing repConn drops it. We
	// also call DROP explicitly to release sooner where supported.
	defer func() {
		if dropErr := DropReplicationSlot(ctx, repConn, tmpSlot, DropReplicationSlotOptions{Wait: false}); dropErr != nil {
			// Temp slots auto-drop on conn close; log at debug only.
			d.logger.Debugf("table discovery: dropping temp slot %s: %v", tmpSlot, dropErr)
		}
	}()

	// Step 3: snapshot T at the exported snapshot.
	if err := d.snapshotTableAt(ctx, table, snapName); err != nil {
		return fmt.Errorf("snapshotting %s: %w", table, err)
	}

	// Steps 4-7: register watermark, monitor entry, live table list. Order
	// matters: the watermark must be set BEFORE the table appears in the
	// known set, otherwise a WAL event for T arriving between knownTableNames
	// being read by processChange and the watermark being installed would
	// emit a duplicate.
	d.stream.installDiscoveredTable(table, snapLSN)
	if err := d.stream.monitor.RegisterTable(ctx, table); err != nil {
		// Non-fatal: progress reporting will be incomplete for T but data
		// flow is unaffected.
		d.logger.Warnf("table discovery: registering %s with monitor: %v", table, err)
	}
	// Snapshot is complete by the time we get here.
	d.stream.monitor.MarkSnapshotComplete(table)
	return nil
}

func (d *discoverer) alterPublicationAddTable(ctx context.Context, table TableFQN) error {
	// pubName and table.String() are both pre-validated identifiers, so
	// fmt.Sprintf is safe here. We still go through sanitize.SQLQuery as a
	// belt-and-braces check.
	q, err := sanitize.SQLQuery(fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s", d.pubName, table.String()))
	if err != nil {
		return fmt.Errorf("sanitizing ALTER PUBLICATION: %w", err)
	}
	if _, err := d.pollConn.ExecContext(ctx, q); err != nil {
		// "relation already in publication" is benign — happens when a crash
		// left the publication ahead of our local known-tables set.
		if strings.Contains(err.Error(), "is already member of publication") {
			return nil
		}
		return err
	}
	return nil
}

// snapshotTableAt snapshots a single table using the given exported snapshot
// name. It opens its own snapshotter scoped to this snapshot, runs a
// sequential scan, and emits messages on the parent stream's message channel.
func (d *discoverer) snapshotTableAt(ctx context.Context, table TableFQN, snapshotName string) error {
	// Use a single reader for incrementally-discovered tables. The
	// keyspace-splitting parallelism in the initial snapshotter is mostly
	// useful for very large tables on cold start; new tables are typically
	// small and a single reader keeps the implementation simple and
	// predictable.
	snap, err := newSnapshotter(d.config, d.config.DBRawDSN, d.logger, snapshotName, 1)
	if err != nil {
		return fmt.Errorf("opening snapshotter: %w", err)
	}
	defer func() {
		if err := snap.closeConn(); err != nil {
			d.logger.Warnf("table discovery: closing snapshotter for %s: %v", table, err)
		}
	}()
	if err := snap.Prepare(ctx); err != nil {
		return fmt.Errorf("preparing snapshotter: %w", err)
	}

	pkColumns, err := d.stream.getPrimaryKeyColumn(ctx, table)
	if err != nil {
		// Tables without a primary key cannot be snapshotted by the existing
		// scanner. Roll back the publication change so streaming for T does
		// not silently emit WAL events for a table whose initial state we
		// never captured.
		d.logger.Warnf("table discovery: skipping %s (no primary key); rolling back publication change", table)
		if rbErr := d.alterPublicationDropTable(ctx, table); rbErr != nil {
			d.logger.Warnf("table discovery: rolling back %s from publication: %v", table, rbErr)
		}
		return fmt.Errorf("getting primary key for %s: %w", table, err)
	}
	return d.stream.scanTableRange(ctx, snap, table, nil, nil, pkColumns)
}

func (d *discoverer) alterPublicationDropTable(ctx context.Context, table TableFQN) error {
	q, err := sanitize.SQLQuery(fmt.Sprintf("ALTER PUBLICATION %s DROP TABLE %s", d.pubName, table.String()))
	if err != nil {
		return err
	}
	_, err = d.pollConn.ExecContext(ctx, q)
	return err
}

func tempSlotName(parent string) (string, error) {
	// Append a random suffix so that overlapping tick cycles (or stale temp
	// slots from a previous crash) don't collide. Slots are created with
	// Temporary: true, so they are auto-dropped on connection close.
	var buf [4]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	suffix := hex.EncodeToString(buf[:])
	// Slot names must be <= 64 bytes and lowercase identifiers; truncate the
	// parent if needed to leave room for "_dsc_" + 8 hex chars.
	const reservedSuffix = len("_dsc_") + 8
	maxParent := 63 - reservedSuffix
	if len(parent) > maxParent {
		parent = parent[:maxParent]
	}
	return fmt.Sprintf("%s_dsc_%s", parent, suffix), nil
}

// pingDiscoveryReady is exported for tests so they can wait until the
// discovery goroutine has had a chance to run without sleeping.
//
//nolint:unused // used by tests in this package
func (d *discoverer) pingDiscoveryReady() time.Duration {
	return d.config.DiscoveryInterval
}
