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
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/asyncroutine"
)

// Report is a structure that contains the current state of the Monitor
type Report struct {
	WalLagInBytes int64
	TableProgress map[TableFQN]float64
	// TablesMissingFromPublication counts the required tables a watched
	// publication does not cover, as of the last check. Zero when no
	// publication is watched.
	TablesMissingFromPublication int64
}

// Monitor is a structure that allows monitoring the progress of snapshot ingestion and replication lag
type Monitor struct {
	// snapshotMu guards tableStat and snapshotProgress. An incremental
	// snapshot takes its tables at runtime, from a signal, so entries are
	// added long after construction and while Report is reading them.
	snapshotMu sync.Mutex
	// tableStat contains numbers of rows for each table determined at the moment of the snapshot creation
	// this is used to calculate snapshot ingestion progress
	tableStat map[TableFQN]float64
	// snapshotProgress is a map of table names to the number of rows ingested from the snapshot
	snapshotProgress map[TableFQN]*atomic.Int64
	// estimateFailed records tables whose row estimate could not be read, so
	// the retry per chunk warns only once each.
	estimateFailed map[TableFQN]struct{}
	// replicationLagInBytes is the replication lag in bytes measured by
	// finding the difference between the latest LSN and the last confirmed LSN for the replication slot
	replicationLagInBytes atomic.Int64

	// publicationMu guards publication, publicationTables and
	// lastMissingFromPublication, which WatchPublication sets after the loop
	// has started.
	publicationMu              sync.Mutex
	publication                string
	publicationTables          []TableFQN
	lastMissingFromPublication string
	missingFromPublication     atomic.Int64

	dbConn   *sql.DB
	slotName string
	logger   *service.Logger
	loop     *asyncroutine.Periodic
}

// NewMonitor creates a new Monitor instance.
func NewMonitor(
	ctx context.Context,
	config *Config,
	logger *service.Logger,
	tables []TableFQN,
	slotName string,
) (*Monitor, error) {
	if config.WalMonitorInterval <= 0 {
		return nil, fmt.Errorf("invalid monitoring interval: %s", config.WalMonitorInterval.String())
	}
	dbConn, err := openPgConnectionFromConfig(config)
	if err != nil {
		return nil, err
	}

	m := &Monitor{
		snapshotProgress:      make(map[TableFQN]*atomic.Int64, len(tables)),
		tableStat:             make(map[TableFQN]float64, len(tables)),
		estimateFailed:        map[TableFQN]struct{}{},
		replicationLagInBytes: atomic.Int64{},
		dbConn:                dbConn,
		slotName:              slotName,
		logger:                logger,
	}
	m.loop = asyncroutine.NewPeriodicWithContext(config.WalMonitorInterval, func(ctx context.Context) {
		m.readReplicationLag(ctx)
		m.checkPublication(ctx)
	})
	for _, table := range tables {
		m.snapshotProgress[table] = &atomic.Int64{}
		m.tableStat[table] = 0
	}
	if err = m.readTablesStat(ctx, tables); err != nil {
		return nil, err
	}
	m.loop.Start()
	return m, nil
}

// UpdateSnapshotProgressForTable updates the snapshot ingestion progress for a given table.
func (m *Monitor) UpdateSnapshotProgressForTable(table TableFQN, read int) {
	m.snapshotMu.Lock()
	progress, tracked := m.snapshotProgress[table]
	m.snapshotMu.Unlock()
	if tracked {
		progress.Add(int64(read))
	}
}

// MarkSnapshotComplete means that we finished snapshotting.
func (m *Monitor) MarkSnapshotComplete(table TableFQN) {
	m.snapshotMu.Lock()
	defer m.snapshotMu.Unlock()
	if progress, ok := m.snapshotProgress[table]; ok {
		progress.Store(int64(m.tableStat[table]))
	}
}

// TrackSnapshotTable makes a progress metric exist for table, reading its row
// estimate as the denominator. Idempotent, and a no-op for a table already
// tracked -- calling it twice must not reset the progress of a backfill in
// flight.
func (m *Monitor) TrackSnapshotTable(ctx context.Context, table TableFQN) {
	m.snapshotMu.Lock()
	_, tracked := m.snapshotProgress[table]
	m.snapshotMu.Unlock()
	if tracked {
		return
	}

	// Read outside the lock: Report must not wait on a round trip.
	estimate, err := m.readTableRowEstimate(ctx, table)
	if err != nil {
		// No entry, so the next chunk tries again. Report drops a table with
		// a total of zero, so caching a failed estimate would lose the metric
		// for the rest of the backfill.
		m.snapshotMu.Lock()
		defer m.snapshotMu.Unlock()
		if _, warned := m.estimateFailed[table]; !warned {
			m.estimateFailed[table] = struct{}{}
			m.logger.Warnf("Unable to read the row estimate for table %s, its snapshot progress is unavailable: %s", table, err)
		}
		return
	}

	m.snapshotMu.Lock()
	defer m.snapshotMu.Unlock()
	if _, raced := m.snapshotProgress[table]; raced {
		return
	}
	delete(m.estimateFailed, table)
	m.snapshotProgress[table] = &atomic.Int64{}
	m.tableStat[table] = estimate
}

// we need to read the tables stat to calculate the snapshot ingestion progress.
//
// Construction only, so it takes no lock: nothing else can see the maps yet.
func (m *Monitor) readTablesStat(ctx context.Context, tables []TableFQN) error {
	for _, table := range tables {
		count, err := m.readTableRowEstimate(ctx, table)
		if err != nil {
			// Keep going if only the table does not exist
			if strings.Contains(err.Error(), "does not exist") {
				continue
			}
			if errors.Is(err, errRowEstimateUnavailable) {
				m.logger.Warnf("No snapshot progress will be reported for table %s: %s", table, err)
				continue
			}
			// For any other error, we'll return it
			return err
		}

		m.tableStat[table] = count
	}
	return nil
}

var errRowEstimateUnavailable = errors.New("row estimate unavailable until the table is analysed; run ANALYZE to populate it")

// readTableRowEstimate reads the planner's row estimate for a table, which
// is the denominator of its snapshot progress. It is an estimate, so the
// progress is one too.
func (m *Monitor) readTableRowEstimate(ctx context.Context, table TableFQN) (float64, error) {
	var count float64
	if err := m.dbConn.QueryRowContext(
		ctx,
		`SELECT reltuples FROM pg_class WHERE oid = $1::regclass`,
		table.String(),
	).Scan(&count); err != nil {
		return 0, fmt.Errorf("error counting rows in table %s: %w", table, err)
	}
	// Negative means unknown, not a count. Reporting it as the denominator
	// would make Report drop the table for the life of the run.
	if count < 0 {
		return 0, fmt.Errorf("%w: table %s", errRowEstimateUnavailable, table)
	}
	return count, nil
}

func (m *Monitor) readReplicationLag(ctx context.Context) {
	result, err := m.dbConn.QueryContext(ctx, `SELECT slot_name,
       pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS lag_bytes
       FROM pg_replication_slots WHERE slot_name = $1;`, m.slotName)
	// calculate the replication lag in bytes
	// replicationLagInBytes = latestLsn - confirmedLsn
	if err != nil || result.Err() != nil {
		m.logger.Warnf("Error reading replication lag: %v", err)
		return
	}

	var slotName string
	var lagbytes int64
	for result.Next() {
		if err = result.Scan(&slotName, &lagbytes); err != nil {
			m.logger.Warnf("Error reading replication lag: %v", err)
			return
		}
	}

	m.replicationLagInBytes.Store(lagbytes)
}

// WatchPublication makes every monitor tick check that publication still
// covers tables, logging an error and counting the tables it does not. It is
// for an externally managed publication, which can change while the stream
// runs: a table dropped from one simply stops streaming, with nothing else to
// say so until the next connect.
func (m *Monitor) WatchPublication(publication string, tables []TableFQN) {
	m.publicationMu.Lock()
	defer m.publicationMu.Unlock()
	m.publication = publication
	m.publicationTables = slices.Clone(tables)
}

func (m *Monitor) checkPublication(ctx context.Context) {
	m.publicationMu.Lock()
	defer m.publicationMu.Unlock()
	if m.publication == "" {
		return
	}

	var missing []string
	for _, table := range m.publicationTables {
		q, err := publicationCoversTableQuery(m.publication, table.String())
		if err != nil {
			m.logger.Warnf("Unable to build the publication membership query for table %s: %s", table, err)
			return
		}
		var covered bool
		if err := m.dbConn.QueryRowContext(ctx, q).Scan(&covered); err != nil {
			// Unknown rather than missing, so the last result stands.
			m.logger.Warnf("Unable to check whether publication %s includes table %s: %s", m.publication, table, err)
			return
		}
		if !covered {
			missing = append(missing, table.String())
		}
	}

	m.missingFromPublication.Store(int64(len(missing)))
	// Logged on a change rather than on every tick.
	current := strings.Join(missing, ", ")
	if current == m.lastMissingFromPublication {
		return
	}
	m.lastMissingFromPublication = current
	if current == "" {
		m.logger.Infof("Publication %s includes every configured table again", m.publication)
		return
	}
	m.logger.Errorf("Publication %s no longer includes table(s) %s, so their changes are not being streamed: add them back with ALTER PUBLICATION %s ADD TABLE, or remove them from the input's tables",
		m.publication, current, m.publication)
}

// Report returns a snapshot of the monitor's state.
func (m *Monitor) Report() *Report {
	// report the snapshot ingestion progress
	// report the replication lag
	progress := map[TableFQN]float64{}
	m.snapshotMu.Lock()
	defer m.snapshotMu.Unlock()
	for table, read := range m.snapshotProgress {
		total := m.tableStat[table]
		if total <= 0 {
			continue
		}
		progress[table] = float64(read.Load()) / total
	}
	return &Report{
		WalLagInBytes:                m.replicationLagInBytes.Load(),
		TableProgress:                progress,
		TablesMissingFromPublication: m.missingFromPublication.Load(),
	}
}

// Stop stops the monitor.
func (m *Monitor) Stop() error {
	m.loop.Stop()
	return m.dbConn.Close()
}
