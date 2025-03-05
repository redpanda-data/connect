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
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/asyncroutine"
)

// Report is a structure that contains the current state of the Monitor
type Report struct {
	WalLagInBytes int64
	TableProgress map[TableFQN]float64
}

// Monitor is a structure that allows monitoring the progress of snapshot ingestion and replication lag
type Monitor struct {
	// tableStat contains numbers of rows for each table determined at the moment of the snapshot creation
	// this is used to calculate snapshot ingestion progress
	tableStat map[TableFQN]float64
	// snapshotProgress is a map of table names to the number of rows ingested from the snapshot
	snapshotProgress map[TableFQN]*atomic.Int64
	// replicationLagInBytes is the replication lag in bytes measured by
	// finding the difference between the latest LSN and the last confirmed LSN for the replication slot
	replicationLagInBytes atomic.Int64

	dbConn   *sql.DB
	slotName string
	logger   *service.Logger
	loop     *asyncroutine.Periodic
}

// NewMonitor creates a new Monitor instance
func NewMonitor(
	ctx context.Context,
	dbDSN string,
	logger *service.Logger,
	tables []TableFQN,
	slotName string,
	interval time.Duration,
) (*Monitor, error) {
	dbConn, err := openPgConnectionFromConfig(dbDSN)
	if err != nil {
		return nil, err
	}
	if interval <= 0 {
		return nil, fmt.Errorf("invalid monitoring interval: %s", interval.String())
	}

	m := &Monitor{
		snapshotProgress:      make(map[TableFQN]*atomic.Int64, len(tables)),
		tableStat:             make(map[TableFQN]float64, len(tables)),
		replicationLagInBytes: atomic.Int64{},
		dbConn:                dbConn,
		slotName:              slotName,
		logger:                logger,
	}
	m.loop = asyncroutine.NewPeriodicWithContext(interval, m.readReplicationLag)
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

// UpdateSnapshotProgressForTable updates the snapshot ingestion progress for a given table
func (m *Monitor) UpdateSnapshotProgressForTable(table TableFQN, read int) {
	m.snapshotProgress[table].Add(int64(read))
}

// MarkSnapshotComplete means that we finished snapshotting.
func (m *Monitor) MarkSnapshotComplete(table TableFQN) {
	m.snapshotProgress[table].Store(int64(m.tableStat[table]))
}

// we need to read the tables stat to calculate the snapshot ingestion progress
func (m *Monitor) readTablesStat(ctx context.Context, tables []TableFQN) error {
	for _, table := range tables {
		var count float64
		err := m.dbConn.QueryRowContext(
			ctx,
			`SELECT reltuples FROM pg_class WHERE oid = $1::regclass`,
			table.String(),
		).Scan(&count)

		if err != nil {
			// Keep going if only the table does not exist
			if strings.Contains(err.Error(), "does not exist") {
				continue
			}
			// For any other error, we'll return it
			return fmt.Errorf("error counting rows in table %s: %w", table, err)
		}

		m.tableStat[table] = count
	}
	return nil
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

// Report returns a snapshot of the monitor's state
func (m *Monitor) Report() *Report {
	// report the snapshot ingestion progress
	// report the replication lag
	progress := map[TableFQN]float64{}
	for table, read := range m.snapshotProgress {
		total := m.tableStat[table]
		if total <= 0 {
			continue
		}
		progress[table] = float64(read.Load()) / total
	}
	return &Report{
		WalLagInBytes: m.replicationLagInBytes.Load(),
		TableProgress: progress,
	}
}

// Stop stops the monitor
func (m *Monitor) Stop() error {
	m.loop.Stop()
	return m.dbConn.Close()
}
