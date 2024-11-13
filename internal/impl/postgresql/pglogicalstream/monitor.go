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
	"math"
	"strings"
	"sync"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Report is a structure that contains the current state of the Monitor
type Report struct {
	WalLagInBytes int64
	TableProgress map[string]float64
}

// Monitor is a structure that allows monitoring the progress of snapshot ingestion and replication lag
type Monitor struct {
	// tableStat contains numbers of rows for each table determined at the moment of the snapshot creation
	// this is used to calculate snapshot ingestion progress
	tableStat map[string]int64
	lock      sync.Mutex
	// snapshotProgress is a map of table names to the percentage of rows ingested from the snapshot
	snapshotProgress map[string]float64
	// replicationLagInBytes is the replication lag in bytes measured by
	// finding the difference between the latest LSN and the last confirmed LSN for the replication slot
	replicationLagInBytes int64

	dbConn       *sql.DB
	slotName     string
	logger       *service.Logger
	ticker       *time.Ticker
	cancelTicker context.CancelFunc
	ctx          context.Context
}

// NewMonitor creates a new Monitor instance
func NewMonitor(dbDSN string, logger *service.Logger, tables []string, slotName string, interval time.Duration) (*Monitor, error) {
	dbConn, err := openPgConnectionFromConfig(dbDSN)
	if err != nil {
		return nil, err
	}

	m := &Monitor{
		snapshotProgress:      map[string]float64{},
		replicationLagInBytes: 0,
		dbConn:                dbConn,
		slotName:              slotName,
		logger:                logger,
	}

	if err = m.readTablesStat(tables); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	m.ctx = ctx
	m.cancelTicker = cancel
	m.ticker = time.NewTicker(interval)

	go func() {
		for {
			select {
			case <-m.ticker.C:
				m.readReplicationLag()
				break
			case <-m.ctx.Done():
				m.ticker.Stop()
				return
			}
		}
	}()

	return m, nil
}

// GetSnapshotProgressForTable returns the snapshot ingestion progress for a given table
func (m *Monitor) GetSnapshotProgressForTable(table string) float64 {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.snapshotProgress[table]
}

// UpdateSnapshotProgressForTable updates the snapshot ingestion progress for a given table
func (m *Monitor) UpdateSnapshotProgressForTable(table string, position int) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.snapshotProgress[table] = math.Round(float64(position) / float64(m.tableStat[table]) * 100)
}

// we need to read the tables stat to calculate the snapshot ingestion progress
func (m *Monitor) readTablesStat(tables []string) error {
	results := make(map[string]int64)

	for _, table := range tables {
		tableWithoutSchema := strings.Split(table, ".")[1]
		query := "SELECT COUNT(*) FROM " + tableWithoutSchema

		var count int64
		err := m.dbConn.QueryRow(query).Scan(&count)

		if err != nil {
			// If the error is because the table doesn't exist, we'll set the count to 0
			// and continue. You might want to log this situation.
			if strings.Contains(err.Error(), "does not exist") {
				results[tableWithoutSchema] = 0
				continue
			}
			// For any other error, we'll return it
			return fmt.Errorf("error counting rows in table %s: %w", tableWithoutSchema, err)
		}

		results[tableWithoutSchema] = count
	}

	m.tableStat = results
	return nil
}

func (m *Monitor) readReplicationLag() {
	result, err := m.dbConn.Query(`SELECT slot_name,
       pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS lag_bytes
       FROM pg_replication_slots WHERE slot_name = $1;`, m.slotName)
	// calculate the replication lag in bytes
	// replicationLagInBytes = latestLsn - confirmedLsn
	if err != nil || result.Err() != nil {
		m.logger.Errorf("Error reading replication lag: %v", err)
		return
	}

	var slotName string
	var lagbytes int64
	for result.Next() {
		if err = result.Scan(&slotName, &lagbytes); err != nil {
			m.logger.Errorf("Error reading replication lag: %v", err)
			return
		}
	}

	m.replicationLagInBytes = lagbytes
}

// Report returns a snapshot of the monitor's state
func (m *Monitor) Report() *Report {
	m.lock.Lock()
	defer m.lock.Unlock()
	// report the snapshot ingestion progress
	// report the replication lag
	return &Report{
		WalLagInBytes: m.replicationLagInBytes,
		TableProgress: m.snapshotProgress,
	}
}

// Stop stops the monitor
func (m *Monitor) Stop() {
	m.cancelTicker()
	m.ticker.Stop()
	m.dbConn.Close()
}
