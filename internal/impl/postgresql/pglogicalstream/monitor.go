package pglogicalstream

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/redpanda-data/benthos/v4/public/service"
)

type Report struct {
	WalLagInBytes int64
	TableProgress map[string]float64
}

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

	debounced func(f func())

	dbConn       *sql.DB
	slotName     string
	logger       *service.Logger
	ticker       *time.Ticker
	cancelTicker context.CancelFunc
	ctx          context.Context
}

func NewMonitor(conf *pgconn.Config, logger *service.Logger, tables []string, slotName string) (*Monitor, error) {
	// debounces is user to throttle locks on the monitor to prevent unnecessary updates that would affect the performance
	debounced := NewDebouncer(100 * time.Millisecond)
	dbConn, err := openPgConnectionFromConfig(*conf)
	if err != nil {
		return nil, err
	}

	m := &Monitor{
		snapshotProgress:      map[string]float64{},
		replicationLagInBytes: 0,
		debounced:             debounced,
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
	m.ticker = time.NewTicker(5 * time.Second)

	return m, nil
}

// UpdateSnapshotProgressForTable updates the snapshot ingestion progress for a given table
func (m *Monitor) UpdateSnapshotProgressForTable(table string, position int) {
	storeSnapshotProgress := func() {
		m.lock.Lock()
		defer m.lock.Unlock()
		m.snapshotProgress[table] = float64(position) / float64(m.tableStat[table]) * 100
	}

	m.debounced(storeSnapshotProgress)
}

// we need to read the tables stat to calculate the snapshot ingestion progress
func (m *Monitor) readTablesStat(tables []string) error {
	results := make(map[string]int64)

	// Construct the query
	queryParts := make([]string, len(tables))
	for i, table := range tables {
		queryParts[i] = fmt.Sprintf("SELECT '%s' AS table_name, COUNT(*) FROM %s", table, table)
	}
	query := strings.Join(queryParts, " UNION ALL ")

	// Execute the query
	rows, err := m.dbConn.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Process the results
	for rows.Next() {
		var tableName string
		var count int64
		if err := rows.Scan(&tableName, &count); err != nil {
			return err
		}
		results[tableName] = count
	}

	if err := rows.Err(); err != nil {
		return err
	}

	m.tableStat = results
	return nil
}

func (m *Monitor) readReplicationLag() {
	result, err := m.dbConn.Query(`SELECT slot_name,
       pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS lag_bytes
       FROM pg_replication_slots WHERE slot_name = ?;`, m.slotName)
	// calculate the replication lag in bytes
	// replicationLagInBytes = latestLsn - confirmedLsn
	if result.Err() != nil || err != nil {
		m.logger.Errorf("Error reading replication lag: %v", err)
		return
	}

	var slotName string
	var lagbytes int64
	if err = result.Scan(&slotName, &lagbytes); err != nil {
		m.logger.Errorf("Error reading replication lag: %v", err)
		return
	}

	m.replicationLagInBytes = lagbytes
}

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

func (m *Monitor) Stop() {
	m.cancelTicker()
	m.ticker.Stop()
	m.dbConn.Close()
}

func (m *Monitor) startSync() {
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-m.ticker.C:
			m.readReplicationLag()
		}
	}
}
