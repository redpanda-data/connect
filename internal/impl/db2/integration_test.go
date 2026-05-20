// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package db2_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	// Register the db2_cdc input.
	_ "github.com/redpanda-data/connect/v4/internal/impl/db2"
	"github.com/redpanda-data/connect/v4/internal/impl/db2/db2test"
	"github.com/redpanda-data/connect/v4/internal/license"
)

// parseIntField extracts an integer from a JSON-decoded map value.
// DB2 CLI driver may return INTEGER columns as strings ("1") or as float64 (1)
// depending on the clidriver version and platform; this helper handles both.
func parseIntField(v any) int {
	switch t := v.(type) {
	case float64:
		return int(t)
	case string:
		n, _ := strconv.Atoi(t)
		return n
	case int64:
		return int(t)
	case int:
		return t
	default:
		return 0
	}
}

// TestIntegrationDB2CDCDriver tests basic SQL driver connectivity and query execution.
func TestIntegrationDB2CDCDriver(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	db := db2test.SetupTest(t)
	ctx := t.Context()

	// Drop table if it exists from a previous run (idempotent setup).
	_, _ = db.ExecContext(ctx, "DROP TABLE DB2INST1.INTEGRATION_TEST")
	t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), "DROP TABLE DB2INST1.INTEGRATION_TEST") })

	// Create a test table and insert rows.
	db.MustExecContext(ctx, `
		CREATE TABLE DB2INST1.INTEGRATION_TEST (
			ID   INTEGER NOT NULL PRIMARY KEY,
			NAME VARCHAR(100)
		)
	`)

	for i := 1; i <= 10; i++ {
		db.MustExecContext(ctx,
			"INSERT INTO DB2INST1.INTEGRATION_TEST (ID, NAME) VALUES (?, ?)",
			i, fmt.Sprintf("row-%d", i),
		)
	}

	// Query rows back and verify the driver returns correct data.
	rows, err := db.QueryContext(ctx, "SELECT ID, NAME FROM DB2INST1.INTEGRATION_TEST ORDER BY ID")
	require.NoError(t, err)
	defer rows.Close()

	var count int
	for rows.Next() {
		var id int
		var name string
		require.NoError(t, rows.Scan(&id, &name))
		t.Logf("row: id=%d name=%s", id, name)
		assert.Equal(t, count+1, id)
		assert.Equal(t, fmt.Sprintf("row-%d", count+1), name)
		count++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 10, count)
}

// TestIntegrationDB2DriverQueryContextCancel verifies that QueryContext returns
// within a short deadline when the context expires while a query is running.
// This exercises the QueryerContext + SQLCancel cancel path in driver.go.
//
// Uses a lock-based approach: a blocker connection holds an exclusive row lock
// inside an open transaction, and the cancellable query tries to acquire the
// same lock via SELECT...WITH RS USE AND KEEP EXCLUSIVE LOCKS. Because the
// blocker never commits, the query blocks indefinitely — until SQLCancel fires
// via the context deadline, proving the cancel path works.
//
// A computational cross-join is not reliable because DB2's query optimiser may
// complete it faster than the cancel deadline on a given instance.
func TestIntegrationDB2DriverQueryContextCancel(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	db := db2test.SetupTest(t)
	ctx := t.Context()

	// Set up a dedicated table for lock contention.
	_, _ = db.ExecContext(ctx, `DROP TABLE DB2INST1.CANCEL_QUERY_LOCK_TEST`)
	db.MustExecContext(ctx, `CREATE TABLE DB2INST1.CANCEL_QUERY_LOCK_TEST (ID INTEGER NOT NULL PRIMARY KEY, VAL VARCHAR(100))`)
	db.MustExecContext(ctx, `INSERT INTO DB2INST1.CANCEL_QUERY_LOCK_TEST (ID, VAL) VALUES (1, 'locked')`)

	// Open a second, independent *sql.DB so the blocker connection is completely
	// separate from the pool used by the cancellable query. database/sql reuses
	// connections from the pool, so using the same *sql.DB risks both the blocker
	// and the query sharing one underlying db2Conn — which would serialize them
	// rather than producing contention.
	blocker, err := sql.Open("db2-cli", db.DSN)
	require.NoError(t, err, "opening blocker connection")
	blocker.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = blocker.Close() })

	// Start a transaction on the blocker connection and acquire an exclusive lock
	// on the test row by updating it (without committing).
	blockerTx, err := blocker.BeginTx(ctx, nil)
	require.NoError(t, err, "beginning blocker transaction")
	_, err = blockerTx.ExecContext(ctx,
		`UPDATE DB2INST1.CANCEL_QUERY_LOCK_TEST SET VAL = 'still-locked' WHERE ID = 1`)
	require.NoError(t, err, "blocker UPDATE (acquiring row lock)")

	// The blocker holds the lock. Now run a SELECT that must acquire the same lock;
	// it will block until the blocker commits/rolls back or until SQLCancel fires.
	// WITH RS USE AND KEEP EXCLUSIVE LOCKS forces DB2 to request an X lock on the
	// row rather than a share lock, causing contention with the blocker's U/X lock.
	cancelCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	rows, queryErr := db.QueryContext(cancelCtx,
		`SELECT ID, VAL FROM DB2INST1.CANCEL_QUERY_LOCK_TEST WHERE ID = 1 WITH RS USE AND KEEP EXCLUSIVE LOCKS`)
	if rows != nil {
		_ = rows.Err()
		_ = rows.Close()
	}
	elapsed := time.Since(start)

	// Release the blocker lock so the table can be dropped at test teardown.
	_ = blockerTx.Rollback()

	// require.Error stops the test if err == nil, preventing nil dereference below.
	require.Error(t, queryErr, "expected an error from cancelled context — SQLCancel should have fired")
	assert.True(t,
		errors.Is(queryErr, context.DeadlineExceeded) ||
			errors.Is(queryErr, context.Canceled) ||
			strings.Contains(queryErr.Error(), "context"),
		"expected context error, got: %v", queryErr)
	// Generous bound: must return well within 10 s (did not block forever).
	assert.Less(t, elapsed, 10*time.Second, "QueryContext did not return promptly after cancel")
}

// TestIntegrationDB2DriverExecContextCancel verifies that ExecContext returns
// within a short deadline when the context expires during a DML statement.
// This exercises the ExecerContext + SQLCancel cancel path in driver.go.
//
// Uses a lock-based approach: a blocker connection holds an exclusive row lock
// inside an open transaction, and the cancellable UPDATE tries to modify the
// same row. Because the blocker never commits, the UPDATE blocks indefinitely
// — until SQLCancel fires via the context deadline, proving the cancel path
// works for ExecerContext.
//
// A computational UPDATE over many rows is not reliable because DB2 can complete
// it faster than the cancel deadline on a given instance.
func TestIntegrationDB2DriverExecContextCancel(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	db := db2test.SetupTest(t)
	ctx := t.Context()

	// Set up a table for lock contention.
	_, _ = db.ExecContext(ctx, `DROP TABLE DB2INST1.CANCEL_EXEC_LOCK_TEST`)
	db.MustExecContext(ctx, `CREATE TABLE DB2INST1.CANCEL_EXEC_LOCK_TEST (ID INTEGER NOT NULL PRIMARY KEY, VAL VARCHAR(100))`)
	db.MustExecContext(ctx, `INSERT INTO DB2INST1.CANCEL_EXEC_LOCK_TEST (ID, VAL) VALUES (1, 'initial')`)

	// Open a second, independent *sql.DB for the blocker connection so it does
	// not share the underlying db2Conn with the test's cancel query.
	blocker, err := sql.Open("db2-cli", db.DSN)
	require.NoError(t, err, "opening blocker connection")
	blocker.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = blocker.Close() })

	// Begin a transaction and acquire an exclusive row lock via UPDATE (no commit).
	blockerTx, err := blocker.BeginTx(ctx, nil)
	require.NoError(t, err, "beginning blocker transaction")
	_, err = blockerTx.ExecContext(ctx,
		`UPDATE DB2INST1.CANCEL_EXEC_LOCK_TEST SET VAL = 'held' WHERE ID = 1`)
	require.NoError(t, err, "blocker UPDATE (acquiring row lock)")

	// Now try to UPDATE the same row with a short deadline — it must block
	// waiting for the lock held by blockerTx, then be cancelled by SQLCancel.
	cancelCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, execErr := db.ExecContext(cancelCtx,
		`UPDATE DB2INST1.CANCEL_EXEC_LOCK_TEST SET VAL = 'cancelled' WHERE ID = 1`)
	elapsed := time.Since(start)

	// Release the blocker lock so the table can be dropped at test teardown.
	_ = blockerTx.Rollback()

	require.Error(t, execErr, "expected an error from cancelled context — SQLCancel should have fired")
	assert.True(t,
		errors.Is(execErr, context.DeadlineExceeded) ||
			errors.Is(execErr, context.Canceled) ||
			strings.Contains(execErr.Error(), "context"),
		"expected context error, got: %v", execErr)
	assert.Less(t, elapsed, 10*time.Second, "ExecContext did not return promptly after cancel")
}

// TestIntegrationDB2CDCSnapshotAndStreaming verifies the full snapshot → streaming flow.
func TestIntegrationDB2CDCSnapshotAndStreaming(t *testing.T) {
	integration.CheckSkip(t)

	db := db2test.SetupTest(t)
	ctx := t.Context()

	// Deactivate the CDC registration before dropping the table.  If asncap is
	// running with this table registered (STATE='A'), it will read the DROP TABLE
	// log entry and crash with ASN0061E.  Setting STATE='I' first tells asncap
	// to ignore log entries for this table, so the DROP is safe.
	_, _ = db.ExecContext(ctx, "UPDATE ASNCDC.IBMSNAP_REGISTER SET STATE='I' WHERE SOURCE_OWNER='DB2INST1' AND SOURCE_TABLE='CDC_TEST_EMPLOYEES'")
	_, _ = db.ExecContext(ctx, "DROP TABLE DB2INST1.CDC_TEST_EMPLOYEES")
	// Cleanup runs in LIFO order: stream.Stop → this Cleanup.
	// The stream is already stopped when we run, so deactivate then drop.
	t.Cleanup(func() {
		ctx2 := context.Background()
		_, _ = db.ExecContext(ctx2, "UPDATE ASNCDC.IBMSNAP_REGISTER SET STATE='I' WHERE SOURCE_OWNER='DB2INST1' AND SOURCE_TABLE='CDC_TEST_EMPLOYEES'")
		_, _ = db.ExecContext(ctx2, "DROP TABLE DB2INST1.CDC_TEST_EMPLOYEES")
	})

	// Clear any stale checkpoint so this run always starts with a fresh snapshot.
	_, _ = db.ExecContext(ctx, "DELETE FROM DB2INST1.CDC_CHECKPOINT")
	t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), "DELETE FROM DB2INST1.CDC_CHECKPOINT") })

	// Create the test table.
	db.MustExecContext(ctx, `
		CREATE TABLE DB2INST1.CDC_TEST_EMPLOYEES (
			EMP_ID   INTEGER NOT NULL PRIMARY KEY,
			EMP_NAME VARCHAR(100)
		)
	`)

	// Pre-populate for snapshot.
	for i := 1; i <= 5; i++ {
		db.MustExecContext(ctx,
			"INSERT INTO DB2INST1.CDC_TEST_EMPLOYEES (EMP_ID, EMP_NAME) VALUES (?, ?)",
			i, fmt.Sprintf("employee-%d", i),
		)
	}

	// Enable ASNCDC capture on this table.
	db.EnableASNCDC("DB2INST1", []string{"CDC_TEST_EMPLOYEES"})

	// Build and run the connector.
	connectorYAML := fmt.Sprintf(`
db2_cdc:
  dsn: %q
  schema: "DB2INST1"
  tables: ["CDC_TEST_EMPLOYEES"]
  snapshot_mode: initial
  snapshot_max_batch_size: 10
  poll_batch_size: 100
  stream_backoff_interval: 500ms
  checkpoint_cache_table_name: "DB2INST1.CDC_CHECKPOINT"
`, db.DSN)

	var (
		received   []string
		receivedMu sync.Mutex
	)

	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.AddInputYAML(connectorYAML))
	require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(_ context.Context, batch service.MessageBatch) error {
		receivedMu.Lock()
		defer receivedMu.Unlock()
		for _, msg := range batch {
			b, err := msg.AsBytes()
			if err != nil {
				continue
			}
			op, _ := msg.MetaGet("db2_operation")
			table, _ := msg.MetaGet("db2_table")
			csn, _ := msg.MetaGet("db2_csn")
			t.Logf("CDC event [%s] %s csn=%s: %s", op, table, csn, b)
			received = append(received, string(b))
		}
		return nil
	}))

	stream, err := streamBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())

	streamDone := make(chan error, 1)
	go func() {
		streamDone <- stream.Run(ctx)
	}()
	t.Cleanup(func() {
		if err := stream.StopWithin(10 * time.Second); err != nil {
			t.Logf("stream stop: %v", err)
		}
		if err := <-streamDone; err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("stream error: %v", err)
		}
	})

	// Wait for snapshot rows (5 pre-populated rows).
	assert.Eventually(t, func() bool {
		receivedMu.Lock()
		defer receivedMu.Unlock()
		return len(received) >= 5
	}, 2*time.Minute, 500*time.Millisecond, "snapshot: expected at least 5 events")

	// Log CT table and register state before streaming inserts.
	{
		var ctCount int
		_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM ASNCDC."CDC_DB2INST1_CDC_TEST_EMPLOYEES"`).Scan(&ctCount)
		var synchHex []byte
		_ = db.QueryRowContext(ctx,
			"SELECT SYNCHPOINT FROM ASNCDC.IBMSNAP_REGISTER WHERE SOURCE_OWNER='DB2INST1' AND SOURCE_TABLE='CDC_TEST_EMPLOYEES'",
		).Scan(&synchHex)
		t.Logf("before streaming inserts: CT_count=%d synchpoint=%X asncap_pid=%s",
			ctCount, synchHex, asncapPID())
	}

	// Insert streaming rows while the connector is running.
	for i := 6; i <= 10; i++ {
		db.MustExecContext(ctx,
			"INSERT INTO DB2INST1.CDC_TEST_EMPLOYEES (EMP_ID, EMP_NAME) VALUES (?, ?)",
			i, fmt.Sprintf("employee-%d", i),
		)
	}

	// Log CT table state a few seconds after inserts — if asncap is working,
	// the CT table should have rows by now.
	time.Sleep(5 * time.Second)
	{
		var ctCount int
		_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM ASNCDC."CDC_DB2INST1_CDC_TEST_EMPLOYEES"`).Scan(&ctCount)
		var synchHex []byte
		_ = db.QueryRowContext(ctx,
			"SELECT SYNCHPOINT FROM ASNCDC.IBMSNAP_REGISTER WHERE SOURCE_OWNER='DB2INST1' AND SOURCE_TABLE='CDC_TEST_EMPLOYEES'",
		).Scan(&synchHex)
		t.Logf("5s after streaming inserts: CT_count=%d synchpoint=%X asncap_pid=%s",
			ctCount, synchHex, asncapPID())
		if ctCount == 0 {
			t.Logf("WARNING: CT table is empty — asncap may not be capturing; printing asncap log")
			if logOut, err := os.ReadFile("/tmp/asncap.log"); err == nil && len(logOut) > 0 {
				tail := logOut
				if len(tail) > 3000 {
					tail = tail[len(tail)-3000:]
				}
				t.Logf("asncap log:\n%s", tail)
			}
		}
	}

	// Wait for streaming rows (5 snapshot + 5 streaming = 10 total).
	// asnccmd reinit can take >2 minutes on slow machines, so allow 4 minutes.
	assert.Eventually(t, func() bool {
		receivedMu.Lock()
		defer receivedMu.Unlock()
		return len(received) >= 10
	}, 4*time.Minute, 500*time.Millisecond, "streaming: expected at least 10 events total")
}

// asncapPID returns the PID of the running asncap process for TESTDB, or "none".
func asncapPID() string {
	out, _ := exec.Command("pgrep", "-f", "capture_server=TESTDB").Output()
	pid := strings.TrimSpace(string(out))
	if pid == "" {
		return "none"
	}
	return pid
}

// TestIntegrationDB2CDCUpdateBeforeImage verifies that UPDATE operations
// produce a single event with op="u", before=old row, after=new row.
func TestIntegrationDB2CDCUpdateBeforeImage(t *testing.T) {
	integration.CheckSkip(t)

	db := db2test.SetupTest(t)
	ctx := t.Context()

	// Cleanup from any previous run.
	_, _ = db.ExecContext(ctx, `DROP TABLE ASNCDC."CDC_DB2INST1_CDC_UPDATE_TEST"`)
	_, _ = db.ExecContext(ctx, `DELETE FROM ASNCDC.IBMSNAP_PRUNCNTL WHERE SOURCE_TABLE='CDC_UPDATE_TEST'`)
	_, _ = db.ExecContext(ctx, `DELETE FROM ASNCDC.IBMSNAP_REGISTER WHERE SOURCE_TABLE='CDC_UPDATE_TEST'`)
	_, _ = db.ExecContext(ctx, `DROP TABLE DB2INST1.CDC_UPDATE_TEST`)
	_, _ = db.ExecContext(ctx, `DROP TABLE DB2INST1.CDC_UPDATE_CHECKPOINT`)

	db.MustExecContext(ctx, `CREATE TABLE DB2INST1.CDC_UPDATE_TEST (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR(100))`)
	db.MustExecContext(ctx, `INSERT INTO DB2INST1.CDC_UPDATE_TEST (ID, NAME) VALUES (1, 'original')`)
	db.EnableASNCDC("DB2INST1", []string{"CDC_UPDATE_TEST"})

	connectorYAML := fmt.Sprintf(`
db2_cdc:
  dsn: %q
  schema: "DB2INST1"
  tables: ["CDC_UPDATE_TEST"]
  snapshot_mode: never
  poll_batch_size: 100
  stream_backoff_interval: 200ms
  checkpoint_cache_table_name: "DB2INST1.CDC_UPDATE_CHECKPOINT"
`, db.DSN)

	type cdcEvent struct {
		op     string
		before map[string]any
		after  map[string]any
	}
	var (
		received   []cdcEvent
		receivedMu sync.Mutex
	)

	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.AddInputYAML(connectorYAML))
	require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(_ context.Context, batch service.MessageBatch) error {
		receivedMu.Lock()
		defer receivedMu.Unlock()
		for _, msg := range batch {
			b, err := msg.AsBytes()
			if err != nil {
				continue
			}
			t.Logf("CDC event: %s", b)
			var env map[string]any
			if jsonErr := json.Unmarshal(b, &env); jsonErr != nil {
				continue
			}
			op, _ := env["op"].(string)
			var before, after map[string]any
			if v, ok := env["before"]; ok && v != nil {
				before, _ = v.(map[string]any)
			}
			if v, ok := env["after"]; ok && v != nil {
				after, _ = v.(map[string]any)
			}
			received = append(received, cdcEvent{op: op, before: before, after: after})
		}
		return nil
	}))

	stream, err := streamBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())

	streamDone := make(chan error, 1)
	go func() {
		streamDone <- stream.Run(ctx)
	}()
	t.Cleanup(func() {
		if err := stream.StopWithin(10 * time.Second); err != nil {
			t.Logf("stream stop: %v", err)
		}
		if err := <-streamDone; err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("stream error: %v", err)
		}
	})

	// Let streaming stabilize.
	time.Sleep(5 * time.Second)

	// Perform an UPDATE — DB2 SQL Replication captures this as a D+I pair in the CD table.
	db.MustExecContext(ctx, `UPDATE DB2INST1.CDC_UPDATE_TEST SET NAME = 'updated' WHERE ID = 1`)

	assert.Eventually(t, func() bool {
		receivedMu.Lock()
		defer receivedMu.Unlock()
		for _, e := range received {
			if e.op == "u" && e.before != nil && e.after != nil {
				name, _ := e.before["NAME"].(string)
				newName, _ := e.after["NAME"].(string)
				return name == "original" && newName == "updated"
			}
		}
		return false
	}, 2*time.Minute, 500*time.Millisecond,
		"expected UPDATE event with op='u', before.NAME='original', after.NAME='updated'")
}

// TestIntegrationDB2CDCIncrementalSnapshotMultiChunk verifies that a table with more
// rows than snapshot_max_batch_size is fully snapshotted across multiple chunks with
// no missing or duplicate rows.
func TestIntegrationDB2CDCIncrementalSnapshotMultiChunk(t *testing.T) {
	integration.CheckSkip(t)

	db := db2test.SetupTest(t)
	ctx := t.Context()

	const (
		srcTable  = "INC_MC_TEST"
		schema    = "DB2INST1"
		rowCount  = 20
		chunkSize = 4
	)

	_, _ = db.ExecContext(ctx, `DROP TABLE ASNCDC."CDC_DB2INST1_INC_MC_TEST"`)
	_, _ = db.ExecContext(ctx, `DELETE FROM ASNCDC.IBMSNAP_PRUNCNTL WHERE SOURCE_TABLE='INC_MC_TEST'`)
	_, _ = db.ExecContext(ctx, `DELETE FROM ASNCDC.IBMSNAP_REGISTER WHERE SOURCE_TABLE='INC_MC_TEST'`)
	_, _ = db.ExecContext(ctx, `DROP TABLE DB2INST1.INC_MC_TEST`)
	_, _ = db.ExecContext(ctx, `DROP TABLE DB2INST1.INC_MC_CHECKPOINT`)
	_, _ = db.ExecContext(ctx, `DROP TABLE DB2INST1.INC_MC_SIGNALS`)

	db.MustExecContext(ctx, `CREATE TABLE DB2INST1.INC_MC_TEST (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR(100))`)
	for i := 1; i <= rowCount; i++ {
		db.MustExecContext(ctx,
			`INSERT INTO DB2INST1.INC_MC_TEST (ID, NAME) VALUES (?, ?)`,
			i, fmt.Sprintf("row-%d", i),
		)
	}
	db.EnableASNCDC(schema, []string{srcTable})

	connectorYAML := fmt.Sprintf(`
db2_cdc:
  dsn: %q
  schema: "DB2INST1"
  tables: ["INC_MC_TEST"]
  snapshot_mode: never
  snapshot_max_batch_size: %d
  poll_batch_size: 100
  stream_backoff_interval: 300ms
  checkpoint_cache_table_name: "DB2INST1.INC_MC_CHECKPOINT"
  signal_table: "DB2INST1.INC_MC_SIGNALS"
`, db.DSN, chunkSize)

	type snapRow struct{ id int }
	var (
		snapRows   []snapRow
		snapRowsMu sync.Mutex
	)

	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.AddInputYAML(connectorYAML))
	require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(_ context.Context, batch service.MessageBatch) error {
		snapRowsMu.Lock()
		defer snapRowsMu.Unlock()
		for _, msg := range batch {
			b, _ := msg.AsBytes()
			var env map[string]any
			if err := json.Unmarshal(b, &env); err != nil {
				continue
			}
			op, _ := env["op"].(string)
			if op != "r" {
				continue
			}
			after, _ := env["after"].(map[string]any)
			if after == nil {
				continue
			}
			id := parseIntField(after["ID"])
			snapRows = append(snapRows, snapRow{id: id})
			t.Logf("snapshot op=r id=%d", id)
		}
		return nil
	}))

	stream, err := streamBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())

	streamDone := make(chan error, 1)
	go func() { streamDone <- stream.Run(ctx) }()
	t.Cleanup(func() {
		if err := stream.StopWithin(10 * time.Second); err != nil {
			t.Logf("stream stop: %v", err)
		}
		if err := <-streamDone; err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("stream error: %v", err)
		}
	})

	// Let streaming stabilize before triggering snapshot.
	time.Sleep(5 * time.Second)

	db.MustExecContext(ctx,
		`INSERT INTO DB2INST1.INC_MC_SIGNALS (ID, TYPE, DATA) VALUES ('mc-1', 'execute-snapshot', '{"data-collections":["DB2INST1.INC_MC_TEST"]}')`)

	// Wait for all rowCount snapshot events across multiple chunks (chunkSize=4 → 5 chunks).
	assert.Eventually(t, func() bool {
		snapRowsMu.Lock()
		defer snapRowsMu.Unlock()
		return len(snapRows) >= rowCount
	}, 3*time.Minute, 500*time.Millisecond,
		"multi-chunk snapshot: expected %d op=r events", rowCount)

	snapRowsMu.Lock()
	defer snapRowsMu.Unlock()

	// Each row ID must appear exactly once.
	idCounts := make(map[int]int, rowCount)
	for _, r := range snapRows {
		idCounts[r.id]++
	}
	for i := 1; i <= rowCount; i++ {
		assert.Equal(t, 1, idCounts[i],
			"row ID=%d: expected exactly 1 snapshot event, got %d", i, idCounts[i])
	}
}

// TestIntegrationDB2CDCIncrementalSnapshotWithConcurrentUpdate verifies the open-window
// deduplication invariant: when rows are updated concurrently with an incremental snapshot,
// no row ID appears more than once as a snapshot (op="r") event. This confirms the
// DeduplicationWindow correctly evicts snapshot rows whose CDC update arrived in-window.
func TestIntegrationDB2CDCIncrementalSnapshotWithConcurrentUpdate(t *testing.T) {
	integration.CheckSkip(t)

	db := db2test.SetupTest(t)
	ctx := t.Context()

	const (
		srcTable  = "INC_UPD_TEST"
		schema    = "DB2INST1"
		rowCount  = 10
		chunkSize = 3
	)

	_, _ = db.ExecContext(ctx, `DROP TABLE ASNCDC."CDC_DB2INST1_INC_UPD_TEST"`)
	_, _ = db.ExecContext(ctx, `DELETE FROM ASNCDC.IBMSNAP_PRUNCNTL WHERE SOURCE_TABLE='INC_UPD_TEST'`)
	_, _ = db.ExecContext(ctx, `DELETE FROM ASNCDC.IBMSNAP_REGISTER WHERE SOURCE_TABLE='INC_UPD_TEST'`)
	_, _ = db.ExecContext(ctx, `DROP TABLE DB2INST1.INC_UPD_TEST`)
	_, _ = db.ExecContext(ctx, `DROP TABLE DB2INST1.INC_UPD_CHECKPOINT`)
	_, _ = db.ExecContext(ctx, `DROP TABLE DB2INST1.INC_UPD_SIGNALS`)

	db.MustExecContext(ctx, `CREATE TABLE DB2INST1.INC_UPD_TEST (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR(100))`)
	for i := 1; i <= rowCount; i++ {
		db.MustExecContext(ctx,
			`INSERT INTO DB2INST1.INC_UPD_TEST (ID, NAME) VALUES (?, ?)`,
			i, fmt.Sprintf("before-%d", i),
		)
	}
	db.EnableASNCDC(schema, []string{srcTable})

	connectorYAML := fmt.Sprintf(`
db2_cdc:
  dsn: %q
  schema: "DB2INST1"
  tables: ["INC_UPD_TEST"]
  snapshot_mode: never
  snapshot_max_batch_size: %d
  poll_batch_size: 100
  stream_backoff_interval: 300ms
  checkpoint_cache_table_name: "DB2INST1.INC_UPD_CHECKPOINT"
  signal_table: "DB2INST1.INC_UPD_SIGNALS"
`, db.DSN, chunkSize)

	// Track snapshot events (op="r") and CDC update events (op="u") per row ID.
	type eventKind struct {
		op    string
		value string // NAME value
	}
	var (
		byID   = make(map[int][]eventKind)
		byIDMu sync.Mutex
	)

	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.AddInputYAML(connectorYAML))
	require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(_ context.Context, batch service.MessageBatch) error {
		byIDMu.Lock()
		defer byIDMu.Unlock()
		for _, msg := range batch {
			b, _ := msg.AsBytes()
			var env map[string]any
			if err := json.Unmarshal(b, &env); err != nil {
				continue
			}
			op, _ := env["op"].(string)
			if op != "r" && op != "u" {
				continue
			}
			// For op="r" and op="u", the row is in "after".
			after, _ := env["after"].(map[string]any)
			if after == nil {
				continue
			}
			id := parseIntField(after["ID"])
			name, _ := after["NAME"].(string)
			byID[id] = append(byID[id], eventKind{op: op, value: name})
			t.Logf("event op=%s id=%d name=%s", op, id, name)
		}
		return nil
	}))

	stream, err := streamBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())

	streamDone := make(chan error, 1)
	go func() { streamDone <- stream.Run(ctx) }()
	t.Cleanup(func() {
		if err := stream.StopWithin(10 * time.Second); err != nil {
			t.Logf("stream stop: %v", err)
		}
		if err := <-streamDone; err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("stream error: %v", err)
		}
	})

	time.Sleep(5 * time.Second)

	// Trigger snapshot, then immediately update all rows concurrently.
	// Some updates will land inside the dedup window (evicting snapshot rows);
	// others will land after — both orderings are correct by the algorithm.
	db.MustExecContext(ctx,
		`INSERT INTO DB2INST1.INC_UPD_SIGNALS (ID, TYPE, DATA) VALUES ('upd-1', 'execute-snapshot', '{"data-collections":["DB2INST1.INC_UPD_TEST"]}')`)

	go func() {
		// Update all rows as fast as possible — overlapping with snapshot chunks.
		for i := 1; i <= rowCount; i++ {
			_, _ = db.ExecContext(ctx,
				`UPDATE DB2INST1.INC_UPD_TEST SET NAME = ? WHERE ID = ?`,
				fmt.Sprintf("after-%d", i), i,
			)
		}
	}()

	// Wait until we have at least rowCount total events (snapshot + CDC combined)
	// and at least rowCount distinct IDs appear with some event.
	assert.Eventually(t, func() bool {
		byIDMu.Lock()
		defer byIDMu.Unlock()
		return len(byID) >= rowCount
	}, 3*time.Minute, 500*time.Millisecond,
		"concurrent-update: expected events for all %d row IDs", rowCount)

	// Wait for at least one CDC update event.  The snapshot may complete before
	// asncap flushes the in-flight update transactions to the CD table; give it
	// extra time to deliver the CDC events so the dedup-window invariant can be
	// verified.
	assert.Eventually(t, func() bool {
		byIDMu.Lock()
		defer byIDMu.Unlock()
		for _, events := range byID {
			for _, e := range events {
				if e.op == "u" {
					return true
				}
			}
		}
		return false
	}, 2*time.Minute, 500*time.Millisecond,
		"concurrent-update: expected at least 1 CDC update event (proves dedup window was exercised)")

	// KEY INVARIANT: no row ID appears more than once as op="r" (snapshot).
	// Regardless of whether the update was inside or outside the dedup window,
	// the dedup window must never emit two snapshot events for the same row.
	byIDMu.Lock()
	defer byIDMu.Unlock()

	totalCDCUpdates := 0
	for id, events := range byID {
		readCount := 0
		for _, e := range events {
			if e.op == "r" {
				readCount++
			}
			if e.op == "u" {
				totalCDCUpdates++
			}
		}
		assert.LessOrEqual(t, readCount, 1,
			"row ID=%d: dedup window must emit at most 1 snapshot event, got %d", id, readCount)
	}
	assert.GreaterOrEqual(t, totalCDCUpdates, 1,
		"expected at least 1 CDC update event — proves the dedup window was actually exercised")
}

// TestIntegrationDB2CDCIncrementalSnapshotWithConcurrentDelete verifies that when rows
// are deleted during an incremental snapshot, each deleted row appears at most once as
// op="r": either the snapshot row is evicted by the in-window CDC delete (row appears
// only as op="d"), or the delete arrives after the window closes (row appears as op="r"
// then op="d"). No row may appear twice as op="r".
func TestIntegrationDB2CDCIncrementalSnapshotWithConcurrentDelete(t *testing.T) {
	integration.CheckSkip(t)

	db := db2test.SetupTest(t)
	ctx := t.Context()

	const (
		srcTable    = "INC_DEL_TEST"
		schema      = "DB2INST1"
		rowCount    = 10
		deleteCount = 5 // delete the first 5 rows
		chunkSize   = 3
	)

	_, _ = db.ExecContext(ctx, `DROP TABLE ASNCDC."CDC_DB2INST1_INC_DEL_TEST"`)
	_, _ = db.ExecContext(ctx, `DELETE FROM ASNCDC.IBMSNAP_PRUNCNTL WHERE SOURCE_TABLE='INC_DEL_TEST'`)
	_, _ = db.ExecContext(ctx, `DELETE FROM ASNCDC.IBMSNAP_REGISTER WHERE SOURCE_TABLE='INC_DEL_TEST'`)
	_, _ = db.ExecContext(ctx, `DROP TABLE DB2INST1.INC_DEL_TEST`)
	_, _ = db.ExecContext(ctx, `DROP TABLE DB2INST1.INC_DEL_CHECKPOINT`)
	_, _ = db.ExecContext(ctx, `DROP TABLE DB2INST1.INC_DEL_SIGNALS`)

	db.MustExecContext(ctx, `CREATE TABLE DB2INST1.INC_DEL_TEST (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR(100))`)
	for i := 1; i <= rowCount; i++ {
		db.MustExecContext(ctx,
			`INSERT INTO DB2INST1.INC_DEL_TEST (ID, NAME) VALUES (?, ?)`,
			i, fmt.Sprintf("row-%d", i),
		)
	}
	db.EnableASNCDC(schema, []string{srcTable})

	connectorYAML := fmt.Sprintf(`
db2_cdc:
  dsn: %q
  schema: "DB2INST1"
  tables: ["INC_DEL_TEST"]
  snapshot_mode: never
  snapshot_max_batch_size: %d
  poll_batch_size: 100
  stream_backoff_interval: 300ms
  checkpoint_cache_table_name: "DB2INST1.INC_DEL_CHECKPOINT"
  signal_table: "DB2INST1.INC_DEL_SIGNALS"
`, db.DSN, chunkSize)

	// Track events per row ID.
	var (
		snapCount         = make(map[int]int) // op="r" count per ID
		concurrentDeletes = make(map[int]int) // op="d" count per ID
		eventsMu          sync.Mutex
	)

	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.AddInputYAML(connectorYAML))
	require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(_ context.Context, batch service.MessageBatch) error {
		eventsMu.Lock()
		defer eventsMu.Unlock()
		for _, msg := range batch {
			b, _ := msg.AsBytes()
			var env map[string]any
			if err := json.Unmarshal(b, &env); err != nil {
				continue
			}
			op, _ := env["op"].(string)
			if op != "r" && op != "d" {
				continue
			}
			// For op="d", row data is in "before"; for op="r" it's in "after".
			rowData, _ := env["after"].(map[string]any)
			if op == "d" {
				rowData, _ = env["before"].(map[string]any)
			}
			if rowData == nil {
				continue
			}
			id := parseIntField(rowData["ID"])
			if op == "r" {
				snapCount[id]++
				t.Logf("op=r id=%d", id)
			} else {
				concurrentDeletes[id]++
				t.Logf("op=d id=%d", id)
			}
		}
		return nil
	}))

	stream, err := streamBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())

	streamDone := make(chan error, 1)
	go func() { streamDone <- stream.Run(ctx) }()
	t.Cleanup(func() {
		if err := stream.StopWithin(10 * time.Second); err != nil {
			t.Logf("stream stop: %v", err)
		}
		if err := <-streamDone; err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("stream error: %v", err)
		}
	})

	time.Sleep(5 * time.Second)

	// Trigger snapshot and concurrently delete the first deleteCount rows.
	db.MustExecContext(ctx,
		`INSERT INTO DB2INST1.INC_DEL_SIGNALS (ID, TYPE, DATA) VALUES ('del-1', 'execute-snapshot', '{"data-collections":["DB2INST1.INC_DEL_TEST"]}')`)

	go func() {
		for i := 1; i <= deleteCount; i++ {
			_, _ = db.ExecContext(ctx,
				`DELETE FROM DB2INST1.INC_DEL_TEST WHERE ID = ?`, i)
		}
	}()

	// Wait for snapshot events for the non-deleted rows (IDs deleteCount+1 through rowCount).
	assert.Eventually(t, func() bool {
		eventsMu.Lock()
		defer eventsMu.Unlock()
		for i := deleteCount + 1; i <= rowCount; i++ {
			if snapCount[i] == 0 {
				return false
			}
		}
		return true
	}, 3*time.Minute, 500*time.Millisecond,
		"concurrent-delete: expected snapshot events for surviving rows %d-%d", deleteCount+1, rowCount)

	// KEY INVARIANT: no row ID appears more than once as op="r".
	eventsMu.Lock()
	defer eventsMu.Unlock()

	for i := 1; i <= rowCount; i++ {
		assert.LessOrEqual(t, snapCount[i], 1,
			"row ID=%d: dedup window must emit at most 1 snapshot event, got %d", i, snapCount[i])
	}
}

// TestIntegrationDB2CDCIncrementalSnapshotWithConcurrentInsert verifies that new rows
// inserted after the snapshot window opens arrive via CDC (op="c") and are not emitted
// a second time as snapshot (op="r") rows — the dedup window never evicts CDC inserts.
func TestIntegrationDB2CDCIncrementalSnapshotWithConcurrentInsert(t *testing.T) {
	integration.CheckSkip(t)

	db := db2test.SetupTest(t)
	ctx := t.Context()

	const (
		srcTable  = "INC_INS_TEST"
		schema    = "DB2INST1"
		baseRows  = 10
		newRows   = 5
		chunkSize = 4
	)

	_, _ = db.ExecContext(ctx, `DROP TABLE ASNCDC."CDC_DB2INST1_INC_INS_TEST"`)
	_, _ = db.ExecContext(ctx, `DELETE FROM ASNCDC.IBMSNAP_PRUNCNTL WHERE SOURCE_TABLE='INC_INS_TEST'`)
	_, _ = db.ExecContext(ctx, `DELETE FROM ASNCDC.IBMSNAP_REGISTER WHERE SOURCE_TABLE='INC_INS_TEST'`)
	_, _ = db.ExecContext(ctx, `DROP TABLE DB2INST1.INC_INS_TEST`)
	_, _ = db.ExecContext(ctx, `DROP TABLE DB2INST1.INC_INS_CHECKPOINT`)
	_, _ = db.ExecContext(ctx, `DROP TABLE DB2INST1.INC_INS_SIGNALS`)

	db.MustExecContext(ctx, `CREATE TABLE DB2INST1.INC_INS_TEST (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR(100))`)
	for i := 1; i <= baseRows; i++ {
		db.MustExecContext(ctx,
			`INSERT INTO DB2INST1.INC_INS_TEST (ID, NAME) VALUES (?, ?)`,
			i, fmt.Sprintf("base-%d", i),
		)
	}
	db.EnableASNCDC(schema, []string{srcTable})

	connectorYAML := fmt.Sprintf(`
db2_cdc:
  dsn: %q
  schema: "DB2INST1"
  tables: ["INC_INS_TEST"]
  snapshot_mode: never
  snapshot_max_batch_size: %d
  poll_batch_size: 100
  stream_backoff_interval: 300ms
  checkpoint_cache_table_name: "DB2INST1.INC_INS_CHECKPOINT"
  signal_table: "DB2INST1.INC_INS_SIGNALS"
`, db.DSN, chunkSize)

	var (
		snapIDs   = make(map[int]int) // op="r" count per ID
		insertIDs = make(map[int]int) // op="c" count per ID
		eventsMu  sync.Mutex
	)

	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.AddInputYAML(connectorYAML))
	require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(_ context.Context, batch service.MessageBatch) error {
		eventsMu.Lock()
		defer eventsMu.Unlock()
		for _, msg := range batch {
			b, _ := msg.AsBytes()
			var env map[string]any
			if err := json.Unmarshal(b, &env); err != nil {
				continue
			}
			op, _ := env["op"].(string)
			if op != "r" && op != "c" {
				continue
			}
			after, _ := env["after"].(map[string]any)
			if after == nil {
				continue
			}
			id := parseIntField(after["ID"])
			if op == "r" {
				snapIDs[id]++
				t.Logf("op=r id=%d", id)
			} else {
				insertIDs[id]++
				t.Logf("op=c id=%d", id)
			}
		}
		return nil
	}))

	stream, err := streamBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())

	streamDone := make(chan error, 1)
	go func() { streamDone <- stream.Run(ctx) }()
	t.Cleanup(func() {
		if err := stream.StopWithin(10 * time.Second); err != nil {
			t.Logf("stream stop: %v", err)
		}
		if err := <-streamDone; err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("stream error: %v", err)
		}
	})

	time.Sleep(5 * time.Second)

	// Trigger snapshot and concurrently insert new rows beyond the original key range.
	db.MustExecContext(ctx,
		`INSERT INTO DB2INST1.INC_INS_SIGNALS (ID, TYPE, DATA) VALUES ('ins-1', 'execute-snapshot', '{"data-collections":["DB2INST1.INC_INS_TEST"]}')`)

	go func() {
		for i := baseRows + 1; i <= baseRows+newRows; i++ {
			_, _ = db.ExecContext(ctx,
				`INSERT INTO DB2INST1.INC_INS_TEST (ID, NAME) VALUES (?, ?)`,
				i, fmt.Sprintf("new-%d", i),
			)
		}
	}()

	// Wait for the base snapshot rows.
	assert.Eventually(t, func() bool {
		eventsMu.Lock()
		defer eventsMu.Unlock()
		return len(snapIDs) >= baseRows
	}, 3*time.Minute, 500*time.Millisecond,
		"concurrent-insert: expected %d snapshot events for base rows", baseRows)

	// Wait for all new rows to appear as CDC inserts.
	assert.Eventually(t, func() bool {
		eventsMu.Lock()
		defer eventsMu.Unlock()
		for i := baseRows + 1; i <= baseRows+newRows; i++ {
			// New rows may appear as op="c" (CDC) or op="r" (if the snapshot chunk
			// was read after the insert and before the chunk's highCSN). Either is
			// correct; what matters is no double-emission.
			if snapIDs[i]+insertIDs[i] == 0 {
				return false
			}
		}
		return true
	}, 3*time.Minute, 500*time.Millisecond,
		"concurrent-insert: expected all new row IDs to appear via CDC or snapshot")

	eventsMu.Lock()
	defer eventsMu.Unlock()

	// Base rows should appear exactly once as op="r" (they were in the snapshot).
	for i := 1; i <= baseRows; i++ {
		assert.Equal(t, 1, snapIDs[i],
			"base row ID=%d: expected exactly 1 snapshot event", i)
	}

	// New rows must appear exactly once — LessOrEqual(1) would allow silent drops (count=0).
	for i := baseRows + 1; i <= baseRows+newRows; i++ {
		total := snapIDs[i] + insertIDs[i]
		assert.Equal(t, 1, total,
			"new row ID=%d: must appear exactly once across op=r and op=c, got %d", i, total)
	}
}

// TestIntegrationDB2CDCIncrementalSnapshot verifies that inserting an execute-snapshot
// signal row triggers an ad-hoc re-snapshot of the specified table.
func TestIntegrationDB2CDCIncrementalSnapshot(t *testing.T) {
	integration.CheckSkip(t)

	db := db2test.SetupTest(t)
	ctx := t.Context()

	_, _ = db.ExecContext(ctx, `DROP TABLE ASNCDC."CDC_DB2INST1_INC_SNAP_TEST"`)
	_, _ = db.ExecContext(ctx, `DELETE FROM ASNCDC.IBMSNAP_PRUNCNTL WHERE SOURCE_TABLE='INC_SNAP_TEST'`)
	_, _ = db.ExecContext(ctx, `DELETE FROM ASNCDC.IBMSNAP_REGISTER WHERE SOURCE_TABLE='INC_SNAP_TEST'`)
	_, _ = db.ExecContext(ctx, `DROP TABLE DB2INST1.INC_SNAP_TEST`)
	_, _ = db.ExecContext(ctx, `DROP TABLE DB2INST1.INC_SNAP_CHECKPOINT`)
	_, _ = db.ExecContext(ctx, `DROP TABLE DB2INST1.CDC_SIGNALS`)

	db.MustExecContext(ctx, `CREATE TABLE DB2INST1.INC_SNAP_TEST (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR(100))`)
	db.MustExecContext(ctx, `INSERT INTO DB2INST1.INC_SNAP_TEST (ID, NAME) VALUES (1, 'row-1')`)
	db.MustExecContext(ctx, `INSERT INTO DB2INST1.INC_SNAP_TEST (ID, NAME) VALUES (2, 'row-2')`)
	db.EnableASNCDC("DB2INST1", []string{"INC_SNAP_TEST"})

	connectorYAML := fmt.Sprintf(`
db2_cdc:
  dsn: %q
  schema: "DB2INST1"
  tables: ["INC_SNAP_TEST"]
  snapshot_mode: never
  poll_batch_size: 100
  stream_backoff_interval: 500ms
  checkpoint_cache_table_name: "DB2INST1.INC_SNAP_CHECKPOINT"
  signal_table: "DB2INST1.CDC_SIGNALS"
`, db.DSN)

	var (
		received   []string
		receivedMu sync.Mutex
	)

	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.AddInputYAML(connectorYAML))
	require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(_ context.Context, batch service.MessageBatch) error {
		receivedMu.Lock()
		defer receivedMu.Unlock()
		for _, msg := range batch {
			b, err := msg.AsBytes()
			if err != nil {
				continue
			}
			op, _ := msg.MetaGet("db2_operation")
			t.Logf("CDC event [%s]: %s", op, b)
			received = append(received, string(b))
		}
		return nil
	}))

	stream, err := streamBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())

	streamDone := make(chan error, 1)
	go func() {
		streamDone <- stream.Run(ctx)
	}()
	t.Cleanup(func() {
		if err := stream.StopWithin(10 * time.Second); err != nil {
			t.Logf("stream stop: %v", err)
		}
		if err := <-streamDone; err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("stream error: %v", err)
		}
	})

	// Let streaming stabilize (snapshot_mode=never so no initial events).
	time.Sleep(5 * time.Second)

	receivedMu.Lock()
	initialCount := len(received)
	receivedMu.Unlock()
	t.Logf("events before signal: %d", initialCount)

	// Insert execute-snapshot signal.
	db.MustExecContext(ctx,
		`INSERT INTO DB2INST1.CDC_SIGNALS (ID, TYPE, DATA) VALUES ('sig-1', 'execute-snapshot', '{"data-collections":["DB2INST1.INC_SNAP_TEST"]}')`)

	// Wait for the re-snapshot to deliver the 2 pre-existing rows.
	assert.Eventually(t, func() bool {
		receivedMu.Lock()
		defer receivedMu.Unlock()
		return len(received)-initialCount >= 2
	}, 2*time.Minute, 500*time.Millisecond,
		"incremental snapshot: expected at least 2 events after execute-snapshot signal")
}

// TestIntegrationDB2CDCIncrementalSnapshotResumeAfterRestart verifies that the
// incremental snapshot engine resumes from a per-chunk checkpoint after a
// connector restart rather than re-delivering all rows from the beginning.
//
// Algorithm under test:
//  1. Insert 20 rows into INC_RESUME_TEST (chunkSize=4 → 5 chunks)
//  2. Start connector; trigger snapshot; wait until ≥2 chunks processed (≥8 op="r")
//  3. Stop the connector (simulating a restart)
//  4. Start a NEW connector instance with the same checkpoint table
//  5. Verify all 20 rows appear as op="r" and no row ID appears more than once
func TestIntegrationDB2CDCIncrementalSnapshotResumeAfterRestart(t *testing.T) {
	integration.CheckSkip(t)

	db := db2test.SetupTest(t)
	ctx := t.Context()

	const (
		srcTable  = "INC_RESUME_TEST"
		schema    = "DB2INST1"
		rowCount  = 20
		chunkSize = 4
	)

	_, _ = db.ExecContext(ctx, `DROP TABLE ASNCDC."CDC_DB2INST1_INC_RESUME_TEST"`)
	_, _ = db.ExecContext(ctx, `DELETE FROM ASNCDC.IBMSNAP_PRUNCNTL WHERE SOURCE_TABLE='INC_RESUME_TEST'`)
	_, _ = db.ExecContext(ctx, `DELETE FROM ASNCDC.IBMSNAP_REGISTER WHERE SOURCE_TABLE='INC_RESUME_TEST'`)
	_, _ = db.ExecContext(ctx, `DROP TABLE DB2INST1.INC_RESUME_TEST`)
	_, _ = db.ExecContext(ctx, `DROP TABLE DB2INST1.INC_RESUME_CHECKPOINT`)
	_, _ = db.ExecContext(ctx, `DROP TABLE DB2INST1.INC_RESUME_SIGNALS`)

	db.MustExecContext(ctx, `CREATE TABLE DB2INST1.INC_RESUME_TEST (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR(100))`)
	for i := 1; i <= rowCount; i++ {
		db.MustExecContext(ctx, `INSERT INTO DB2INST1.INC_RESUME_TEST (ID, NAME) VALUES (?, ?)`, i, fmt.Sprintf("row-%d", i))
	}
	db.EnableASNCDC(schema, []string{srcTable})

	connectorYAML := fmt.Sprintf(`
db2_cdc:
  dsn: %q
  schema: "DB2INST1"
  tables: ["INC_RESUME_TEST"]
  snapshot_mode: never
  snapshot_max_batch_size: %d
  poll_batch_size: 100
  stream_backoff_interval: 300ms
  checkpoint_cache_table_name: "DB2INST1.INC_RESUME_CHECKPOINT"
  signal_table: "DB2INST1.INC_RESUME_SIGNALS"
`, db.DSN, chunkSize)

	// snapshotByID tracks how many times each row ID was delivered as op="r".
	snapshotByID := make(map[int]int)
	var mu sync.Mutex

	buildStream := func(t *testing.T) *service.Stream {
		t.Helper()
		sb := service.NewStreamBuilder()
		require.NoError(t, sb.AddInputYAML(connectorYAML))
		require.NoError(t, sb.AddBatchConsumerFunc(func(_ context.Context, batch service.MessageBatch) error {
			mu.Lock()
			defer mu.Unlock()
			for _, msg := range batch {
				b, _ := msg.AsBytes()
				var env map[string]any
				if err := json.Unmarshal(b, &env); err != nil {
					continue
				}
				if env["op"] != "r" {
					continue
				}
				after, _ := env["after"].(map[string]any)
				if after == nil {
					continue
				}
				id := parseIntField(after["ID"])
				snapshotByID[id]++
			}
			return nil
		}))
		stream, err := sb.Build()
		require.NoError(t, err)
		license.InjectTestService(stream.Resources())
		return stream
	}

	// Phase 1: start connector, trigger snapshot, wait for ≥2 chunks worth of rows.
	stream1 := buildStream(t)
	done1 := make(chan error, 1)
	go func() { done1 <- stream1.Run(ctx) }()

	time.Sleep(5 * time.Second) // let streaming stabilise

	db.MustExecContext(ctx,
		`INSERT INTO DB2INST1.INC_RESUME_SIGNALS (ID, TYPE, DATA) VALUES ('resume-1', 'execute-snapshot', '{"data-collections":["DB2INST1.INC_RESUME_TEST"]}')`)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(snapshotByID) >= 2*chunkSize
	}, 2*time.Minute, 500*time.Millisecond,
		"expected at least %d op=r events after 2 chunks", 2*chunkSize)

	// Phase 2: stop connector (simulate crash/restart).
	require.NoError(t, stream1.StopWithin(10*time.Second))
	<-done1

	mu.Lock()
	rowsAfterPhase1 := len(snapshotByID)
	mu.Unlock()
	t.Logf("Phase 1: %d rows snapshotted before restart", rowsAfterPhase1)

	// Phase 3: start a new connector with the same checkpoint table.
	stream2 := buildStream(t)
	done2 := make(chan error, 1)
	go func() { done2 <- stream2.Run(ctx) }()
	t.Cleanup(func() {
		_ = stream2.StopWithin(10 * time.Second)
		<-done2
	})

	// Wait for all 20 rows to appear.
	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(snapshotByID) == rowCount
	}, 3*time.Minute, 500*time.Millisecond,
		"expected all %d rows after resume", rowCount)

	// Verify no duplicates — each row must appear exactly once.
	mu.Lock()
	defer mu.Unlock()
	for id, count := range snapshotByID {
		assert.Equal(t, 1, count,
			"row id=%d appeared %d times (expected exactly 1 — no duplicates across restart)", id, count)
	}
}
