// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/microsoft/go-mssqldb"
)

// SQL Server's row-constructor limit: an INSERT ... VALUES (...),(...) batch may
// carry at most 1000 rows.
const batchSize = 1000

// schema is where the bench table lives. Both the Connect input's `include`
// regexes and Debezium's table.include.list are written as dbo.<table>, so this
// is not independently configurable without editing those too.
const schema = "dbo"

// captureJob holds the sp_cdc_change_job knobs for the CDC capture job.
type captureJob struct {
	maxTrans     int
	maxScans     int
	pollInterval int
}

func openDB(dsn string, maxConns int) (*sql.DB, error) {
	if dsn == "" {
		return nil, errors.New("empty DSN")
	}
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	return db, nil
}

// execSQL opens a short-lived connection and runs a single statement. Escape
// hatch for debugging; there is no sqlcmd on the bench hosts.
func execSQL(ctx context.Context, dsn, query string) error {
	if dsn == "" || query == "" {
		return fmt.Errorf("exec requires both --dsn and --sql")
	}
	db, err := openDB(dsn, 1)
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("%s: %w", query, err)
	}
	return nil
}

// dbNameFromDSN pulls the `database` query parameter out of a go-mssqldb DSN.
func dbNameFromDSN(dsn string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("parse DSN: %w", err)
	}
	name := u.Query().Get("database")
	if name == "" {
		return "", errors.New("DSN has no `database` parameter")
	}
	return name, nil
}

func seed(ctx context.Context, tables []string, rows int64, rowSize int, job captureJob) error {
	benchDSN := os.Getenv("MSSQL_DSN")
	masterDSN := os.Getenv("MSSQL_MASTER_DSN")
	if masterDSN == "" {
		return errors.New("MSSQL_MASTER_DSN is required: RDS creates a SQL Server instance with no application database, so the bench database has to be created before MSSQL_DSN is connectable")
	}
	dbName, err := dbNameFromDSN(benchDSN)
	if err != nil {
		return err
	}

	// Phase 1 (master context): create the application database and turn on CDC
	// at the database level.
	master, err := openDB(masterDSN, 2)
	if err != nil {
		return err
	}
	defer master.Close()
	if err := ensureDatabase(ctx, master, dbName); err != nil {
		return err
	}
	if err := enableCDCOnDatabase(ctx, master, benchDSN, dbName); err != nil {
		return err
	}

	// Phase 2 (bench database context): tables, per-table CDC, capture-job tuning.
	db, err := openDB(benchDSN, 16)
	if err != nil {
		return err
	}
	defer db.Close()

	// ORDER MATTERS: tables first, then the capture job.
	//
	// rds_cdc_enable_db (and sp_cdc_enable_db) turn CDC on at the database level
	// but do NOT populate the capture job's row in msdb — that happens when the
	// first table is enabled. Tuning before any table exists fails with "The
	// Change Data Capture job table containing job information for database
	// 'benchdb' cannot be found in the msdb system database."
	for _, table := range tables {
		if err := ensureTable(ctx, db, table); err != nil {
			return err
		}
	}
	if err := tuneCaptureJob(ctx, db, job); err != nil {
		return err
	}
	for _, table := range tables {
		if err := waitForCaptureInstance(ctx, db, table); err != nil {
			return err
		}
	}
	if err := ensureCaptureJobRunning(ctx, db, tables[0]); err != nil {
		return err
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(tables))
	for _, table := range tables {
		wg.Add(1)
		go func(t string) {
			defer wg.Done()
			errCh <- bulkInsert(ctx, db, t, rows, rowSize)
		}(table)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

// reset is the per-sweep-point cleanup. SQL Server rejects TRUNCATE on a table
// that is enabled for CDC ("Cannot truncate table ... because it is published
// for replication or enabled for Change Data Capture"), so the table's CDC has
// to come off and go back on around the truncate.
//
// Re-enabling is not just bookkeeping — it is what makes the scenario's
// `stream_snapshot: false` safe. A fresh capture instance starts at the current
// LSN, so the change table each engine reads is empty at the start of every
// sweep point. Drop the re-enable (or reuse a long-lived capture instance) and
// Connect starts from the capture instance's original start_lsn and replays
// every change ever captured — the same silent "reader never catches up to the
// live workload" failure that produced a bogus Oracle ceiling.
func reset(ctx context.Context, dsn string, tables []string, job captureJob) error {
	if dsn == "" {
		return errors.New("reset requires --dsn")
	}
	db, err := openDB(dsn, 2)
	if err != nil {
		return err
	}
	defer db.Close()

	for _, table := range tables {
		// Ground truth for the PREVIOUS sweep point, logged before we destroy it.
		// Three counters are supposed to agree over a point — rows the generator
		// says it wrote, rows Connect says it read, and bytes the broker says it
		// received — and when they disagree there is currently no way to tell
		// which one is lying. This is the cheapest possible tiebreaker: the row
		// count the database actually committed.
		var rows int64
		if err := db.QueryRowContext(ctx,
			fmt.Sprintf("SELECT COUNT_BIG(*) FROM [%s].[%s]", schema, table)).Scan(&rows); err != nil {
			// Table may not exist on the very first reset — not fatal.
			fmt.Printf("[groundtruth] %s.%s row count unavailable: %v\n", schema, table, err)
		} else {
			fmt.Printf("[groundtruth] %s.%s committed %d rows during the previous point\n", schema, table, rows)
		}

		if err := disableCDCOnTable(ctx, db, table); err != nil {
			return err
		}
		q := fmt.Sprintf("TRUNCATE TABLE [%s].[%s]", schema, table)
		if _, err := db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("%s: %w", q, err)
		}
		if err := enableCDCOnTable(ctx, db, table); err != nil {
			return err
		}
	}
	// DO NOT stop/start the capture job here.
	//
	// This used to call tuneCaptureJob, for the cosmetic benefit of a fresh
	// log-scan session per sweep point. That was actively harmful: the job IS
	// running by this point (the seed started it), so sp_cdc_stop_job succeeds,
	// and the immediately following sp_cdc_start_job is then refused by the Agent
	// with "the job already has a pending request" — leaving the capture job
	// STOPPED for the whole sweep point. With no capture job, the change tables
	// never fill, and both engines report 0 MB/s with no error anywhere. That is
	// exactly what happened to Debezium on 2026-08-07: max_lsn stayed frozen at
	// its seed-time value for the entire run.
	//
	// The job's tuning is applied once at seed time and persists, so there is
	// nothing to re-apply. All the reset owes is a guarantee that the job is
	// running before the point starts.
	if err := ensureCaptureJobRunning(ctx, db, tables[0]); err != nil {
		return err
	}
	for _, table := range tables {
		if err := waitForCaptureInstance(ctx, db, table); err != nil {
			return err
		}
	}
	return ensureCaptureJobRunning(ctx, db, tables[0])
}

func ensureDatabase(ctx context.Context, master *sql.DB, dbName string) error {
	var n int
	if err := master.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sys.databases WHERE name = @p1", dbName).Scan(&n); err != nil {
		return fmt.Errorf("check database %q: %w", dbName, err)
	}
	if n > 0 {
		fmt.Printf("database %s already exists\n", dbName)
		return nil
	}
	// dbName comes from our own terraform output (alphanumeric), and CREATE
	// DATABASE takes no parameters, so it is interpolated.
	if _, err := master.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE [%s]", dbName)); err != nil {
		return fmt.Errorf("create database %q: %w", dbName, err)
	}
	fmt.Printf("created database %s\n", dbName)
	return nil
}

// enableCDCOnDatabase turns on database-level CDC.
//
// On RDS this MUST go through msdb.dbo.rds_cdc_enable_db: the native
// sys.sp_cdc_enable_db requires sysadmin, which RDS does not grant to the
// master user. The native path is kept as a fallback so the same seeder works
// against a plain SQL Server container during local development.
func enableCDCOnDatabase(ctx context.Context, master *sql.DB, benchDSN, dbName string) error {
	var enabled bool
	if err := master.QueryRowContext(ctx,
		"SELECT is_cdc_enabled FROM sys.databases WHERE name = @p1", dbName).Scan(&enabled); err != nil {
		return fmt.Errorf("check is_cdc_enabled for %q: %w", dbName, err)
	}
	if enabled {
		fmt.Printf("cdc already enabled on database %s\n", dbName)
		return nil
	}

	var hasRDSWrapper bool
	if err := master.QueryRowContext(ctx,
		"SELECT CASE WHEN OBJECT_ID('msdb.dbo.rds_cdc_enable_db') IS NULL THEN 0 ELSE 1 END").Scan(&hasRDSWrapper); err != nil {
		return fmt.Errorf("probe for rds_cdc_enable_db: %w", err)
	}

	if hasRDSWrapper {
		if _, err := master.ExecContext(ctx,
			fmt.Sprintf("EXEC msdb.dbo.rds_cdc_enable_db '%s'", dbName)); err != nil {
			return fmt.Errorf("rds_cdc_enable_db %q: %w", dbName, err)
		}
		fmt.Printf("cdc enabled on database %s via rds_cdc_enable_db\n", dbName)
	} else {
		// Non-RDS: sp_cdc_enable_db is database-scoped, so it needs a connection
		// in the bench database's context.
		bench, err := openDB(benchDSN, 1)
		if err != nil {
			return err
		}
		defer bench.Close()
		if _, err := bench.ExecContext(ctx, "EXEC sys.sp_cdc_enable_db"); err != nil {
			return fmt.Errorf("sp_cdc_enable_db %q: %w", dbName, err)
		}
		fmt.Printf("cdc enabled on database %s via sp_cdc_enable_db\n", dbName)
	}

	// Enabling is synchronous, but the CDC jobs are created by the Agent right
	// after; poll so a fast follow-on sp_cdc_change_job doesn't race it.
	deadline := time.Now().Add(2 * time.Minute)
	for {
		if err := master.QueryRowContext(ctx,
			"SELECT is_cdc_enabled FROM sys.databases WHERE name = @p1", dbName).Scan(&enabled); err != nil {
			return err
		}
		if enabled {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for is_cdc_enabled on %q", dbName)
		}
		time.Sleep(2 * time.Second)
	}
}

// tuneCaptureJob raises the capture job's per-cycle work limits.
//
// THIS IS THE MOST IMPORTANT KNOB IN THIS BENCH. Neither Connect's
// microsoft_sql_server_cdc nor Debezium reads the transaction log directly —
// both tail the cdc.<schema>_<table>_CT change tables, which are populated by
// SQL Server's own capture job. That job does @maxscans passes of at most
// @maxtrans transactions each, then sleeps @pollinginterval seconds. At the
// stock 10 / 500 / 5 that is a hard ceiling of 10*500/5 = 1000 transactions per
// second on how fast changes can even APPEAR to a reader, no matter how much
// CPU either engine has. Leaving the defaults in place would measure SQL
// Server's capture job and label the number a connector ceiling.
//
// 100 / 5000 / 1 lifts that to ~500K transactions/sec, far above anything the
// load generator produces (it batches 1000 rows per INSERT, so 150K rows/sec is
// ~150 transactions/sec). To bench the capture job itself, sweep these back
// down via matrix.arms rather than editing the defaults here.
// Callers must have enabled CDC on at least one table first — see the ORDER
// MATTERS note in seed().
func tuneCaptureJob(ctx context.Context, db *sql.DB, job captureJob) error {
	// sp_cdc_add_job creates the capture job with these settings outright, which
	// covers the case where enabling the first table did not leave one behind
	// (RDS's rds_cdc_enable_db wrapper does not create it). It errors when a
	// capture job already exists, which is the normal case on the reset path —
	// sp_cdc_change_job below then enforces the settings either way.
	add := fmt.Sprintf(
		"EXEC sys.sp_cdc_add_job @job_type = N'capture', @maxtrans = %d, @maxscans = %d, @continuous = 1, @pollinginterval = %d",
		job.maxTrans, job.maxScans, job.pollInterval,
	)
	if _, err := db.ExecContext(ctx, add); err != nil {
		fmt.Printf("sp_cdc_add_job (capture): %v (ignored — job already exists)\n", err)
	}
	q := fmt.Sprintf(
		"EXEC sys.sp_cdc_change_job @job_type = N'capture', @maxtrans = %d, @maxscans = %d, @pollinginterval = %d, @continuous = 1",
		job.maxTrans, job.maxScans, job.pollInterval,
	)
	if _, err := db.ExecContext(ctx, q); err != nil {
		return fmt.Errorf("sp_cdc_change_job: %w", err)
	}
	// Changes only take effect when the job restarts. Stopping fails when the
	// job isn't currently running, which is expected on the first call.
	// Both of these are advisory, not assertions.
	//
	// SQL Server Agent processes job start/stop ASYNCHRONOUSLY, so this pair
	// races whatever the Agent is already doing: stop fails with "the job is not
	// currently running" and start fails with "the job already has a pending
	// request from User rdsa" — the latter meaning a start is already queued,
	// i.e. exactly the state we want. Treating either as fatal makes the seed
	// fail on a healthy database.
	//
	// waitForCaptureInstance is the real readiness gate: a start_lsn can only
	// appear once the capture job has actually scanned, so it proves what these
	// two calls merely request.
	if _, err := db.ExecContext(ctx, "EXEC sys.sp_cdc_stop_job @job_type = N'capture'"); err != nil {
		fmt.Printf("sp_cdc_stop_job (capture): %v (ignored — job was not running)\n", err)
	}
	if _, err := db.ExecContext(ctx, "EXEC sys.sp_cdc_start_job @job_type = N'capture'"); err != nil {
		fmt.Printf("sp_cdc_start_job (capture): %v (ignored — a start is already queued; waitForCaptureInstance will confirm)\n", err)
	}
	fmt.Printf("capture job tuned: maxtrans=%d maxscans=%d pollinginterval=%ds\n",
		job.maxTrans, job.maxScans, job.pollInterval)
	return nil
}

func ensureTable(ctx context.Context, db *sql.DB, table string) error {
	// A CDC-enabled table can't be dropped cleanly while its capture instances
	// exist, so take CDC off first if a previous run left it on.
	if err := disableCDCOnTable(ctx, db, table); err != nil {
		return err
	}

	qualified := fmt.Sprintf("[%s].[%s]", schema, table)
	dropIfExists := fmt.Sprintf(
		"IF OBJECT_ID('%s.%s', 'U') IS NOT NULL DROP TABLE %s", schema, table, qualified)
	// VARCHAR (not NVARCHAR): 1 byte per character, so the scenario's
	// row_size_bytes maps directly onto payload length. NVARCHAR would silently
	// double every row and halve the effective rate for a given MB/s target.
	create := fmt.Sprintf(`CREATE TABLE %s (
		id          BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
		created_at  DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME(),
		payload     VARCHAR(4000) NOT NULL
	)`, qualified)

	for _, s := range []string{dropIfExists, create} {
		if _, err := db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("%s: %w", s, err)
		}
	}
	return enableCDCOnTable(ctx, db, table)
}

func enableCDCOnTable(ctx context.Context, db *sql.DB, table string) error {
	var tracked bool
	if err := db.QueryRowContext(ctx, `
		SELECT t.is_tracked_by_cdc
		FROM sys.tables t
		JOIN sys.schemas s ON t.schema_id = s.schema_id
		WHERE s.name = @p1 AND t.name = @p2`, schema, table).Scan(&tracked); err != nil {
		return fmt.Errorf("check is_tracked_by_cdc for %s.%s: %w", schema, table, err)
	}
	if tracked {
		return nil
	}
	// @role_name = NULL: no gating role, the connecting master user can read the
	// change tables directly. @supports_net_changes = 0 skips creation of the
	// net-changes query function, which neither engine uses and which costs the
	// capture job extra work per change.
	q := fmt.Sprintf(
		"EXEC sys.sp_cdc_enable_table @source_schema = N'%s', @source_name = N'%s', @role_name = NULL, @supports_net_changes = 0",
		schema, table)
	if _, err := db.ExecContext(ctx, q); err != nil {
		return fmt.Errorf("sp_cdc_enable_table %s.%s: %w", schema, table, err)
	}
	fmt.Printf("cdc enabled on table %s.%s\n", schema, table)
	return nil
}

func disableCDCOnTable(ctx context.Context, db *sql.DB, table string) error {
	var tracked bool
	err := db.QueryRowContext(ctx, `
		SELECT t.is_tracked_by_cdc
		FROM sys.tables t
		JOIN sys.schemas s ON t.schema_id = s.schema_id
		WHERE s.name = @p1 AND t.name = @p2`, schema, table).Scan(&tracked)
	if errors.Is(err, sql.ErrNoRows) {
		// Table doesn't exist yet (first seed) — nothing to disable.
		return nil
	}
	if err != nil {
		return fmt.Errorf("check is_tracked_by_cdc for %s.%s: %w", schema, table, err)
	}
	if !tracked {
		return nil
	}
	q := fmt.Sprintf(
		"EXEC sys.sp_cdc_disable_table @source_schema = N'%s', @source_name = N'%s', @capture_instance = N'all'",
		schema, table)
	if _, err := db.ExecContext(ctx, q); err != nil {
		return fmt.Errorf("sp_cdc_disable_table %s.%s: %w", schema, table, err)
	}
	return nil
}

// waitForCaptureInstance blocks until the capture job has published a start_lsn
// for the table's capture instance.
//
// cdc.change_tables.start_lsn is NULL between sp_cdc_enable_table and the
// capture job's first scan. Connect reads exactly that column to decide where
// to begin (replication/stream.go: "SELECT TOP 1 start_lsn FROM
// cdc.change_tables WHERE capture_instance = ?"), so starting the pipeline
// inside that window is a race whose loser silently streams nothing. Blocking
// here keeps it out of the sweep.
func waitForCaptureInstance(ctx context.Context, db *sql.DB, table string) error {
	const q = `
		SELECT COUNT(*)
		FROM cdc.change_tables ct
		JOIN sys.tables t ON ct.source_object_id = t.object_id
		JOIN sys.schemas s ON t.schema_id = s.schema_id
		WHERE s.name = @p1 AND t.name = @p2 AND ct.start_lsn IS NOT NULL`
	deadline := time.Now().Add(3 * time.Minute)
	for {
		var n int
		if err := db.QueryRowContext(ctx, q, schema, table).Scan(&n); err != nil {
			return fmt.Errorf("poll start_lsn for %s.%s: %w", schema, table, err)
		}
		if n > 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for a start_lsn on capture instance for %s.%s "+
				"(is the SQL Server Agent capture job running? check sys.dm_cdc_errors)", schema, table)
		}
		time.Sleep(time.Second)
	}
}

// waitForCaptureJobLive proves the capture job is ACTUALLY SCANNING, by writing
// one sentinel row and requiring sys.fn_cdc_get_max_lsn() to advance past its
// pre-insert value.
//
// This exists because waitForCaptureInstance is not sufficient, and the 2026-08-07
// smoke proved it the expensive way. That function polls
// cdc.change_tables.start_lsn for non-NULL, which shows the job scanned ONCE at
// some point — not that it is running now. sp_cdc_stop_job/sp_cdc_start_job are
// advisory (the Agent handles them asynchronously and start legitimately fails
// with "already has a pending request"), so nothing else asserts liveness.
//
// The failure mode this catches is silent and total: Debezium's streaming loop
// gates on fn_cdc_get_max_lsn() advancing, so against a stopped capture job it
// waits forever, logs no error, and reports 0 MB/s. Connect degrades the same
// way. A stopped capture job must fail the seed loudly, not produce a zero.
//
// One sentinel row per point is symmetric across engines and negligible against
// a 15-minute workload.
// ensureCaptureJobRunning guarantees the capture job is scanning, retrying the
// start across the whole window rather than firing one advisory start and hoping.
//
// SQL Server Agent handles start/stop asynchronously, so a single
// sp_cdc_start_job can be refused ("the job already has a pending request") and
// a single liveness probe can lose the race. Both are transient. What is NOT
// acceptable is proceeding with a stopped job, because that reports 0 MB/s for
// both engines with no error — so this keeps trying and fails the run loudly if
// it cannot prove liveness.
//
// Starting an already-running job is harmless, which is why this only ever
// starts and never stops.
func ensureCaptureJobRunning(ctx context.Context, db *sql.DB, table string) error {
	const (
		attempts      = 5
		perAttemptTTL = 60 * time.Second
	)
	var lastErr error
	for i := 0; i < attempts; i++ {
		// Tolerated: "already running" and "pending request" are both fine.
		if _, err := db.ExecContext(ctx, "EXEC sys.sp_cdc_start_job @job_type = N'capture'"); err != nil {
			fmt.Printf("sp_cdc_start_job (capture), attempt %d: %v (tolerated)\n", i+1, err)
		}
		if lastErr = waitForCaptureJobLive(ctx, db, table, perAttemptTTL); lastErr == nil {
			return nil
		}
		fmt.Printf("capture job not live yet after attempt %d: %v\n", i+1, lastErr)
	}
	return lastErr
}

func waitForCaptureJobLive(ctx context.Context, db *sql.DB, table string, timeout time.Duration) error {
	var before []byte
	if err := db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&before); err != nil {
		return fmt.Errorf("read max_lsn before sentinel: %w", err)
	}

	stmt := fmt.Sprintf("INSERT INTO [%s].[%s] (payload) VALUES (@p1)", schema, table)
	if _, err := db.ExecContext(ctx, stmt, "capture-job-liveness-sentinel"); err != nil {
		return fmt.Errorf("insert liveness sentinel into %s.%s: %w", schema, table, err)
	}

	deadline := time.Now().Add(timeout)
	for {
		var now []byte
		if err := db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&now); err != nil {
			return fmt.Errorf("poll max_lsn after sentinel: %w", err)
		}
		if len(now) > 0 && !bytes.Equal(now, before) {
			fmt.Printf("capture job live: max_lsn advanced to %x\n", now)
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("capture job is NOT scanning: sys.fn_cdc_get_max_lsn() did not advance "+
				"within %s of writing a sentinel row to %s.%s (max_lsn=%x). The SQL Server Agent capture "+
				"job cdc.<db>_capture is most likely stopped — check sys.dm_cdc_errors and "+
				"sys.dm_cdc_log_scan_sessions. Proceeding would report 0 MB/s for both engines with no error",
				timeout, schema, table, now)
		}
		time.Sleep(2 * time.Second)
	}
}

// payloadPoolSize is the number of distinct random payloads cycled per worker.
//
// Ported from cdc-rows-mongodb, which is the only seeder that had this right.
// Reusing ONE identical payload for every row (what this seeder and the
// postgres/mysql/oracle ones used to do) makes each producer batch trivially
// compressible, and that single fact was the entire 11-17x gap between
// Connect's self-reported throughput and the broker's byte counters: on the
// 2026-08-07 SQL Server run the wire carried 94 bytes per 1200-byte row. Mongo's
// calibration note is the authority on the size — 4096 comfortably exceeds one
// compression batch, where 1024 still left ~1.5x compressible.
const payloadPoolSize = 4096

// randomPayloadPool builds n distinct random payloads of ~size bytes.
func randomPayloadPool(size, n int) []string {
	pool := make([]string, n)
	for i := range pool {
		pool[i] = randomPayload(size)
	}
	return pool
}

// insertStmt builds a multi-row INSERT with ONE PARAMETER PER ROW:
// VALUES (@p1),(@p2),...,(@pn)
//
// It used to bind a single parameter n times, which was only possible because
// every row carried an identical payload. Distinct payloads need distinct
// parameters. At batchSize=1000 that is 1000 parameters, comfortably inside SQL
// Server's 2100-per-statement cap.
//
// The TDS bulk-copy path (mssql.CopyIn) stays deliberately unused: bulk load is
// not the OLTP write shape this bench represents.
func insertStmt(table string, n int) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "INSERT INTO [%s].[%s] (payload) VALUES ", schema, table)
	for i := 0; i < n; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		fmt.Fprintf(&sb, "(@p%d)", i+1)
	}
	return sb.String()
}

// batchArgs slices batchSize distinct payloads out of the pool, advancing the
// caller's cursor. Returns them as []any for Stmt.ExecContext.
func batchArgs(pool []string, cursor *int, n int) []any {
	args := make([]any, n)
	for i := 0; i < n; i++ {
		args[i] = pool[*cursor%len(pool)]
		*cursor++
	}
	return args
}

func bulkInsert(ctx context.Context, db *sql.DB, table string, rows int64, rowSize int) error {
	const workers = 16
	rowsPerWorker := rows / workers
	if rowsPerWorker == 0 {
		// Allow rows=0 (scenario.dataset.initial_rows: 0): ensureTable already
		// ran so the table exists but stays empty.
		return nil
	}
	pool := randomPayloadPool(rowSize, payloadPoolSize)
	fullStmt := insertStmt(table, batchSize)
	start := time.Now()

	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Prepare once per worker: see the note on the workload path. A
			// 1000-row VALUES list re-sent as ad-hoc SQL on every insert is what
			// held the generator to ~8% of target.
			prepared, err := db.PrepareContext(ctx, fullStmt)
			if err != nil {
				errCh <- fmt.Errorf("prepare seed insert: %w", err)
				return
			}
			defer prepared.Close()

			cursor := 0
			done := int64(0)
			for done < rowsPerWorker {
				n := int64(batchSize)
				if rem := rowsPerWorker - done; rem < n {
					n = rem
					// Odd-sized tail: ad-hoc, runs at most once per worker.
					if _, err := db.ExecContext(ctx, insertStmt(table, int(n)),
						batchArgs(pool, &cursor, int(n))...); err != nil {
						errCh <- err
						return
					}
					done += n
					continue
				}
				if _, err := prepared.ExecContext(ctx, batchArgs(pool, &cursor, int(n))...); err != nil {
					errCh <- err
					return
				}
				done += n
			}
			errCh <- nil
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	fmt.Printf("seeded %d rows into %s.%s in %s\n", rows, schema, table, time.Since(start))
	return nil
}

// workload drives a sustained insert rate across `tables` for `dur`.
//
// PACING MODEL (do not reintroduce a ticker here). Copied deliberately from
// cdc-rows-oracle: gating each worker on a time.Ticker and doing one blocking
// insert per tick silently caps the generator, because Ticker DROPS missed
// ticks — a worker whose insert outlasts the interval loses the intervening
// quota outright. That capped every Oracle run at ~12 MB/s regardless of the
// requested rate, and the resulting read-side plateau was misread as a
// connector ceiling for months.
//
// Instead workers loop continuously and rate is enforced by comparing rows
// actually inserted against rows owed for the elapsed time, so a slow insert is
// absorbed by the next iteration rather than discarded.
//
// Rate semantics: `rate` is rows/sec TOTAL across tables, divided evenly, with
// dedicated workers per table so one slow table cannot starve the others.
func workload(ctx context.Context, tables []string, rowSize, rate int, dur time.Duration, workers int) error {
	dsn := os.Getenv("MSSQL_DSN")
	if workers < len(tables) {
		workers = len(tables)
	}
	db, err := openDB(dsn, workers)
	if err != nil {
		return err
	}
	defer db.Close()

	workersPerTable := workers / len(tables)
	perWorkerRate := float64(rate) / float64(len(tables)) / float64(workersPerTable)

	// Achieved-rate instrumentation: a read-side number must never be quoted
	// without the load that produced it.
	counters := make([]atomic.Int64, len(tables))
	reportCtx, stopReport := context.WithCancel(ctx)
	defer stopReport()
	go reportLoadRate(reportCtx, tables, counters, rowSize, rate)

	deadline := time.Now().Add(dur)
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for tIdx, table := range tables {
		fullStmt := insertStmt(table, batchSize)
		for w := 0; w < workersPerTable; w++ {
			wg.Add(1)
			go func(tIdx int, table, fullStmt string) {
				defer wg.Done()
				// PREPARE ONCE. The 2026-08-07 smoke delivered only ~13K of
				// 150K rows/sec — ~8% of target — and the arithmetic put the cost
				// in per-statement latency: ~2.4s for a 1000-row, 1.2 MB batch.
				// The cause was re-sending a ~7 KB ad-hoc VALUES list through
				// sp_executesql on every insert, forcing SQL Server to re-parse a
				// 1000-row table value constructor each time. Preparing hands the
				// statement over once (sp_prepare) then executes by handle, paying
				// parse and wire cost once per worker instead of once per batch.
				prepared, err := db.PrepareContext(ctx, fullStmt)
				if err != nil {
					errCh <- fmt.Errorf("prepare workload insert: %w", err)
					return
				}
				defer prepared.Close()

				// Distinct payloads (built once) so change events aren't
				// trivially compressible — see payloadPoolSize.
				pool := randomPayloadPool(rowSize, payloadPoolSize)
				cursor := 0
				start := time.Now()
				var done int64
				for {
					if ctx.Err() != nil {
						errCh <- ctx.Err()
						return
					}
					if time.Now().After(deadline) {
						errCh <- nil
						return
					}
					// Rows this worker owes for the elapsed window. Running
					// behind (the normal case under load) means no sleep at
					// all — insert back-to-back until caught up.
					owed := int64(perWorkerRate * time.Since(start).Seconds())
					if done >= owed {
						time.Sleep(10 * time.Millisecond)
						continue
					}
					n := owed - done
					if n >= batchSize {
						// Hot path: full batch through the prepared handle.
						if _, err := prepared.ExecContext(ctx, batchArgs(pool, &cursor, batchSize)...); err != nil {
							if ctx.Err() != nil {
								errCh <- nil
								return
							}
							errCh <- err
							return
						}
						done += batchSize
						counters[tIdx].Add(batchSize)
						continue
					}
					// Short of a full batch: ad-hoc statement for the remainder.
					// Only reached once the worker has caught up with its quota,
					// so it stays off the hot path.
					if _, err := db.ExecContext(ctx, insertStmt(table, int(n)),
						batchArgs(pool, &cursor, int(n))...); err != nil {
						if ctx.Err() != nil {
							errCh <- nil
							return
						}
						errCh <- err
						return
					}
					done += n
					counters[tIdx].Add(n)
				}
			}(tIdx, table, fullStmt)
		}
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			return err
		}
	}
	return nil
}

// reportLoadRate logs delivered write throughput every 10s, in total and per
// table, alongside the target. The `[load]` prefix matches what the bench
// runner greps for in the load-gen output.
func reportLoadRate(ctx context.Context, tables []string, counters []atomic.Int64, rowSize, target int) {
	const interval = 10 * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	prev := make([]int64, len(tables))
	last := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			elapsed := time.Since(last).Seconds()
			last = time.Now()
			var totalRows int64
			parts := make([]string, len(tables))
			for i := range tables {
				cur := counters[i].Load()
				delta := cur - prev[i]
				prev[i] = cur
				totalRows += delta
				rps := float64(delta) / elapsed
				parts[i] = fmt.Sprintf("%s=%.0f rows/s (%.1f MB/s)",
					tables[i], rps, rps*float64(rowSize)/(1024*1024))
			}
			rps := float64(totalRows) / elapsed
			fmt.Printf("[load] delivered %.0f rows/s (%.1f MB/s) of %d rows/s target | %s\n",
				rps, rps*float64(rowSize)/(1024*1024), target, strings.Join(parts, " "))
		}
	}
}

func randomPayload(size int) string {
	b := make([]byte, (size*3)/4+1)
	_, _ = rand.Read(b)
	s := base64.StdEncoding.EncodeToString(b)
	if len(s) > size {
		s = s[:size]
	}
	return s
}
