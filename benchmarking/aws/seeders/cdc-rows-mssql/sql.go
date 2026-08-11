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

	mssql "github.com/microsoft/go-mssqldb"
)

// Rows per bulk-copy batch, i.e. rows per transaction.
//
// No longer bounded by SQL Server's 1000-row VALUES-constructor limit (bulk copy
// has no such cap), but kept at 1000 because it sets the TRANSACTION rate the CDC
// capture job has to keep up with: at 150K rows/sec this is ~150 transactions/sec,
// comfortably inside the tuned @maxtrans/@maxscans/@pollinginterval budget. Raise
// it and each transaction gets larger; lower it and the capture job's per-cycle
// transaction ceiling starts to matter.
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

// querySQL runs a SELECT and prints the result set as TSV with a header row.
// NULLs print as "NULL"; []byte columns (LSNs) print as hex.
func querySQL(ctx context.Context, dsn, query string) error {
	if dsn == "" || query == "" {
		return fmt.Errorf("query requires both --dsn and --sql")
	}
	db, err := openDB(dsn, 1)
	if err != nil {
		return err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("%s: %w", query, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	fmt.Println(strings.Join(cols, "\t"))
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		out := make([]string, len(cols))
		for i, v := range vals {
			switch x := v.(type) {
			case nil:
				out[i] = "NULL"
			case []byte:
				out[i] = fmt.Sprintf("%x", x)
			case time.Time:
				out[i] = x.UTC().Format(time.RFC3339)
			default:
				out[i] = fmt.Sprintf("%v", x)
			}
		}
		fmt.Println(strings.Join(out, "\t"))
	}
	return rows.Err()
}

var diagSections = []struct{ label, q string }{
	{"max_lsn", "SELECT sys.fn_cdc_get_max_lsn() AS max_lsn"},
	{"capture job config (msdb)", "SELECT job_type, maxtrans, maxscans, continuous, pollinginterval FROM msdb.dbo.cdc_jobs"},
	{"log scan sessions (latest 5)", `SELECT TOP 5 session_id, start_time, end_time, duration,
			scan_phase, error_count, tran_count, latency,
			empty_scan_count, failed_sessions_count
		FROM sys.dm_cdc_log_scan_sessions ORDER BY start_time DESC`},
	{"cdc errors (latest 5)", `SELECT TOP 5 entry_time, error_number, error_severity, error_message
		FROM sys.dm_cdc_errors ORDER BY entry_time DESC`},
	{"change_tables", "SELECT capture_instance, create_date, start_lsn FROM cdc.change_tables"},
	{"log space", "SELECT total_log_size_in_bytes/1048576 AS log_mb, used_log_space_in_bytes/1048576 AS used_mb, used_log_space_in_percent FROM sys.dm_db_log_space_usage"},
}

// diagCDC prints a one-shot CDC health snapshot. Run it whenever the liveness
// gate reports a frozen max_lsn: together these distinguish a capture job that
// is SLOW (scan sessions present and advancing, no errors) from one that is
// WEDGED (no sessions, or errors in sys.dm_cdc_errors).
func diagCDC(ctx context.Context, dsn string) error {
	if dsn == "" {
		return errors.New("diag-cdc requires --dsn")
	}
	db, err := openDB(dsn, 1)
	if err != nil {
		return err
	}
	defer db.Close()

	for _, s := range diagSections {
		fmt.Printf("\n### %s\n", s.label)
		if err := querySection(ctx, db, s.q); err != nil {
			// Sections are independent; a permissions failure on one DMV must
			// not hide the rest.
			fmt.Printf("(error: %v)\n", err)
		}
	}
	return nil
}

func querySection(ctx context.Context, db *sql.DB, query string) error {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	fmt.Println(strings.Join(cols, "\t"))
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	n := 0
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		out := make([]string, len(cols))
		for i, v := range vals {
			switch x := v.(type) {
			case nil:
				out[i] = "NULL"
			case []byte:
				out[i] = fmt.Sprintf("%x", x)
			case time.Time:
				out[i] = x.UTC().Format(time.RFC3339)
			default:
				out[i] = fmt.Sprintf("%v", x)
			}
		}
		fmt.Println(strings.Join(out, "\t"))
		n++
	}
	if n == 0 {
		fmt.Println("(no rows)")
	}
	return rows.Err()
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
		// Ground truth BEFORE the drop, same as the reset path. When sweeps run
		// as separate per-engine invocations, the next run's seed (not a reset)
		// is what destroys the previous point's table — without this line the
		// last point of every invocation loses its offered-load count and its
		// capture ratio can't be computed. Cost of learning this: run A's
		// 2 vCPU connect point (2026-08-11).
		var prevRows int64
		if err := db.QueryRowContext(ctx,
			fmt.Sprintf("SELECT COUNT_BIG(*) FROM [%s].[%s]", schema, table)).Scan(&prevRows); err == nil {
			fmt.Printf("[groundtruth] %s.%s committed %d rows during the previous point\n", schema, table, prevRows)
		}
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

// execWithDeadlockRetry runs a statement, retrying while SQL Server picks it as a
// deadlock victim (error 1205).
//
// The CDC enable/disable procedures are the reason this exists.
// sp_cdc_disable_table drops cdc.<instance>_CT, and the capture job may be
// writing to that very table at the same time — especially when it is working
// through a backlog, which after a bulk-copy workload it always is. SQL Server
// resolves the conflict by killing one side and telling it to rerun; observed on
// 2026-08-10 as a seed failure deep inside
// sp_cdc_disable_table -> sp_cdc_drop_change_table_objects -> drop table.
//
// Retrying is the documented remedy, not a workaround: error 1205's own text is
// "Rerun the transaction."
func execWithDeadlockRetry(ctx context.Context, db *sql.DB, label, stmt string) error {
	const attempts = 5
	var lastErr error
	for i := 0; i < attempts; i++ {
		_, lastErr = db.ExecContext(ctx, stmt)
		if lastErr == nil {
			return nil
		}
		// The deadlock does NOT surface as error number 1205 here. The CDC
		// procedures wrap it twice — sp_cdc_disable_table fails with 22837,
		// whose message embeds 22933, whose message embeds the actual 1205 —
		// and go-mssqldb reports the OUTER number. Verified live 2026-08-11: a
		// reset under load produced Number=22837 with "deadlocked on lock
		// resources ... chosen as the deadlock victim" three levels deep in the
		// text. So match the number when it does come through directly, and the
		// message text for the wrapped case.
		var me mssql.Error
		isDeadlock := (errors.As(lastErr, &me) && me.Number == 1205) ||
			strings.Contains(lastErr.Error(), "deadlock victim") ||
			strings.Contains(lastErr.Error(), "error returned was 1205")
		if !isDeadlock {
			return fmt.Errorf("%s: %w", label, lastErr)
		}
		wait := time.Duration(i+1) * 2 * time.Second
		fmt.Printf("%s: deadlock victim (1205), attempt %d/%d, retrying in %s\n",
			label, i+1, attempts, wait)
		time.Sleep(wait)
	}
	return fmt.Errorf("%s: still deadlocking after %d attempts: %w", label, attempts, lastErr)
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
	if err := execWithDeadlockRetry(ctx, db,
		fmt.Sprintf("sp_cdc_enable_table %s.%s", schema, table), q); err != nil {
		return err
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
	return execWithDeadlockRetry(ctx, db,
		fmt.Sprintf("sp_cdc_disable_table %s.%s", schema, table), q)
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
	// PROGRESS-AWARE, not a fixed attempt count. The old 5x60s budget failed a
	// healthy database on 2026-08-10: after an ~18M-row point the capture job
	// spends minutes grinding through transaction-log backlog before the new
	// capture instance's max_lsn can move, and during that grind a fixed budget
	// reads as "stopped". Local diagnosis (diag-cdc, 2026-08-11) settled slow vs
	// wedged: sys.dm_cdc_log_scan_sessions showed sessions completing with
	// tran_count in the hundreds and zero rows in sys.dm_cdc_errors while
	// max_lsn appeared frozen.
	//
	// So: keep waiting as long as the scanner is demonstrably making progress
	// (recent scan-session end_time), up to a generous overall cap. Fail fast
	// only when there is no scan activity at all — that is the genuinely-stopped
	// case the gate exists for. On failure, dump the full diagnostic bundle so
	// the AWS log self-diagnoses instead of needing a live instance.
	const (
		totalBudget   = 15 * time.Minute
		perAttemptTTL = 60 * time.Second
	)
	deadline := time.Now().Add(totalBudget)
	var lastErr error
	for attempt := 1; ; attempt++ {
		// Tolerated: "already running" and "pending request" are both fine.
		if _, err := db.ExecContext(ctx, "EXEC sys.sp_cdc_start_job @job_type = N'capture'"); err != nil {
			fmt.Printf("sp_cdc_start_job (capture), attempt %d: %v (tolerated)\n", attempt, err)
		}
		if lastErr = waitForCaptureJobLive(ctx, db, table, perAttemptTTL); lastErr == nil {
			return nil
		}
		if time.Now().After(deadline) {
			fmt.Println("capture job liveness budget exhausted; diagnostic snapshot follows:")
			_ = diagCDCWithDB(ctx, db)
			return fmt.Errorf("after %s: %w", totalBudget, lastErr)
		}
		if active, detail := captureScanActive(ctx, db); active {
			fmt.Printf("capture job is scanning through backlog (%s); waiting on (attempt %d)\n", detail, attempt)
			continue
		} else if detail != "" {
			fmt.Printf("no recent scan activity (%s); retrying start (attempt %d)\n", detail, attempt)
		}
	}
}

// captureScanActive reports whether the CDC log scanner shows recent progress.
// "Recent" is a scan session whose end_time is within the last two minutes (in
// continuous mode the job updates the open session's end_time as it scans, so a
// live scanner always has a fresh one). Errors reading the DMV are reported in
// detail but treated as "unknown", not "active" — a permissions gap must not
// convert the gate into an infinite wait.
func captureScanActive(ctx context.Context, db *sql.DB) (bool, string) {
	var end sql.NullTime
	var trans, empties sql.NullInt64
	err := db.QueryRowContext(ctx, `SELECT TOP 1 end_time, tran_count, empty_scan_count
		FROM sys.dm_cdc_log_scan_sessions ORDER BY start_time DESC`).Scan(&end, &trans, &empties)
	if err != nil {
		return false, fmt.Sprintf("dm_cdc_log_scan_sessions unreadable: %v", err)
	}
	if !end.Valid {
		// Session open with no end_time yet: the scanner is mid-scan.
		return true, "scan session in progress"
	}
	age := time.Since(end.Time)
	detail := fmt.Sprintf("last scan ended %s ago, tran_count=%d, empty_scans=%d",
		age.Round(time.Second), trans.Int64, empties.Int64)
	return age < 2*time.Minute, detail
}

// diagCDCWithDB is diagCDC against an already-open handle, for use inside the
// liveness failure path.
func diagCDCWithDB(ctx context.Context, db *sql.DB) error {
	for _, s := range diagSections {
		fmt.Printf("\n### %s\n", s.label)
		if err := querySection(ctx, db, s.q); err != nil {
			fmt.Printf("(error: %v)\n", err)
		}
	}
	return nil
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

// bulkInsertBatch writes n rows via the TDS bulk-copy protocol (mssql.CopyIn).
//
// WHY BULK COPY, measured not assumed. Parameterized multi-row INSERT tops out
// at ~11K rows/sec and NOTHING about its shape moves that number. A local
// A/B (2026-08-10, SQL Server 2022 container) over batch sizes 25/100/250/1000
// and both NVARCHAR and explicit VARCHAR parameters landed every variant in the
// 10.5-11.3K band, while bulk copy ran 4-7x faster in the same harness:
//
//	parameterized, any shape   ~10.9-11.3K rows/sec
//	bulk copy (CopyIn)          44.9K-81K rows/sec
//
// That ~11K figure is the same ceiling the AWS runs hit on a db.r5.2xlarge with
// a 16 vCPU load generator, so it is a property of the parameterized INSERT
// path, not of RDS, instance size, or the client. It also explains two earlier
// refuted fixes (prepared statements; load-gen 2 -> 16 vCPU): both were
// changing things that were never the constraint.
//
// An earlier version of this file rejected CopyIn as "not the OLTP write shape
// this bench represents". That was wrong twice over. Every other seeder already
// uses its engine's fast path - go-ora array binding for oracle, multi-row
// VALUES for postgres and mysql - so bulk copy is the CONSISTENT choice, not a
// deviation. And the load generator's job is to saturate the reader under test;
// it is an instrument, not a workload model.
//
// CDC capture is verified, not assumed: after a bulk-copy-only run the base
// table held 449,000 rows and cdc.dbo_t_bench_CT held 449,000 - captured 1:1.
// Column defaults apply to columns outside the copy list, so id (IDENTITY) and
// created_at (DEFAULT SYSUTCDATETIME()) fill in as they do for INSERT.
//
// TABLOCK is deliberately NOT set: it would speed a single loader but serialize
// the concurrent workers this generator relies on.
func bulkInsertBatch(ctx context.Context, db *sql.DB, table string, pool []string, cursor *int, n int) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, mssql.CopyIn(schema+"."+table, mssql.BulkOptions{}, "payload"))
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("prepare bulk copy into %s.%s: %w", schema, table, err)
	}
	for i := 0; i < n; i++ {
		if _, err := stmt.ExecContext(ctx, pool[*cursor%len(pool)]); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("bulk copy row: %w", err)
		}
		*cursor++
	}
	// The argument-less Exec flushes the batch; Close finalises it. Both must
	// happen before Commit or the rows are silently discarded.
	if _, err := stmt.ExecContext(ctx); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("flush bulk copy: %w", err)
	}
	if err := stmt.Close(); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("close bulk copy: %w", err)
	}
	return tx.Commit()
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
	start := time.Now()

	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			// Stagger each worker's start position in the pool so concurrent
			// batches don't all carry identical rows.
			cursor := w * 131
			done := int64(0)
			for done < rowsPerWorker {
				n := int64(batchSize)
				if rem := rowsPerWorker - done; rem < n {
					n = rem
				}
				if err := bulkInsertBatch(ctx, db, table, pool, &cursor, int(n)); err != nil {
					errCh <- err
					return
				}
				done += n
			}
			errCh <- nil
		}(w)
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
		for w := 0; w < workersPerTable; w++ {
			wg.Add(1)
			go func(tIdx, w int, table string) {
				defer wg.Done()
				pool := randomPayloadPool(rowSize, payloadPoolSize)
				cursor := w * 131
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
					if n > batchSize {
						n = batchSize
					}
					if err := bulkInsertBatch(ctx, db, table, pool, &cursor, int(n)); err != nil {
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
			}(tIdx, w, table)
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
