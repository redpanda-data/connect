// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/sijms/go-ora/v2"
)

func openDB(maxConns int) (*sql.DB, error) {
	return openDBWithDSN(os.Getenv("ORACLE_DSN"), maxConns)
}

func openDBWithDSN(dsn string, maxConns int) (*sql.DB, error) {
	db, err := sql.Open("oracle", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	return db, nil
}

// execSQL opens a short-lived connection with an explicit DSN and runs a single
// statement. Used by the bench reset (TRUNCATE) via the `exec` subcommand.
func execSQL(ctx context.Context, dsn, query string) error {
	if dsn == "" || query == "" {
		return fmt.Errorf("exec requires both --dsn and --sql")
	}
	db, err := openDBWithDSN(dsn, 1)
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("%s: %w", query, err)
	}
	return nil
}

func seed(ctx context.Context, tables []string, rows int64, rowSize int) error {
	db, err := openDB(16)
	if err != nil {
		return err
	}
	defer db.Close()

	// One-time RDS Oracle LogMiner setup (supplemental logging + privileges).
	// Idempotent — safe to re-run.
	if err := setupRDS(ctx, db); err != nil {
		return err
	}

	for _, table := range tables {
		if err := ensureTable(ctx, db, table); err != nil {
			return err
		}
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

// setupRDS enables the prerequisites LogMiner-based CDC needs on RDS Oracle.
// RDS blocks direct `ALTER DATABASE ...`, so database-level supplemental logging
// and SYS-object access go through the rdsadmin.rdsadmin_util package, and the
// privilege set mirrors the AWS DMS "Oracle source" prerequisites (the same
// redo/LogMiner views both Connect's oracledb_cdc and Debezium read).
//
// Every statement is wrapped so a re-run (or an already-granted privilege) is a
// no-op: procedure calls get a `BEGIN ...; EXCEPTION WHEN OTHERS THEN NULL; END;`
// and GRANT DDL runs via EXECUTE IMMEDIATE inside the same guard.
func setupRDS(ctx context.Context, db *sql.DB) error {
	// grantee is the connecting user (RDS master, e.g. BENCH). It comes from our
	// own terraform-generated DSN — alphanumeric, trusted input — so interpolating
	// it into the statements below is safe.
	g := grantee()

	// PL/SQL procedure call, exceptions swallowed.
	plsql := func(body string) string {
		return "BEGIN " + body + "; EXCEPTION WHEN OTHERS THEN NULL; END;"
	}
	// GRANT (DDL) is not allowed directly inside a PL/SQL block — wrap in
	// EXECUTE IMMEDIATE, exceptions swallowed.
	ddl := func(stmt string) string {
		return "BEGIN EXECUTE IMMEDIATE '" + stmt + "'; EXCEPTION WHEN OTHERS THEN NULL; END;"
	}

	type step struct{ label, sql string }
	steps := []step{
		{"archivelog retention 24h", plsql("rdsadmin.rdsadmin_util.set_configuration('archivelog retention hours', 24)")},
		{"supplemental logging (minimal)", plsql("rdsadmin.rdsadmin_util.alter_supplemental_logging(p_action => 'ADD')")},
		{"supplemental logging (primary key)", plsql("rdsadmin.rdsadmin_util.alter_supplemental_logging(p_action => 'ADD', p_type => 'PRIMARY KEY')")},
		{"grant select any transaction", ddl("GRANT SELECT ANY TRANSACTION TO " + g)},
		{"grant select any table", ddl("GRANT SELECT ANY TABLE TO " + g)},
		{"grant logmining", ddl("GRANT LOGMINING TO " + g)},
		{"grant execute dbms_logmnr", ddl("GRANT EXECUTE ON SYS.DBMS_LOGMNR TO " + g)},
	}
	// SYS dynamic-performance views the connector reads, granted via rdsadmin.
	for _, obj := range []string{
		"V_$DATABASE", "V_$LOG", "V_$LOGFILE", "V_$ARCHIVED_LOG",
		"V_$ARCHIVE_DEST_STATUS", "V_$LOGMNR_CONTENTS", "V_$LOGMNR_LOGS",
		"V_$THREAD", "V_$PARAMETER", "V_$NLS_PARAMETERS", "V_$TRANSACTION",
		"V_$TIMEZONE_NAMES", "V_$STANDBY_LOG",
	} {
		steps = append(steps, step{
			"grant_sys_object " + obj,
			plsql(fmt.Sprintf("rdsadmin.rdsadmin_util.grant_sys_object(p_obj_name => '%s', p_grantee => '%s', p_privilege => 'SELECT')", obj, g)),
		})
	}
	steps = append(steps, step{
		"grant_sys_object DBMS_LOGMNR (execute)",
		plsql(fmt.Sprintf("rdsadmin.rdsadmin_util.grant_sys_object(p_obj_name => 'DBMS_LOGMNR', p_grantee => '%s', p_privilege => 'EXECUTE')", g)),
	})

	for _, s := range steps {
		if _, err := db.ExecContext(ctx, s.sql); err != nil {
			// The blocks swallow Oracle-side errors; a Go-side error here means
			// the connection/statement itself failed — surface it.
			return fmt.Errorf("rds setup %q: %w", s.label, err)
		}
		fmt.Printf("rds setup: %s\n", s.label)
	}
	return nil
}

func grantee() string {
	if u, err := url.Parse(os.Getenv("ORACLE_DSN")); err == nil && u.User != nil {
		if name := u.User.Username(); name != "" {
			return strings.ToUpper(name)
		}
	}
	return "BENCH"
}

func ensureTable(ctx context.Context, db *sql.DB, table string) error {
	// Oracle has no DROP TABLE IF EXISTS; tolerate ORA-00942 (table not found).
	dropIfExists := fmt.Sprintf(
		`BEGIN EXECUTE IMMEDIATE 'DROP TABLE %s PURGE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;`,
		table,
	)
	// CACHE 100000 on the IDENTITY sequence: the default cache (20) forces a
	// recursive SEQ$ dictionary update every 20 ids, which serializes high-rate
	// batch inserts (16 workers contend on one sequence). A large cache reserves
	// ids in bulk from memory so the workload can saturate the write path. The
	// only cost is larger id gaps after a DB restart, harmless for a benchmark.
	create := fmt.Sprintf(`CREATE TABLE %s (
		id          NUMBER GENERATED ALWAYS AS IDENTITY (CACHE 100000) PRIMARY KEY,
		created_at  TIMESTAMP(6) DEFAULT SYSTIMESTAMP NOT NULL,
		payload     VARCHAR2(4000) NOT NULL
	)`, table)
	// Per-table supplemental logging. The connecting user owns the table, so it
	// can ALTER it directly (no rdsadmin needed). The log group survives TRUNCATE,
	// so reset between sweep points doesn't have to re-enable it.
	supplemental := fmt.Sprintf(`ALTER TABLE %s ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS`, table)

	for _, s := range []string{dropIfExists, create, supplemental} {
		if _, err := db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("%s: %w", s, err)
		}
	}
	return nil
}

// bulkInsert uses go-ora ARRAY binding: passing a []string for the single bind
// variable inserts len(slice) rows in one round-trip. go-ora's driver implements
// driver.NamedValueChecker, so the slice passes through database/sql without the
// usual "unsupported type" rejection. created_at is filled by the column DEFAULT
// and id by the IDENTITY, so only payload is bound.
func bulkInsert(ctx context.Context, db *sql.DB, table string, rows int64, rowSize int) error {
	const (
		workers   = 16
		batchSize = 1000
	)
	rowsPerWorker := rows / workers
	if rowsPerWorker == 0 {
		// Allow rows=0 (scenario.dataset.initial_rows: 0): ensureTable already ran
		// so the table exists but stays empty.
		return nil
	}
	payload := randomPayload(rowSize)
	full := make([]string, batchSize)
	for i := range full {
		full[i] = payload
	}
	stmt := fmt.Sprintf("INSERT INTO %s (payload) VALUES (:1)", table)
	start := time.Now()

	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			done := int64(0)
			for done < rowsPerWorker {
				n := int64(batchSize)
				if rem := rowsPerWorker - done; rem < n {
					n = rem
				}
				if _, err := db.ExecContext(ctx, stmt, full[:n]); err != nil {
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
	fmt.Printf("seeded %d rows into %s in %s\n", rows, table, time.Since(start))
	return nil
}

func workload(ctx context.Context, tables []string, rowSize, rate int, dur time.Duration) error {
	const workers = 16
	db, err := openDB(workers)
	if err != nil {
		return err
	}
	defer db.Close()

	perWorkerPer100ms := rate / workers / 10
	if perWorkerPer100ms < 1 {
		perWorkerPer100ms = 1
	}
	deadline := time.Now().Add(dur)
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		workerIdx := w
		go func() {
			defer wg.Done()
			payload := randomPayload(rowSize)
			batch := make([]string, perWorkerPer100ms)
			for i := range batch {
				batch[i] = payload
			}
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			tIdx := workerIdx
			for {
				select {
				case <-ctx.Done():
					errCh <- ctx.Err()
					return
				case <-ticker.C:
					if time.Now().After(deadline) {
						errCh <- nil
						return
					}
					table := tables[tIdx%len(tables)]
					tIdx++
					stmt := fmt.Sprintf("INSERT INTO %s (payload) VALUES (:1)", table)
					if _, err := db.ExecContext(ctx, stmt, batch); err != nil {
						errCh <- err
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
			return err
		}
	}
	return nil
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
