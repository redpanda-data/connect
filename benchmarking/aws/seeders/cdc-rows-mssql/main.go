// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: cdc-rows-mssql {seed|workload|reset|exec} [flags]")
		os.Exit(2)
	}
	cmd := os.Args[1]
	switch cmd {
	case "seed":
		fs := flag.NewFlagSet("seed", flag.ExitOnError)
		tables := fs.String("tables", "orders", "comma-separated table list")
		rows := fs.Int64("rows", 1_000_000, "rows per table")
		rowSize := fs.Int("row-size", 1200, "approximate row size in bytes")
		// Capture-job knobs. Defaults here (not the SQL Server defaults) are what
		// keep the capture job from being the bench's hidden ceiling — see the
		// long comment on tuneCaptureJob in sql.go before changing them.
		maxTrans := fs.Int("capture-maxtrans", 5000, "sp_cdc_change_job @maxtrans")
		maxScans := fs.Int("capture-maxscans", 100, "sp_cdc_change_job @maxscans")
		pollInterval := fs.Int("capture-polling-interval", 1, "sp_cdc_change_job @pollinginterval, seconds")
		_ = fs.Parse(os.Args[2:])
		job := captureJob{maxTrans: *maxTrans, maxScans: *maxScans, pollInterval: *pollInterval}
		if err := seed(context.Background(), strings.Split(*tables, ","), *rows, *rowSize, job); err != nil {
			fmt.Fprintln(os.Stderr, "seed:", err)
			os.Exit(1)
		}
	case "workload":
		fs := flag.NewFlagSet("workload", flag.ExitOnError)
		tables := fs.String("tables", "orders", "comma-separated table list")
		rowSize := fs.Int("row-size", 1200, "approximate row size in bytes")
		rate := fs.Int("rate", 5000, "writes per second total across tables")
		dur := fs.Duration("duration", 15*time.Minute, "total duration")
		// Matches cdc-rows-oracle: 32 continuous workers, split evenly across
		// tables, so one slow table can't starve the others.
		workers := fs.Int("workers", 32, "concurrent insert workers, split evenly across tables")
		_ = fs.Parse(os.Args[2:])
		if err := workload(context.Background(), strings.Split(*tables, ","), *rowSize, *rate, *dur, *workers); err != nil {
			fmt.Fprintln(os.Stderr, "workload:", err)
			os.Exit(1)
		}
	case "reset":
		// Per-sweep-point reset, invoked from the scenario's `reset:` bash step.
		// SQL Server refuses TRUNCATE on a CDC-enabled table, so this is a
		// three-step dance (disable CDC -> truncate -> re-enable CDC) rather
		// than the single `sql:` step postgres and mysql get away with.
		fs := flag.NewFlagSet("reset", flag.ExitOnError)
		dsn := fs.String("dsn", "", "SQL Server DSN for the bench database")
		tables := fs.String("tables", "orders", "comma-separated table list")
		maxTrans := fs.Int("capture-maxtrans", 5000, "sp_cdc_change_job @maxtrans")
		maxScans := fs.Int("capture-maxscans", 100, "sp_cdc_change_job @maxscans")
		pollInterval := fs.Int("capture-polling-interval", 1, "sp_cdc_change_job @pollinginterval, seconds")
		_ = fs.Parse(os.Args[2:])
		job := captureJob{maxTrans: *maxTrans, maxScans: *maxScans, pollInterval: *pollInterval}
		if err := reset(context.Background(), *dsn, strings.Split(*tables, ","), job); err != nil {
			fmt.Fprintln(os.Stderr, "reset:", err)
			os.Exit(1)
		}
	case "exec":
		// Single-statement escape hatch. There is no sqlcmd on the bench runner
		// image, so ad-hoc SQL during debugging goes through here.
		fs := flag.NewFlagSet("exec", flag.ExitOnError)
		dsn := fs.String("dsn", "", "SQL Server DSN (sqlserver://user:pass@host:port?database=db)")
		query := fs.String("sql", "", "single SQL statement to execute")
		_ = fs.Parse(os.Args[2:])
		if err := execSQL(context.Background(), *dsn, *query); err != nil {
			fmt.Fprintln(os.Stderr, "exec:", err)
			os.Exit(1)
		}
	case "query":
		// SELECT counterpart of exec: runs a query and prints rows as TSV.
		// exec can only execute — it discards result sets — which made the
		// frozen-capture-job incidents undiagnosable from the bench host:
		// there was no way to read sys.dm_cdc_errors or
		// sys.dm_cdc_log_scan_sessions back. This closes that gap.
		fs := flag.NewFlagSet("query", flag.ExitOnError)
		dsn := fs.String("dsn", "", "SQL Server DSN (sqlserver://user:pass@host:port?database=db)")
		query := fs.String("sql", "", "single SELECT to run; rows print as TSV with a header")
		_ = fs.Parse(os.Args[2:])
		if err := querySQL(context.Background(), *dsn, *query); err != nil {
			fmt.Fprintln(os.Stderr, "query:", err)
			os.Exit(1)
		}
	case "diag-cdc":
		// One-shot CDC health snapshot: capture-job config + state, log-scan
		// session progress, recent CDC errors, and max_lsn. This is the bundle
		// to run whenever the liveness gate reports a frozen max_lsn, so "slow
		// vs wedged" is answered by data instead of another hypothesis.
		fs := flag.NewFlagSet("diag-cdc", flag.ExitOnError)
		dsn := fs.String("dsn", "", "SQL Server DSN for the bench database")
		_ = fs.Parse(os.Args[2:])
		if err := diagCDC(context.Background(), *dsn); err != nil {
			fmt.Fprintln(os.Stderr, "diag-cdc:", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintln(os.Stderr, "unknown subcommand:", cmd)
		os.Exit(2)
	}
}
