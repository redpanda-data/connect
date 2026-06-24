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
		fmt.Fprintln(os.Stderr, "usage: cdc-rows-oracle {seed|workload|exec} [flags]")
		os.Exit(2)
	}
	cmd := os.Args[1]
	switch cmd {
	case "seed":
		fs := flag.NewFlagSet("seed", flag.ExitOnError)
		tables := fs.String("tables", "orders", "comma-separated table list")
		rows := fs.Int64("rows", 1_000_000, "rows per table")
		rowSize := fs.Int("row-size", 1200, "approximate row size in bytes")
		_ = fs.Parse(os.Args[2:])
		if err := seed(context.Background(), strings.Split(*tables, ","), *rows, *rowSize); err != nil {
			fmt.Fprintln(os.Stderr, "seed:", err)
			os.Exit(1)
		}
	case "workload":
		fs := flag.NewFlagSet("workload", flag.ExitOnError)
		tables := fs.String("tables", "orders", "comma-separated table list")
		rowSize := fs.Int("row-size", 1200, "approximate row size in bytes")
		rate := fs.Int("rate", 5000, "writes per second total across tables")
		dur := fs.Duration("duration", 15*time.Minute, "total duration")
		_ = fs.Parse(os.Args[2:])
		if err := workload(context.Background(), strings.Split(*tables, ","), *rowSize, *rate, *dur); err != nil {
			fmt.Fprintln(os.Stderr, "workload:", err)
			os.Exit(1)
		}
	case "exec":
		// Single-statement runner used by the bench reset. Oracle has no
		// psql/mysql CLI on the runner, so the scenario's reset bash: step shells
		// out here with the oracle_dsn terraform output and a TRUNCATE.
		fs := flag.NewFlagSet("exec", flag.ExitOnError)
		dsn := fs.String("dsn", "", "oracle DSN (oracle://user:pass@host:port/service)")
		query := fs.String("sql", "", "single SQL statement to execute")
		_ = fs.Parse(os.Args[2:])
		if err := execSQL(context.Background(), *dsn, *query); err != nil {
			fmt.Fprintln(os.Stderr, "exec:", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintln(os.Stderr, "unknown subcommand:", cmd)
		os.Exit(2)
	}
}
