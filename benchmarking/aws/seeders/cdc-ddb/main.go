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
		fmt.Fprintln(os.Stderr, "usage: cdc-ddb {seed|workload} [flags]")
		os.Exit(2)
	}
	cmd := os.Args[1]
	switch cmd {
	case "seed":
		fs := flag.NewFlagSet("seed", flag.ExitOnError)
		tables := fs.String("tables", "rpcn_bench_ddb_orders", "comma-separated table list")
		rows := fs.Int64("rows", 0, "rows per table (0 = no-op; scenarios use TRUNCATE-equivalent reset)")
		rowSize := fs.Int("row-size", 1024, "approximate row size in bytes")
		_ = fs.Parse(os.Args[2:])
		if err := seed(context.Background(), strings.Split(*tables, ","), *rows, *rowSize); err != nil {
			fmt.Fprintln(os.Stderr, "seed:", err)
			os.Exit(1)
		}
	case "workload":
		fs := flag.NewFlagSet("workload", flag.ExitOnError)
		tables := fs.String("tables", "rpcn_bench_ddb_orders", "comma-separated table list")
		rowSize := fs.Int("row-size", 1024, "approximate row size in bytes")
		rate := fs.Int("rate", 5000, "PutItems per second total across tables")
		dur := fs.Duration("duration", 15*time.Minute, "total duration")
		_ = fs.Parse(os.Args[2:])
		if err := workload(context.Background(), strings.Split(*tables, ","), *rowSize, *rate, *dur); err != nil {
			fmt.Fprintln(os.Stderr, "workload:", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintln(os.Stderr, "unknown subcommand:", cmd)
		os.Exit(2)
	}
}
