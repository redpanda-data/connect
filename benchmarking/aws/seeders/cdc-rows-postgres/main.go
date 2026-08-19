// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

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
		fmt.Fprintln(os.Stderr, "usage: cdc-rows-postgres {seed|workload} [flags]")
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
	default:
		fmt.Fprintln(os.Stderr, "unknown subcommand:", cmd)
		os.Exit(2)
	}
}
