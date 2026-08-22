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
		fmt.Fprintln(os.Stderr, "usage: cdc-rows-mongodb {seed|workload|exec} [flags]")
		os.Exit(2)
	}
	cmd := os.Args[1]
	switch cmd {
	case "seed":
		fs := flag.NewFlagSet("seed", flag.ExitOnError)
		tables := fs.String("tables", "orders", "comma-separated collection list")
		database := fs.String("database", "benchdb", "database name")
		rows := fs.Int64("rows", 1_000_000, "documents per collection")
		rowSize := fs.Int("row-size", 1200, "approximate document size in bytes")
		_ = fs.Parse(os.Args[2:])
		if err := seed(context.Background(), *database, strings.Split(*tables, ","), *rows, *rowSize); err != nil {
			fmt.Fprintln(os.Stderr, "seed:", err)
			os.Exit(1)
		}
	case "workload":
		fs := flag.NewFlagSet("workload", flag.ExitOnError)
		tables := fs.String("tables", "orders", "comma-separated collection list")
		database := fs.String("database", "benchdb", "database name")
		rowSize := fs.Int("row-size", 1200, "approximate document size in bytes")
		rate := fs.Int("rate", 5000, "writes per second total across collections")
		dur := fs.Duration("duration", 15*time.Minute, "total duration")
		_ = fs.Parse(os.Args[2:])
		if err := workload(context.Background(), *database, strings.Split(*tables, ","), *rowSize, *rate, *dur); err != nil {
			fmt.Fprintln(os.Stderr, "workload:", err)
			os.Exit(1)
		}
	case "exec":
		// Collection dropper used by the bench reset. There's no mongosh on the
		// runner, so the scenario's reset bash: step shells out here with the
		// mongodb_dsn terraform output and the collection to drop.
		fs := flag.NewFlagSet("exec", flag.ExitOnError)
		dsn := fs.String("dsn", "", "mongodb DSN (mongodb://host:port/?replicaSet=rs0)")
		database := fs.String("database", "benchdb", "database name")
		dropCollection := fs.String("drop-collection", "", "collection to drop")
		_ = fs.Parse(os.Args[2:])
		if err := execDrop(context.Background(), *dsn, *database, *dropCollection); err != nil {
			fmt.Fprintln(os.Stderr, "exec:", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintln(os.Stderr, "unknown subcommand:", cmd)
		os.Exit(2)
	}
}
