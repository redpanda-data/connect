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
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: json-orders seed [flags]")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "seed":
		fs := flag.NewFlagSet("seed", flag.ExitOnError)
		topic := fs.String("topic", "bench-orders", "destination topic")
		rows := fs.Int64("rows", 1_000_000, "records to produce")
		rowSize := fs.Int("row-size", 1200, "approximate record size in bytes")
		_ = fs.Parse(os.Args[2:])
		if err := seed(context.Background(), *topic, *rows, *rowSize); err != nil {
			fmt.Fprintln(os.Stderr, "seed:", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintln(os.Stderr, "unknown subcommand:", os.Args[1])
		os.Exit(2)
	}
}
