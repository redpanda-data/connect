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
		partitions := fs.Int("partitions", 16, "topic partition count")
		keySpace := fs.Int64("key-space", 0, "cap the id space so ids repeat (id = i %% key-space) for keyed upsert benches; 0 keeps ids unique")
		keyOrder := fs.String("key-order", "sequential", "arrival order of recurring ids when key-space is set: sequential (contiguous runs) or scattered (coprime-stride walk)")
		_ = fs.Parse(os.Args[2:])
		if *keyOrder != "sequential" && *keyOrder != "scattered" {
			fmt.Fprintf(os.Stderr, "seed: --key-order must be sequential or scattered (got %q)\n", *keyOrder)
			os.Exit(2)
		}
		if err := seed(context.Background(), *topic, *rows, *rowSize, *partitions, *keySpace, *keyOrder); err != nil {
			fmt.Fprintln(os.Stderr, "seed:", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintln(os.Stderr, "unknown subcommand:", os.Args[1])
		os.Exit(2)
	}
}
