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
	"database/sql"
	"fmt"
	"os"

	_ "github.com/SAP/go-hdb/driver"
)

func main() {
	dsn := os.Getenv("HANA_DSN")
	if dsn == "" {
		fmt.Fprintf(os.Stderr, "HANA_DSN environment variable is required\n")
		os.Exit(1)
	}
	schema := os.Getenv("HANA_SCHEMA")
	if schema == "" {
		fmt.Fprintf(os.Stderr, "HANA_SCHEMA environment variable is required\n")
		os.Exit(1)
	}
	table := fmt.Sprintf(`"%s"."BENCH_WRITES"`, schema)

	db, err := sql.Open("hdb", dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := db.PingContext(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "ping: %v\n", err)
		os.Exit(1)
	}

	_, _ = db.ExecContext(context.Background(), `DROP TABLE `+table)
	createSQL := `CREATE COLUMN TABLE ` + table + ` (` +
		`ID BIGINT,` +
		`CATEGORY NVARCHAR(50),` +
		`VALUE DOUBLE,` +
		`TS TIMESTAMP` +
		`)`
	if _, err := db.ExecContext(context.Background(), createSQL); err != nil {
		fmt.Fprintf(os.Stderr, "create table: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Table %s ready (empty)\n", table)
}
