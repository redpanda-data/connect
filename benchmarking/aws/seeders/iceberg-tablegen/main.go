// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/apache/iceberg-go/catalog"

	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/catalogx"
)

func main() {
	catalogURI := flag.String("catalog-uri", "", "Iceberg REST catalog URI (e.g. Glue REST endpoint)")
	warehouse := flag.String("warehouse", "", "catalog warehouse (AWS account id for Glue)")
	region := flag.String("region", "us-east-2", "AWS region for SigV4 signing")
	namespace := flag.String("namespace", "bench", "catalog namespace / Glue database")
	tableName := flag.String("table", "", "table name to create")
	location := flag.String("location", "", "explicit S3 location for the table")
	flag.Parse()

	if *catalogURI == "" || *warehouse == "" || *tableName == "" || *location == "" {
		fmt.Fprintln(os.Stderr, "iceberg-tablegen: --catalog-uri, --warehouse, --table, --location are required")
		os.Exit(2)
	}

	ctx := context.Background()
	cl, err := catalogx.NewCatalogClient(ctx, catalogx.Config{
		URL:          *catalogURI,
		Warehouse:    *warehouse,
		AuthType:     "sigv4",
		SigV4Region:  *region,
		SigV4Service: "glue",
	}, []string{*namespace})
	if err != nil {
		fmt.Fprintln(os.Stderr, "iceberg-tablegen: catalog client:", err)
		os.Exit(1)
	}

	if exists, err := cl.CheckTableExists(ctx, *tableName); err == nil && exists {
		fmt.Printf("iceberg-tablegen: table %s.%s already exists\n", *namespace, *tableName)
		return
	}

	_, err = cl.CreateTable(ctx, *tableName, ordersSchema(), catalog.WithLocation(*location))
	if err != nil {
		if errors.Is(err, catalog.ErrTableAlreadyExists) {
			fmt.Printf("iceberg-tablegen: table %s.%s already exists (race)\n", *namespace, *tableName)
			return
		}
		fmt.Fprintln(os.Stderr, "iceberg-tablegen: create table:", err)
		os.Exit(1)
	}
	fmt.Printf("iceberg-tablegen: created %s.%s at %s\n", *namespace, *tableName, *location)
}
