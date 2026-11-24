// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package oracledb

import (
	"context"
	"database/sql"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// ProbeCheckpointMerge exposes probeCheckpointMerge to the integration tests in
// package oracledb_test.
func ProbeCheckpointMerge(ctx context.Context, db *sql.DB, table string) error {
	tbl, err := validateCacheTableName(table)
	if err != nil {
		return err
	}
	return probeCheckpointMerge(ctx, db, tbl)
}

// NewCheckpointCache exposes newCheckpointCache to the integration tests in
// package oracledb_test.
func NewCheckpointCache(ctx context.Context, connStr, table, key string) (service.Cache, error) {
	res := service.MockResources()
	return newCheckpointCache(ctx, connStr, table, key, res.Metrics(), res.Logger())
}

// CreateUpsertStoredProc exposes createUpsertStoredProc to the integration
// tests in package oracledb_test. It creates the procedure that older versions
// wrote checkpoints with.
func CreateUpsertStoredProc(ctx context.Context, db *sql.DB, table string) error {
	tbl, err := validateCacheTableName(table)
	if err != nil {
		return err
	}
	return createUpsertStoredProc(ctx, db, tbl)
}
