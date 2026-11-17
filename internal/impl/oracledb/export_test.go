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

// IsInsufficientPrivileges exposes isInsufficientPrivileges to the integration
// tests in package oracledb_test.
var IsInsufficientPrivileges = isInsufficientPrivileges
