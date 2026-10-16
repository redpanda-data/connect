// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package mssqlserver

import (
	"database/sql"
	"testing"

	_ "github.com/microsoft/go-mssqldb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/sqlredact"
)

func TestOpenDBBadDSNRedacted(t *testing.T) {
	const secret = "S3cretPass"
	dsn := "sqlserver://sa:" + secret + "@bad host:1433?database=db"

	// Guard that the driver still echoes the DSN, otherwise this test proves
	// nothing.
	raw, err := sql.Open("mssql", dsn)
	if err == nil {
		err = raw.PingContext(t.Context())
		_ = raw.Close()
	}
	require.ErrorContains(t, err, secret)

	db, err := sqlredact.Open("mssql", dsn)
	if err == nil {
		defer db.Close()
		err = db.PingContext(t.Context())
	}
	require.Error(t, err)
	assert.NotContains(t, err.Error(), secret)
}
