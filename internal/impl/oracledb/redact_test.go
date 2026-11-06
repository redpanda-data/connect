// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package oracledb

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/sqlredact"
)

const testSecret = "S3cretPass"

func TestBuildConnectionStringBadURLRedacted(t *testing.T) {
	_, err := buildConnectionString("oracle://u:"+testSecret+"@bad host:1521/svc", nil, nil)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), testSecret)
}

func TestOpenDBBadDSNRedacted(t *testing.T) {
	dsn := "oracle://u:" + testSecret + "@bad host:1521/svc"

	// go-ora must still echo the DSN for this test to mean anything.
	raw, err := sql.Open("oracle", dsn)
	require.NoError(t, err)
	require.ErrorContains(t, raw.PingContext(t.Context()), testSecret)
	require.NoError(t, raw.Close())

	db, err := sqlredact.Open("oracle", dsn)
	require.NoError(t, err)
	defer db.Close()

	err = db.PingContext(t.Context())
	require.Error(t, err)
	assert.NotContains(t, err.Error(), testSecret)
}
