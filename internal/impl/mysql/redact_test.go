// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mysql

import (
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/license"
)

func TestBadDSNRedacted(t *testing.T) {
	const secret = "S3cretValue"
	dsn := "u:p@tcp(h:3306)/db?parseTime=" + secret

	// The driver must still echo the value for this test to mean anything.
	_, err := mysql.ParseDSN(dsn)
	require.ErrorContains(t, err, secret)

	conf, err := mysqlStreamConfigSpec.ParseYAML(`
dsn: '`+dsn+`'
tables: [ t ]
checkpoint_cache: c
stream_snapshot: false
`, nil)
	require.NoError(t, err)
	res := service.MockResources()
	license.InjectTestService(res)

	_, err = newMySQLStreamInput(conf, res)
	require.ErrorContains(t, err, "error parsing mysql DSN")
	assert.NotContains(t, err.Error(), secret)
}
