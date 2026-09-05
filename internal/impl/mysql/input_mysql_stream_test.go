// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseMySQLConfigSetsParseTime(t *testing.T) {
	cfg, err := parseMySQLConfig("user:password@tcp(localhost:3306)/db")
	require.NoError(t, err)
	require.True(t, cfg.ParseTime, "ParseTime must be enabled to deserialise binlog timestamps")
}

func TestApplyIAMDriverDefaults(t *testing.T) {
	cfg, err := parseMySQLConfig("user@tcp(mydb.rds.amazonaws.com:3306)/db")
	require.NoError(t, err)

	applyIAMDriverDefaults(cfg, false)
	require.False(t, cfg.AllowCleartextPasswords, "must not enable cleartext passwords when IAM auth is off")

	applyIAMDriverDefaults(cfg, true)
	require.True(t, cfg.AllowCleartextPasswords, "must enable cleartext passwords so the IAM auth token can be sent to MySQL")
}
