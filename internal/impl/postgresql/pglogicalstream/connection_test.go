// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Nothing listens on port 1, so every attempt fails at the dial. That is late
// enough for these tests, which only care about what happens before it.
const unreachableDSN = "postgres://user:startup@127.0.0.1:1/db?sslmode=disable"

func TestOpenPgConnectionFromConfigAuthToken(t *testing.T) {
	t.Run("refreshes the token for every new connection", func(t *testing.T) {
		dbConf, err := pgconn.ParseConfig(unreachableDSN)
		require.NoError(t, err)

		var refreshes atomic.Int64
		db, err := openPgConnectionFromConfig(&Config{
			DBRawDSN: unreachableDSN,
			DBConfig: dbConf,
			RefreshAuthToken: func(context.Context) error {
				dbConf.Password = fmt.Sprintf("token-%d", refreshes.Add(1))
				return nil
			},
		})
		require.NoError(t, err)
		defer db.Close()

		for range 2 {
			require.Error(t, db.PingContext(t.Context()))
		}
		assert.Equal(t, int64(2), refreshes.Load())
		assert.Equal(t, "token-2", dbConf.Password)
	})

	t.Run("surfaces a failed refresh", func(t *testing.T) {
		dbConf, err := pgconn.ParseConfig(unreachableDSN)
		require.NoError(t, err)

		errRefresh := errors.New("token expired")
		db, err := openPgConnectionFromConfig(&Config{
			DBRawDSN: unreachableDSN,
			DBConfig: dbConf,
			RefreshAuthToken: func(context.Context) error {
				return errRefresh
			},
		})
		require.NoError(t, err)
		defer db.Close()

		assert.ErrorIs(t, db.PingContext(t.Context()), errRefresh)
	})

	t.Run("keeps the parsed password when no refresh is configured", func(t *testing.T) {
		dbConf, err := pgconn.ParseConfig(unreachableDSN)
		require.NoError(t, err)

		db, err := openPgConnectionFromConfig(&Config{
			DBRawDSN: unreachableDSN,
			DBConfig: dbConf,
		})
		require.NoError(t, err)
		defer db.Close()

		require.Error(t, db.PingContext(t.Context()))
		assert.Equal(t, "startup", dbConf.Password)
	})
}
