// Copyright 2026 Redpanda Data, Inc.
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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestOpenPgConnectionBadDSNRedacted(t *testing.T) {
	const secret = "S3cretPass"
	_, err := openPgConnectionFromConfig(&Config{
		DBRawDSN: "postgres://user@localhost:5432/db?password=" + secret + "&sslmode=bogus",
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "sslmode")
	assert.NotContains(t, err.Error(), secret)
}

// Nothing listens on port 1, so every connection attempt fails at the dial.
// The password is settled before the dial, which is all these tests look at.
const unreachableDSN = "postgres://user:startup@127.0.0.1:1/db?sslmode=disable"

func TestOpenPgConnectionRefreshesAuthToken(t *testing.T) {
	t.Run("reuses a fresh token for new connections", func(t *testing.T) {
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
		assert.Equal(t, int64(1), refreshes.Load())
		assert.Equal(t, "token-1", dbConf.Password)
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

	t.Run("keeps the configured password without a refresh", func(t *testing.T) {
		dbConf, err := pgconn.ParseConfig(unreachableDSN)
		require.NoError(t, err)

		db, err := openPgConnectionFromConfig(&Config{
			DBRawDSN: unreachableDSN,
			DBConfig: dbConf,
		})
		require.NoError(t, err)
		defer db.Close()

		err = db.PingContext(t.Context())
		require.Error(t, err)
		assert.NotErrorIs(t, err, context.Canceled)
		assert.Equal(t, "startup", dbConf.Password)
	})

	// Run with -race: the pools, the input's Connect and the replication
	// connection all go through the same DBConfig.
	t.Run("serialises concurrent refreshes", func(t *testing.T) {
		dbConf, err := pgconn.ParseConfig(unreachableDSN)
		require.NoError(t, err)

		var refreshes atomic.Int64
		cfg := &Config{
			DBRawDSN: unreachableDSN,
			DBConfig: dbConf,
			RefreshAuthToken: func(context.Context) error {
				dbConf.Password = fmt.Sprintf("token-%d", refreshes.Add(1))
				return nil
			},
		}
		db, err := openPgConnectionFromConfig(cfg)
		require.NoError(t, err)
		defer db.Close()

		var wg sync.WaitGroup
		for range 4 {
			wg.Go(func() { _ = db.PingContext(t.Context()) })
			wg.Go(func() { _ = cfg.RefreshAuth(t.Context()) })
			wg.Go(func() { _ = cfg.dbConfigCopy() })
		}
		wg.Wait()
		// Each RefreshAuth builds a token. The pings reuse one, and build
		// one only if they run before every RefreshAuth.
		assert.GreaterOrEqual(t, refreshes.Load(), int64(4))
		assert.LessOrEqual(t, refreshes.Load(), int64(5))
	})
}

func TestFreshPasswordRebuildsAgedToken(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var (
			refreshes  int
			errRefresh error
		)
		cfg := &Config{
			DBConfig: &pgconn.Config{},
			RefreshAuthToken: func(context.Context) error {
				if errRefresh != nil {
					return errRefresh
				}
				refreshes++
				return nil
			},
		}
		cfg.DBConfig.Password = "startup"

		build := func() {
			t.Helper()
			_, err := cfg.freshPassword(t.Context())
			require.NoError(t, err)
		}

		build()
		assert.Equal(t, 1, refreshes, "no token yet")

		time.Sleep(authTokenMaxAge - time.Second)
		build()
		assert.Equal(t, 1, refreshes, "token still fresh")

		time.Sleep(time.Second)
		build()
		assert.Equal(t, 2, refreshes, "token aged")

		require.NoError(t, cfg.RefreshAuth(t.Context()))
		assert.Equal(t, 3, refreshes, "RefreshAuth always builds")
		time.Sleep(authTokenMaxAge - time.Second)
		build()
		assert.Equal(t, 3, refreshes, "RefreshAuth resets the age")

		// A failed build keeps the old age, so the next connection retries.
		time.Sleep(time.Second)
		errRefresh = errors.New("sts unavailable")
		_, err := cfg.freshPassword(t.Context())
		require.ErrorIs(t, err, errRefresh)
		errRefresh = nil
		build()
		assert.Equal(t, 4, refreshes)
	})
}

func TestFreshPasswordLockIsPerConfig(t *testing.T) {
	release := make(chan struct{})
	building := make(chan struct{})
	slow := &Config{
		DBConfig: &pgconn.Config{},
		RefreshAuthToken: func(context.Context) error {
			close(building)
			<-release
			return nil
		},
	}
	go func() { _, _ = slow.freshPassword(context.Background()) }()
	<-building
	defer close(release)

	for _, cfg := range []*Config{
		{DBConfig: &pgconn.Config{Password: "pw"}},
		{DBConfig: &pgconn.Config{Password: "pw"}, RefreshAuthToken: func(context.Context) error { return nil }},
	} {
		password, err := cfg.freshPassword(t.Context())
		require.NoError(t, err)
		assert.Equal(t, "pw", password)
	}
}

// An IAM auth token stops being accepted about 15 minutes after it is built,
// while the pools of the heartbeat, the monitor and the snapshotter live as
// long as the stream. Rotating the role's password stands in for the expiry:
// the password the pool was opened with is refused from then on.
func TestIntegrationOpenPgConnectionSurvivesTokenExpiry(t *testing.T) {
	integration.CheckSkip(t)

	replURL := createDockerInstance(t)

	dsn := strings.ReplaceAll(replURL, " replication=database", "")
	dbConf, err := pgconn.ParseConfig(dsn)
	require.NoError(t, err)

	var token atomic.Value
	token.Store("secret")
	db, err := openPgConnectionFromConfig(&Config{
		DBRawDSN: dsn,
		DBConfig: dbConf,
		RefreshAuthToken: func(context.Context) error {
			dbConf.Password = token.Load().(string)
			return nil
		},
		// Every token counts as aged, as it would be after authTokenMaxAge.
		authTokenMaxAge: time.Nanosecond,
	})
	require.NoError(t, err)
	defer db.Close()
	// No idle connections, so each statement authenticates again.
	db.SetMaxIdleConns(0)

	_, err = db.ExecContext(t.Context(), `ALTER ROLE user_name PASSWORD 'rotated'`)
	require.NoError(t, err)
	token.Store("rotated")

	var one int
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT 1`).Scan(&one))
	assert.Equal(t, 1, one)
}
