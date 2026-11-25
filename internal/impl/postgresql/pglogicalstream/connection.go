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
	"database/sql"
	"fmt"
	"maps"
	"regexp"
	"strconv"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/redpanda-data/benthos/v4/public/utils/redact"
)

var re = regexp.MustCompile(`^(\d+)`)

// authTokenMu serialises every refresh of the auth token with the reads of
// the password it writes. RefreshAuthToken stores the token in the shared
// Config.DBConfig, and the pools open connections from their own goroutines.
var authTokenMu sync.Mutex

// RefreshAuth generates a new auth token when the config has a short lived
// one (IAM authentication) and stores it as the password of DBConfig. It does
// nothing otherwise.
func (c *Config) RefreshAuth(ctx context.Context) error {
	authTokenMu.Lock()
	defer authTokenMu.Unlock()
	return c.refreshAuthLocked(ctx)
}

func (c *Config) refreshAuthLocked(ctx context.Context) error {
	if c.RefreshAuthToken == nil {
		return nil
	}
	return c.RefreshAuthToken(ctx)
}

// freshPassword returns the password to open a new connection with,
// refreshing the auth token first when there is one.
func (c *Config) freshPassword(ctx context.Context) (string, error) {
	authTokenMu.Lock()
	defer authTokenMu.Unlock()
	if err := c.refreshAuthLocked(ctx); err != nil {
		return "", fmt.Errorf("refreshing auth token: %w", err)
	}
	return c.DBConfig.Password, nil
}

// dbConfigCopy returns a copy of DBConfig that is safe to connect with while
// another goroutine refreshes the auth token.
func (c *Config) dbConfigCopy() *pgconn.Config {
	authTokenMu.Lock()
	defer authTokenMu.Unlock()
	return c.DBConfig.Copy()
}

func openPgConnectionFromConfig(cfg *Config) (*sql.DB, error) {
	return openPgConnectionWithParams(cfg, nil)
}

// openPgConnectionWithParams opens a connection whose sessions start with
// params applied. They go in the startup packet, so every connection the
// pool opens carries them.
func openPgConnectionWithParams(cfg *Config, params map[string]string) (*sql.DB, error) {
	parsedCfg, err := pgxpool.ParseConfig(cfg.DBRawDSN)
	if err != nil {
		return nil, redact.Error(err, redact.Conns(cfg.DBRawDSN))
	}
	parsedCfg.ConnConfig.TLSConfig = cfg.TLSConfig
	maps.Copy(parsedCfg.ConnConfig.RuntimeParams, params)
	// The pool outlives an IAM auth token (about 15 minutes), and opens new
	// physical connections whenever it needs one. Take the password when a
	// connection is opened rather than once here, or every connection opened
	// after the token expires fails to authenticate.
	return stdlib.OpenDB(*parsedCfg.ConnConfig, stdlib.OptionBeforeConnect(
		func(ctx context.Context, connCfg *pgx.ConnConfig) error {
			password, err := cfg.freshPassword(ctx)
			if err != nil {
				return err
			}
			connCfg.Password = password
			return nil
		},
	)), nil
}

func getPostgresVersion(cfg *Config) (int, error) {
	conn, err := openPgConnectionFromConfig(cfg)
	if err != nil {
		return 0, fmt.Errorf("connecting to the database: %w", err)
	}
	defer conn.Close()

	var versionString string
	if err = conn.QueryRow("SHOW server_version").Scan(&versionString); err != nil {
		return 0, fmt.Errorf("executing query: %w", err)
	}

	match := re.FindStringSubmatch(versionString)
	if len(match) < 2 {
		return 0, fmt.Errorf("parsing version string: %s", versionString)
	}

	majorVersion, err := strconv.Atoi(match[1])
	if err != nil {
		return 0, fmt.Errorf("converting version to integer: %w", err)
	}

	return majorVersion, nil
}
