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
	"database/sql/driver"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/redpanda-data/benthos/v4/public/utils/redact"
)

var re = regexp.MustCompile(`^(\d+)`)

// authTokenMaxAge is how long a built auth token is reused for new
// connections. An IAM auth token is accepted for 15 minutes after it is built,
// and it is only checked when a connection authenticates, so reusing it for 10
// minutes leaves a margin while building a new one rarely.
const authTokenMaxAge = 10 * time.Minute

// RefreshAuth generates a new auth token when the config has a short lived
// one (IAM authentication) and stores it as the password of DBConfig. It does
// nothing otherwise.
func (c *Config) RefreshAuth(ctx context.Context) error {
	if c.RefreshAuthToken == nil {
		return nil
	}
	c.authMu.Lock()
	defer c.authMu.Unlock()
	return c.refreshAuthLocked(ctx)
}

func (c *Config) refreshAuthLocked(ctx context.Context) error {
	if err := c.RefreshAuthToken(ctx); err != nil {
		return err
	}
	c.authTokenBuiltAt = time.Now()
	return nil
}

// freshPassword returns the password to open a new connection with. When the
// config has an auth token, it builds a new one first if the current one is
// older than authTokenMaxAge.
func (c *Config) freshPassword(ctx context.Context) (string, error) {
	if c.RefreshAuthToken == nil {
		return c.DBConfig.Password, nil
	}
	c.authMu.Lock()
	defer c.authMu.Unlock()
	maxAge := c.authTokenMaxAge
	if maxAge == 0 {
		maxAge = authTokenMaxAge
	}
	if c.authTokenBuiltAt.IsZero() || time.Since(c.authTokenBuiltAt) >= maxAge {
		if err := c.refreshAuthLocked(ctx); err != nil {
			return "", fmt.Errorf("refreshing auth token: %w", err)
		}
	}
	return c.DBConfig.Password, nil
}

// forgetAuthToken makes the next connection build a new auth token whatever
// the age of the current one. It is called when the server rejects the token,
// which can happen before authTokenMaxAge, for example when the temporary AWS
// credentials that signed it expire first.
func (c *Config) forgetAuthToken() {
	if c.RefreshAuthToken == nil {
		return
	}
	c.authMu.Lock()
	defer c.authMu.Unlock()
	c.authTokenBuiltAt = time.Time{}
}

// SQLSTATEs a server reports when it rejects the auth token. Password
// authentication fails with invalid_password, but RDS IAM authentication goes
// through PAM, and Postgres reports a PAM rejection as
// invalid_authorization_specification.
const (
	invalidPasswordCode          = "28P01"
	invalidAuthorizationSpecCode = "28000"
)

func isAuthRejected(err error) bool {
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	return ok && (pgErr.Code == invalidPasswordCode || pgErr.Code == invalidAuthorizationSpecCode)
}

// authResetConnector forgets the auth token of cfg when the server rejects it,
// so the pool does not keep connecting with it until it ages out.
type authResetConnector struct {
	driver.Connector
	cfg *Config
}

func (a authResetConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := a.Connector.Connect(ctx)
	if isAuthRejected(err) {
		a.cfg.forgetAuthToken()
	}
	return conn, err
}

// dbConfigCopy returns a copy of DBConfig that is safe to connect with while
// another goroutine refreshes the auth token.
func (c *Config) dbConfigCopy() *pgconn.Config {
	if c.RefreshAuthToken == nil {
		return c.DBConfig.Copy()
	}
	c.authMu.Lock()
	defer c.authMu.Unlock()
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
	connector := stdlib.GetConnector(*parsedCfg.ConnConfig, stdlib.OptionBeforeConnect(
		func(ctx context.Context, connCfg *pgx.ConnConfig) error {
			password, err := cfg.freshPassword(ctx)
			if err != nil {
				return err
			}
			connCfg.Password = password
			return nil
		},
	))
	if cfg.RefreshAuthToken != nil {
		connector = authResetConnector{Connector: connector, cfg: cfg}
	}
	return sql.OpenDB(connector), nil
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
