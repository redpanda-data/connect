// Copyright 2024 Redpanda Data, Inc.
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
	"regexp"
	"strconv"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

var re = regexp.MustCompile(`^(\d+)`)

// Every pool refreshes the token through the same Config.DBConfig, so the
// refresh and the read that follows it have to be serialised.
var refreshAuthTokenMu sync.Mutex

func openPgConnectionFromConfig(cfg *Config) (*sql.DB, error) {
	parsedCfg, err := pgxpool.ParseConfig(cfg.DBRawDSN)
	if err != nil {
		return nil, err
	}
	parsedCfg.ConnConfig.Password = cfg.DBConfig.Password
	parsedCfg.ConnConfig.TLSConfig = cfg.TLSConfig
	return stdlib.OpenDB(*parsedCfg.ConnConfig, stdlib.OptionBeforeConnect(
		func(ctx context.Context, connCfg *pgx.ConnConfig) error {
			if cfg.RefreshAuthToken == nil {
				return nil
			}
			// IAM tokens expire long before the pipeline does, so rebuild the
			// password for every new connection rather than reusing the one
			// captured when the pool was opened.
			refreshAuthTokenMu.Lock()
			defer refreshAuthTokenMu.Unlock()
			if err := cfg.RefreshAuthToken(ctx); err != nil {
				return err
			}
			connCfg.Password = cfg.DBConfig.Password
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
