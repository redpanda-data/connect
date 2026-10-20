// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"database/sql"
	"fmt"
	"maps"
	"regexp"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

var re = regexp.MustCompile(`^(\d+)`)

func openPgConnectionFromConfig(cfg *Config) (*sql.DB, error) {
	return openPgConnectionWithParams(cfg, nil)
}

// openPgConnectionWithParams opens a connection whose sessions start with
// params applied. They go in the startup packet, so every connection the
// pool opens carries them.
func openPgConnectionWithParams(cfg *Config, params map[string]string) (*sql.DB, error) {
	parsedCfg, err := pgxpool.ParseConfig(cfg.DBRawDSN)
	if err != nil {
		return nil, err
	}
	parsedCfg.ConnConfig.Password = cfg.DBConfig.Password
	parsedCfg.ConnConfig.TLSConfig = cfg.TLSConfig
	maps.Copy(parsedCfg.ConnConfig.RuntimeParams, params)
	return stdlib.OpenDB(*parsedCfg.ConnConfig), nil
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
