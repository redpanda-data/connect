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
	"regexp"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
)

func openPgConnectionFromConfig(dbConf *pgconn.Config) (*sql.DB, error) {
	conf, _ := pgx.ParseConfig("")
	delete(dbConf.RuntimeParams, "replication")
	conf.Config = *dbConf
	connStr := stdlib.RegisterConnConfig(conf)

	return sql.Open("pgx", connStr)
}

func getPostgresVersion(connConfig *pgconn.Config) (int, error) {
	conn, err := openPgConnectionFromConfig(connConfig)
	if err != nil {
		return 0, fmt.Errorf("failed to connect to the database: %w", err)
	}

	var versionString string
	err = conn.QueryRow("SHOW server_version").Scan(&versionString)
	if err != nil {
		return 0, fmt.Errorf("failed to execute query: %w", err)
	}

	// Extract the major version number
	re := regexp.MustCompile(`^(\d+)`)
	match := re.FindStringSubmatch(versionString)
	if len(match) < 2 {
		return 0, fmt.Errorf("failed to parse version string: %s", versionString)
	}

	majorVersion, err := strconv.Atoi(match[1])
	if err != nil {
		return 0, fmt.Errorf("failed to convert version to integer: %w", err)
	}

	return majorVersion, nil
}
