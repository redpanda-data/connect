// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package oracledb

import (
	"fmt"
	"maps"
	"net/url"
	"strconv"
	"strings"

	go_ora "github.com/sijms/go-ora/v2"
)

const defaultOraclePort = 1521

// buildConnectionString parses connStr (oracle://user:password@host:port/service) supporting
// overriding of of connectio parameters.
func buildConnectionString(connStr string, overrides map[string]string) (string, error) {
	u, err := url.Parse(connStr)
	if err != nil {
		return "", fmt.Errorf("parsing url: %w", err)
	}
	if u.Scheme != "oracle" {
		return "", fmt.Errorf("unsupported connection string scheme %q: connection_string must use the oracle:// format", u.Scheme)
	}

	server := u.Hostname()

	port := defaultOraclePort
	if raw := u.Port(); raw != "" {
		if port, err = strconv.Atoi(raw); err != nil {
			return "", fmt.Errorf("parsing port %q: %w", raw, err)
		}
	}

	service := strings.TrimPrefix(u.Path, "/")

	var user, password string
	if u.User != nil {
		user = u.User.Username()
		password, _ = u.User.Password()
	}

	opts := make(map[string]string)
	for key, vals := range u.Query() {
		if len(vals) > 0 {
			opts[key] = vals[0]
		}
	}
	// if key exists, overrides wins
	maps.Copy(opts, overrides)

	return go_ora.BuildUrl(server, port, service, user, password, opts), nil
}
