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

	"github.com/redpanda-data/benthos/v4/public/service"

	go_ora "github.com/sijms/go-ora/v2"
)

const (
	defaultOraclePort = 1521

	// go-ora magic strings
	walletKey         = "WALLET"
	walletPasswordKey = "WALLET PASSWORD"
	sslKey            = "SSL"
)

// buildConnectionString parses connStr (oracle://user:password@host:port/service) supporting
// overriding of of connectio parameters.
func buildConnectionString(connStr string, overrides map[string]string, log *service.Logger) (string, error) {
	u, err := url.Parse(connStr)
	if err != nil {
		return "", fmt.Errorf("parsing url: %w", err)
	}
	if u.Scheme != "oracle" {
		return "", fmt.Errorf("unsupported connection string scheme %q: connection_string must use the oracle:// format", u.Scheme)
	}

	var (
		server         = u.Hostname()
		port           = defaultOraclePort
		svc            = strings.TrimPrefix(u.Path, "/")
		user, password string
		opts           = make(map[string]string)
	)

	if raw := u.Port(); raw != "" {
		if port, err = strconv.Atoi(raw); err != nil {
			return "", fmt.Errorf("parsing port %q: %w", raw, err)
		}
	}

	if u.User != nil {
		user = u.User.Username()
		password, _ = u.User.Password()
	}

	for key, vals := range u.Query() {
		if len(vals) > 0 {
			opts[key] = vals[0]
		}
	}

	// if key exists, overrides win
	maps.Copy(opts, overrides)

	if val, ok := opts[walletKey]; ok {
		log.Infof("Using wallet path '%s'", val)
	}

	return go_ora.BuildUrl(server, port, svc, user, password, opts), nil
}

// parseWalletConfig constructs a query-param overrides map for Oracle Wallet
// authentication. When wallet_path is configured, SSL is enabled automatically.
// wallet_password is only needed for ewallet.p12 wallets; cwallet.sso auto-login
// wallets do not require a password. Returns nil when wallet_path is not set.
func parseWalletConfig(conf *service.ParsedConfig, overrides map[string]string) error {
	if !conf.Contains(ociFieldWalletPath) {
		return nil
	}

	walletPath, err := conf.FieldString(ociFieldWalletPath)
	if err != nil {
		return err
	}

	overrides[walletKey] = walletPath
	overrides[sslKey] = "true"

	if conf.Contains(ociFieldWalletPassword) {
		if walletPassword, err := conf.FieldString(ociFieldWalletPassword); err != nil {
			return err
		} else if walletPassword != "" {
			overrides[walletPasswordKey] = walletPassword
		}
	}

	return nil
}

// SnapshotMode controls whether and how an initial table snapshot is taken before streaming begins.
type SnapshotMode string

const (
	// SnapshotModeNone skips snapshotting and starts streaming from the current SCN.
	SnapshotModeNone SnapshotMode = "none"
	// SnapshotModeSnapshotOnly performs a full snapshot, persists the SCN checkpoint, then stops without streaming.
	SnapshotModeSnapshotOnly SnapshotMode = "snapshot_only"
	// SnapshotModeSnapshotAndStream performs a full snapshot then transitions to streaming.
	SnapshotModeSnapshotAndStream SnapshotMode = "snapshot_and_stream"
)

// IsSnapshotOnly returns true if the snapshot_mode config is snapshot_only, otherwise false.
func (s SnapshotMode) IsSnapshotOnly() bool {
	return s == SnapshotModeSnapshotOnly
}

// IsSnapshotNone returns true if the snapshot_mode config is none, otherwise false.
func (s SnapshotMode) IsSnapshotNone() bool {
	return s == SnapshotModeNone
}

func parseSnapshotMode(conf *service.ParsedConfig) (SnapshotMode, error) {
	if conf.Contains(ociFieldSnapshotMode) {
		if raw, err := conf.FieldString(ociFieldSnapshotMode); err != nil {
			return SnapshotModeNone, err
		} else {
			return SnapshotMode(raw), nil
		}
	}
	// snapshot_mode not set — apply backward compat
	if streamSnapshot, err := conf.FieldBool(ociFieldStreamSnapshot); err != nil {
		return SnapshotModeNone, err
	} else if streamSnapshot {
		return SnapshotModeSnapshotAndStream, nil
	}
	return SnapshotModeNone, nil
}
