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
	"database/sql"
	"time"

	incsnapshot "github.com/redpanda-data/connect/v4/internal/impl/postgresql/incrementalsnapshot"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/asyncroutine"
)

type heartbeat struct {
	db            *sql.DB
	task          *asyncroutine.Periodic
	logger        *service.Logger
	prefix, value string
	// transactional reports whether this tick needs a transactional
	// message used to advance incremental snapshot on a quiet table.
	transactional func() bool
}

func newHeartbeat(config *Config, interval time.Duration, prefix, value string, transactional func() bool) (*heartbeat, error) {
	dbConn, err := openPgConnectionFromConfig(config)
	if err != nil {
		return nil, err
	}
	h := &heartbeat{db: dbConn, task: nil, logger: config.Logger, prefix: prefix, value: value, transactional: transactional}
	h.task = asyncroutine.NewPeriodicWithContext(interval, h.run)
	return h, nil
}

func (h *heartbeat) Start() {
	h.task.Start()
}

func (h *heartbeat) run(ctx context.Context) {
	var err error
	if h.transactional != nil && h.transactional() {
		_, err = h.db.ExecContext(ctx, "SELECT pg_logical_emit_message(true, $1, $2)", h.prefix, h.value)
	} else {
		_, err = h.db.ExecContext(ctx, "SELECT pg_logical_emit_message(false, $1, $2)", h.prefix, h.value)
	}
	if err != nil {
		h.logger.Warnf("unable to write heartbeat message: %v", err)
	}
}

func (h *heartbeat) Stop() error {
	h.task.Stop()
	return h.db.Close()
}

func effectiveHeartbeatInterval(configured time.Duration, incSnapshot *incsnapshot.Cfg) time.Duration {
	if !incSnapshot.IsEnabled() || incSnapshot.HeartbeatInterval <= 0 {
		return configured
	}
	return min(configured, incSnapshot.HeartbeatInterval)
}
