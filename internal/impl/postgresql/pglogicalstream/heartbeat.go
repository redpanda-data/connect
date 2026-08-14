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

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/asyncroutine"
)

type heartbeat struct {
	db            *sql.DB
	task          *asyncroutine.Periodic
	logger        *service.Logger
	prefix, value string
	// transactional controls whether the heartbeat is emitted
	// transactionally. Must be true during an incremental snapshot: OnCommit
	// only sees a txid for transactional messages, and heartbeats may be the
	// only write traffic on otherwise-quiet tables.
	transactional bool
}

func newHeartbeat(config *Config, prefix, value string) (*heartbeat, error) {
	dbConn, err := openPgConnectionFromConfig(config)
	if err != nil {
		return nil, err
	}
	h := &heartbeat{
		db:     dbConn,
		task:   nil,
		logger: config.Logger,
		prefix: prefix,
		value:  value,
		transactional: config.IncrementalSnapshot != nil &&
			config.IncrementalSnapshot.Enabled,
	}
	h.task = asyncroutine.NewPeriodicWithContext(config.HeartbeatInterval, h.run)
	return h, nil
}

func (h *heartbeat) Start() {
	h.task.Start()
}

func (h *heartbeat) run(ctx context.Context) {
	// Unchanged (literal inline, not a placeholder) when incremental
	// snapshotting is disabled -- a strict no-op for existing users.
	query := "SELECT pg_logical_emit_message(false, $1, $2)"
	if h.transactional {
		query = "SELECT pg_logical_emit_message(true, $1, $2)"
	}
	_, err := h.db.ExecContext(ctx, query, h.prefix, h.value)
	if err != nil {
		h.logger.Warnf("unable to write heartbeat message: %v", err)
	}
}

func (h *heartbeat) Stop() error {
	h.task.Stop()
	return h.db.Close()
}
