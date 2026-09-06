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
	// transactional selects a transactional heartbeat message. It must be
	// true during an incremental snapshot. OnCommit gets a transaction id
	// from a transactional message only. On a quiet table the heartbeat can
	// also be the only write.
	transactional bool
}

func newHeartbeat(config *Config, prefix, value string) (*heartbeat, error) {
	dbConn, err := openPgConnectionFromConfig(config)
	if err != nil {
		return nil, err
	}
	enabled := config.IncrementalSnapshotCfg().IsEnabled()
	h := &heartbeat{db: dbConn, task: nil, logger: config.Logger, prefix: prefix, value: value, transactional: enabled}
	h.task = asyncroutine.NewPeriodicWithContext(config.HeartbeatInterval, h.run)
	return h, nil
}

func (h *heartbeat) Start() {
	h.task.Start()
}

func (h *heartbeat) run(ctx context.Context) {
	var err error
	if h.transactional {
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
