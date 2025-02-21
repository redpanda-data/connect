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

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/asyncroutine"
)

type heartbeat struct {
	db            *sql.DB
	task          *asyncroutine.Periodic
	logger        *service.Logger
	prefix, value string
}

func newHeartbeat(dsn string, interval time.Duration, logger *service.Logger, prefix, value string) (*heartbeat, error) {
	dbConn, err := openPgConnectionFromConfig(dsn)
	if err != nil {
		return nil, err
	}
	h := &heartbeat{db: dbConn, task: nil, logger: logger, prefix: prefix, value: value}
	h.task = asyncroutine.NewPeriodicWithContext(interval, h.run)
	return h, nil
}

func (h *heartbeat) Start() {
	h.task.Start()
}

func (h *heartbeat) run(ctx context.Context) {
	_, err := h.db.ExecContext(ctx, "SELECT pg_logical_emit_message(false, $1, $2)", h.prefix, h.value)
	if err != nil {
		h.logger.Warnf("unable to write heartbeat message: %v", err)
	}
}

func (h *heartbeat) Stop() error {
	h.task.Stop()
	return h.db.Close()
}
