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
	// message. Only a transactional one carries a transaction id, which
	// OnCommit needs to advance an incremental snapshot -- on a quiet table
	// the heartbeat is the only write there is.
	//
	// It is read per tick, so a backfill starting or finishing takes effect
	// on the next one. A transaction id costs something, so an idle
	// coordinator gets the cheaper non-transactional message.
	transactional func() bool
}

// EffectiveHeartbeatInterval returns how often to heartbeat: the more
// frequent of the two intervals while a snapshot is enabled, since a snapshot
// needs commits far more often than slot retention does, and heartbeating
// faster serves both. The interval is fixed for the life of the input, but
// the ticks only cost a transaction id while a backfill runs -- refer to
// heartbeat.transactional.
func EffectiveHeartbeatInterval(configured time.Duration, incSnapshot *incsnapshot.Cfg) time.Duration {
	if !incSnapshot.IsEnabled() || incSnapshot.HeartbeatInterval <= 0 {
		return configured
	}
	return min(configured, incSnapshot.HeartbeatInterval)
}

// newHeartbeat builds the heartbeat. transactional is consulted on each tick;
// a nil one means never transactional.
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
