// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication

import (
	"context"
	"database/sql"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// LogMiner tracks and streams all change events from the configured change
// tables tracked in tables.
type LogMiner struct {
	tables          []UserDefinedTable
	backoffInterval time.Duration
	publisher       ChangePublisher
	log             *service.Logger
}

// NewLogMiner creates a new instance of NewLogMiner, responsible
// for paging through change events based on the tables param.
func NewLogMiner(tables []UserDefinedTable, publisher ChangePublisher, backoffInterval time.Duration, logger *service.Logger) *LogMiner {
	s := &LogMiner{
		tables:          tables,
		publisher:       publisher,
		backoffInterval: backoffInterval,
		log:             logger,
	}
	return s
}

// ReadChanges streams the change events from the configured SQL Server change tables.
func (r *LogMiner) ReadChanges(ctx context.Context, db *sql.DB, startPos SCN) error {
	r.log.Infof("Starting streaming %d change table(s)", len(r.tables))
	var (
		startLSN SCN // load last checkpoint; nil means start from beginning in tables
		endLSN   SCN // often set to fn_cdc_get_max_lsn(); nil means no upper bound
		lastLSN  SCN
	)

	if len(startPos) != 0 {
		startLSN = startPos
		lastLSN = startPos
		r.log.Infof("Resuming from recorded LSN position '%s'", startPos)
	}

	return nil
}
