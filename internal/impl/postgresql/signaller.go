// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package pgstream

import (
	"context"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream"
	"github.com/redpanda-data/connect/v4/internal/replication"
)

var _ replication.Signaller = (*snapshotSignaller)(nil)

type snapshotSignaller struct {
	log          *service.Logger
	schema       string
	tableName    string
	onSignalChan chan struct{}
}

func NewSnapshotSignaller(schema, tableName string, log *service.Logger) (*snapshotSignaller, error) {
	return &snapshotSignaller{
		log:          log,
		schema:       schema,
		tableName:    tableName,
		onSignalChan: make(chan struct{}, 1),
	}, nil
}

func (o *snapshotSignaller) Listen(_ context.Context, event any) error {
	msg, ok := event.(pglogicalstream.StreamMessage)
	if !ok {
		return nil
	}
	if msg.Schema != o.schema || msg.Table != o.tableName {
		return nil
	}

	o.log.Debugf("snapshot signal received: operation=%s lsn=%v", msg.Operation, msg.LSN)

	o.onSignalChan <- struct{}{}
	return nil
}

func (o *snapshotSignaller) OnSignal() struct{} {
	return struct{}{}
}

func (o *snapshotSignaller) ValidateChannel(ctx context.Context) error {
	return nil
}
