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
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream"
	"github.com/redpanda-data/connect/v4/internal/replication"
)

var (
	_ replication.Signaller = (*eventSignaller)(nil)
)

type eventSignaller struct {
	schema       string
	tableName    string
	onSignalChan chan *string
	log          *service.Logger

	eventPending atomic.Bool
}

func NewEventSignaller(schema, tableName string, log *service.Logger) (*eventSignaller, error) {
	return &eventSignaller{
		schema:       schema,
		tableName:    tableName,
		onSignalChan: make(chan *string, 1),
		log:          log,
	}, nil
}

// Listen checks for signal related events, returning true when the message is a signal
// so the caller can decide whether they want to skip it or publish it to the pipeline.
func (o *eventSignaller) Listen(_ context.Context, event any) (bool, error) {
	msg, ok := event.(pglogicalstream.StreamMessage)
	if !ok {
		return false, nil
	}
	if msg.Schema != o.schema || msg.Table != o.tableName {
		return false, nil
	}
	if msg.Operation != pglogicalstream.InsertOpType {
		return false, nil
	}

	row, ok := msg.Data.(map[string]any)
	if !ok {
		return false, fmt.Errorf("failed to parse message data")
	}

	dataStr, ok := row["data"].(string)
	if !ok {
		return false, fmt.Errorf("failed to parse %s.data", o.tableName)
	}

	var sig replication.EventSignal
	if err := json.Unmarshal([]byte(dataStr), &sig); err != nil {
		return false, fmt.Errorf("unmarshaling signal %s.data: %w", o.tableName, err)
	}

	sig.ID = fmt.Sprintf("%v", row["id"])

	evType, ok := row["type"].(string)
	if !ok {
		return false, errors.New("parsing 'type' data")
	}
	sig.Type = evType

	log := o.log.With("id", sig.ID, "type", sig.Type)
	log.Infof("Signal %q received: operation=%s lsn=%v", sig.Type, msg.Operation, msg.LSN)

	o.eventPending.Store(true)
	select {
	case o.onSignalChan <- msg.LSN:
	default:
	}
	return true, nil
}

func (o *eventSignaller) OnSignal() <-chan *string {
	return o.onSignalChan
}

func (o *eventSignaller) IsPending() bool {
	return o.eventPending.Load()
}
func (o *eventSignaller) Reset() {
	o.eventPending.Store(false)
}

func (o *eventSignaller) ValidateChannel(ctx context.Context) error {
	return nil
}
