// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"context"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream/watermark"
)

// PluginHandler is an interface that must be implemented by all plugin handlers
type PluginHandler interface {
	// returns true if we need to ack the clientXLogPos
	Handle(ctx context.Context, clientXLogPos LSN, xld XLogData) (bool, error)
}

// PgOutputUnbufferedPluginHandler is a native output handler that emits each message as it's received.
type PgOutputUnbufferedPluginHandler struct {
	messages chan StreamMessage
	monitor  *Monitor

	relations map[uint32]*RelationMessage
	typeMap   *pgtype.Map

	lastEmitted       LSN
	lsnWatermark      *watermark.Value[LSN]
	includeTxnMarkers bool
}

// NewPgOutputPluginHandler creates a new PgOutputPluginHandler
func NewPgOutputPluginHandler(
	messages chan StreamMessage,
	monitor *Monitor,
	lsnWatermark *watermark.Value[LSN],
	includeTxnMarkers bool,
) PluginHandler {
	return &PgOutputUnbufferedPluginHandler{
		messages:          messages,
		monitor:           monitor,
		relations:         map[uint32]*RelationMessage{},
		typeMap:           pgtype.NewMap(),
		lastEmitted:       lsnWatermark.Get(),
		lsnWatermark:      lsnWatermark,
		includeTxnMarkers: includeTxnMarkers,
	}
}

// Handle handles the pgoutput output
func (p *PgOutputUnbufferedPluginHandler) Handle(ctx context.Context, clientXLogPos LSN, xld XLogData) (bool, error) {
	// parse changes inside the transaction
	message, err := decodePgOutput(xld.WALData, p.relations, p.typeMap)
	if err != nil {
		return false, err
	}
	if message == nil {
		return false, nil
	}

	if !p.includeTxnMarkers {
		switch message.Operation {
		case CommitOpType:
			// when receiving a commit message, we need to acknowledge the LSN
			// but we must wait for connect to flush the messages before we can do that
			select {
			case <-p.lsnWatermark.WaitFor(p.lastEmitted):
				return true, nil
			case <-ctx.Done():
				return false, ctx.Err()
			}
		case BeginOpType:
			return false, nil
		}
	}

	lsn := clientXLogPos.String()
	message.Lsn = &lsn
	select {
	case p.messages <- *message:
		p.lastEmitted = clientXLogPos
	case <-ctx.Done():
		return false, ctx.Err()
	}

	return false, nil
}
