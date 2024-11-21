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

	lastEmitted  LSN
	lsnWatermark *watermark.Value[LSN]
}

// PgOutputBufferedPluginHandler is a native output handler that buffers and emits each transaction together
type PgOutputBufferedPluginHandler struct {
	messages chan StreamMessage
	monitor  *Monitor

	relations       map[uint32]*RelationMessage
	typeMap         *pgtype.Map
	pgoutputChanges []StreamMessageChanges
}

// NewPgOutputPluginHandler creates a new PgOutputPluginHandler
func NewPgOutputPluginHandler(
	messages chan StreamMessage,
	batchTransactions bool,
	monitor *Monitor,
	lsnWatermark *watermark.Value[LSN],
) PluginHandler {
	if batchTransactions {
		return &PgOutputUnbufferedPluginHandler{
			messages:     messages,
			monitor:      monitor,
			relations:    map[uint32]*RelationMessage{},
			typeMap:      pgtype.NewMap(),
			lastEmitted:  lsnWatermark.Get(),
			lsnWatermark: lsnWatermark,
		}
	}
	return &PgOutputBufferedPluginHandler{
		messages:        messages,
		monitor:         monitor,
		relations:       map[uint32]*RelationMessage{},
		typeMap:         pgtype.NewMap(),
		pgoutputChanges: []StreamMessageChanges{},
	}
}

// Handle handles the pgoutput output
func (p *PgOutputUnbufferedPluginHandler) Handle(ctx context.Context, clientXLogPos LSN, xld XLogData) (bool, error) {
	// parse changes inside the transaction
	message, err := decodePgOutput(xld.WALData, p.relations, p.typeMap)
	if err != nil {
		return false, err
	}

	isCommit, _, err := isCommitMessage(xld.WALData)
	if err != nil {
		return false, err
	}

	// when receiving a commit message, we need to acknowledge the LSN
	// but we must wait for connect to flush the messages before we can do that
	if isCommit {
		select {
		case <-p.lsnWatermark.WaitFor(p.lastEmitted):
			return true, nil
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}

	if message != nil {
		lsn := clientXLogPos.String()
		msg := StreamMessage{
			Lsn:     &lsn,
			Changes: []StreamMessageChanges{*message},
			Mode:    StreamModeStreaming,
		}
		select {
		case p.messages <- msg:
			p.lastEmitted = clientXLogPos
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}

	return false, nil
}

// Handle handles the pgoutput output
func (p *PgOutputBufferedPluginHandler) Handle(ctx context.Context, clientXLogPos LSN, xld XLogData) (bool, error) {
	// message changes must be collected in the buffer in the context of the same transaction
	// as single transaction can contain multiple changes
	// and LSN ack will cause potential loss of changes
	isBegin, err := isBeginMessage(xld.WALData)
	if err != nil {
		return false, err
	}

	if isBegin {
		p.pgoutputChanges = []StreamMessageChanges{}
	}

	// parse changes inside the transaction
	message, err := decodePgOutput(xld.WALData, p.relations, p.typeMap)
	if err != nil {
		return false, err
	}

	if message != nil {
		p.pgoutputChanges = append(p.pgoutputChanges, *message)
	}

	isCommit, _, err := isCommitMessage(xld.WALData)
	if err != nil {
		return false, err
	}

	if !isCommit {
		return false, nil
	}

	if len(p.pgoutputChanges) > 0 {
		// send all collected changes
		lsn := clientXLogPos.String()
		msg := StreamMessage{
			Lsn:     &lsn,
			Changes: p.pgoutputChanges,
			Mode:    StreamModeStreaming,
		}
		select {
		case p.messages <- msg:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}

	return false, nil
}
