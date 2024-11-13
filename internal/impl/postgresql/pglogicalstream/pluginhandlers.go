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
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream/watermark"
)

// PluginHandler is an interface that must be implemented by all plugin handlers
type PluginHandler interface {
	// returns true if we need to ack the clientXLogPos
	Handle(ctx context.Context, clientXLogPos LSN, xld XLogData) (bool, error)
}

// Wal2JsonPluginHandler is a handler for wal2json output plugin
type Wal2JsonPluginHandler struct {
	messages chan StreamMessage
	monitor  *Monitor
}

// NewWal2JsonPluginHandler creates a new Wal2JsonPluginHandler
func NewWal2JsonPluginHandler(messages chan StreamMessage, monitor *Monitor) *Wal2JsonPluginHandler {
	return &Wal2JsonPluginHandler{
		messages: messages,
		monitor:  monitor,
	}
}

// Handle handles the wal2json output
func (w *Wal2JsonPluginHandler) Handle(_ context.Context, clientXLogPos LSN, xld XLogData) (bool, error) {
	// get current stream metrics
	metrics := w.monitor.Report()
	message, err := decodeWal2JsonChanges(clientXLogPos.String(), xld.WALData)
	if err != nil {
		return false, err
	}

	if message != nil && len(message.Changes) > 0 {
		message.WALLagBytes = &metrics.WalLagInBytes
		w.messages <- *message
	}

	return false, nil
}

// PgOutputPluginHandler is a handler for pgoutput output plugin
type PgOutputPluginHandler struct {
	messages chan StreamMessage
	monitor  *Monitor

	streamUncommitted bool
	relations         map[uint32]*RelationMessage
	typeMap           *pgtype.Map
	pgoutputChanges   []StreamMessageChanges

	lastEmitted  LSN
	lsnWatermark *watermark.Value[LSN]
}

// NewPgOutputPluginHandler creates a new PgOutputPluginHandler
func NewPgOutputPluginHandler(
	messages chan StreamMessage,
	streamUncommitted bool,
	monitor *Monitor,
	lsnWatermark *watermark.Value[LSN],
) *PgOutputPluginHandler {
	return &PgOutputPluginHandler{
		messages:          messages,
		monitor:           monitor,
		streamUncommitted: streamUncommitted,
		relations:         map[uint32]*RelationMessage{},
		typeMap:           pgtype.NewMap(),
		pgoutputChanges:   []StreamMessageChanges{},
		lastEmitted:       lsnWatermark.Get(),
		lsnWatermark:      lsnWatermark,
	}
}

// Handle handles the pgoutput output
func (p *PgOutputPluginHandler) Handle(ctx context.Context, clientXLogPos LSN, xld XLogData) (bool, error) {
	if p.streamUncommitted {
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
		} else {
			if message == nil && !isCommit {
				return false, nil
			} else if message != nil {
				lsn := clientXLogPos.String()
				p.lastEmitted = clientXLogPos
				p.messages <- StreamMessage{
					Lsn: &lsn,
					Changes: []StreamMessageChanges{
						*message,
					},
					Mode:        StreamModeStreaming,
					WALLagBytes: &p.monitor.Report().WalLagInBytes,
				}
			}
		}
	} else {
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

		if isCommit {
			if len(p.pgoutputChanges) == 0 {
				return false, nil
			} else {
				// send all collected changes
				lsn := clientXLogPos.String()
				p.messages <- StreamMessage{
					Lsn:         &lsn,
					Changes:     p.pgoutputChanges,
					Mode:        StreamModeStreaming,
					WALLagBytes: &p.monitor.Report().WalLagInBytes,
				}
			}
		}
	}

	return false, nil
}
