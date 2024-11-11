// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import "github.com/jackc/pgx/v5/pgtype"

// PluginHandler is an interface that must be implemented by all plugin handlers
type PluginHandler interface {
	Handle(clientXLogPos LSN, xld XLogData) error
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
func (w *Wal2JsonPluginHandler) Handle(clientXLogPos LSN, xld XLogData) error {
	// get current stream metrics
	metrics := w.monitor.Report()
	message, err := decodeWal2JsonChanges(clientXLogPos.String(), xld.WALData)
	if err != nil {
		return err
	}

	if message != nil && len(message.Changes) > 0 {
		message.WALLagBytes = &metrics.WalLagInBytes
		w.messages <- *message
	}

	return nil
}

// PgOutputPluginHandler is a handler for pgoutput output plugin
type PgOutputPluginHandler struct {
	messages chan StreamMessage
	monitor  *Monitor

	streamUncommitted bool
	relations         map[uint32]*RelationMessage
	typeMap           *pgtype.Map
	pgoutputChanges   []StreamMessageChanges

	consumedCallback   chan bool
	transactionAckChan chan string
}

// NewPgOutputPluginHandler creates a new PgOutputPluginHandler
func NewPgOutputPluginHandler(
	messages chan StreamMessage,
	streamUncommitted bool,
	monitor *Monitor,
	consumedCallback chan bool,
	transactionAckChan chan string,
) *PgOutputPluginHandler {
	return &PgOutputPluginHandler{
		messages:           messages,
		monitor:            monitor,
		streamUncommitted:  streamUncommitted,
		relations:          map[uint32]*RelationMessage{},
		typeMap:            pgtype.NewMap(),
		pgoutputChanges:    []StreamMessageChanges{},
		consumedCallback:   consumedCallback,
		transactionAckChan: transactionAckChan,
	}
}

// Handle handles the pgoutput output
func (p *PgOutputPluginHandler) Handle(clientXLogPos LSN, xld XLogData) error {
	if p.streamUncommitted {
		// parse changes inside the transaction
		message, err := decodePgOutput(xld.WALData, p.relations, p.typeMap)
		if err != nil {
			return err
		}

		isCommit, _, err := isCommitMessage(xld.WALData)
		if err != nil {
			return err
		}

		// when receiving a commit message, we need to acknowledge the LSN
		// but we must wait for benthos to flush the messages before we can do that
		if isCommit {
			p.transactionAckChan <- clientXLogPos.String()
			<-p.consumedCallback
		} else {
			if message == nil && !isCommit {
				return nil
			} else if message != nil {
				lsn := clientXLogPos.String()
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
			return err
		}

		if isBegin {
			p.pgoutputChanges = []StreamMessageChanges{}
		}

		// parse changes inside the transaction
		message, err := decodePgOutput(xld.WALData, p.relations, p.typeMap)
		if err != nil {
			return err
		}

		if message != nil {
			p.pgoutputChanges = append(p.pgoutputChanges, *message)
		}

		isCommit, _, err := isCommitMessage(xld.WALData)
		if err != nil {
			return err
		}

		if isCommit {
			if len(p.pgoutputChanges) == 0 {
				return nil
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

	return nil
}
