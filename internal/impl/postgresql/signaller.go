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
	"strings"
	"time"

	"github.com/Jeffail/checkpoint"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream"
	"github.com/redpanda-data/connect/v4/internal/replication"
)

var _ replication.Signaller = (*postgresSignaller)(nil)

type postgresSignaller struct {
	*replication.ControlSignaller

	schema    string
	tableName string
}

// NewControlSignaller creates a replication.Signaller that detects signal INSERTs on the given schema.tableName.
func NewControlSignaller(schema, tableName string, log *service.Logger) *postgresSignaller {
	s := replication.NewControlSignaller(log)
	return &postgresSignaller{s, schema, tableName}
}

// Listen checks for signal related events and stores any valid pending signal.
// Signal rows are published as normal messages; callers should not suppress them.
func (s *postgresSignaller) Listen(_ context.Context, signal any) error {
	msg, ok := signal.(pglogicalstream.StreamMessage)
	if !ok {
		return nil
	}
	if msg.Schema != s.schema || msg.Table != s.tableName {
		return nil
	}
	if msg.Operation != pglogicalstream.InsertOpType {
		return nil
	}

	row, ok := msg.Data.(map[string]any)
	if !ok {
		return fmt.Errorf("expected map for %s message data, got %T", s.tableName, msg.Data)
	}

	dataStr, ok := row["data"].(string)
	if !ok {
		return fmt.Errorf("expected string for %s.data column, got %T", s.tableName, row["data"])
	}

	var sig replication.ControlSignal
	if err := json.Unmarshal([]byte(dataStr), &sig); err != nil {
		return fmt.Errorf("unmarshaling signal %s.data: %w", s.tableName, err)
	}

	sig.ID = fmt.Sprintf("%v", row["id"])

	evType, ok := row["type"].(string)
	if !ok {
		return errors.New("parsing 'type' data")
	}
	sig.Type = evType

	log := s.Log.With("id", sig.ID, "type", sig.Type)

	// Validate snapshot signals before triggering a stream interruption.
	// Invalid or no-op signals are not stored as pending, so streaming continues uninterrupted.
	if sig.IsSnapshot() {
		if len(sig.DataCollections) == 0 {
			log.Warnf("Signal %q received but data-collections is empty — ignoring, streaming continues uninterrupted", sig.Type)
			return nil
		}
		if len(tableNamesFromSchema(sig.DataCollections, s.schema)) == 0 {
			log.Warnf("Signal %q received but data-collections %v matched no tables for schema %q — ignoring, streaming continues uninterrupted", sig.Type, sig.DataCollections, s.schema)
			return nil
		}
	}

	log.Infof("Signal %q received: operation=%s lsn=%v", sig.Type, msg.Operation, msg.LSN)

	// Notify the pending signal but don't StoreSignal it yet: IsPending drives
	// whether Connect re-runs a snapshot on the next reconnect, and that must
	// only happen once the caller confirms this signal (and anything batched
	// ahead of it) has actually been acknowledged downstream.
	if msg.LSN != nil {
		sig.LSN = []byte(*msg.LSN)
	}
	select {
	case s.SignalChan <- &sig:
	default:
	}
	return nil
}

// awaitCheckpointLSN blocks until checkpointer's highest resolved offset has reached or passed target,
// meaning every message up to and including it has been acknowledged downstream. An empty target is a no-op.
func awaitCheckpointLSN(ctx context.Context, checkpointer *checkpoint.Capped[*string], target []byte, waitInterval time.Duration) error {
	if len(target) == 0 {
		return nil
	}
	targetLSN, err := pglogicalstream.ParseLSN(string(target))
	if err != nil {
		return fmt.Errorf("unable to parse target LSN: %w", err)
	}

	ticker := time.NewTicker(waitInterval)
	defer ticker.Stop()
	for {
		if highest := checkpointer.Highest(); highest != nil && *highest != nil {
			if gotLSN, err := pglogicalstream.ParseLSN(**highest); err == nil && gotLSN >= targetLSN {
				return nil
			}
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func tableNamesFromSchema(collections []string, schema string) []string {
	if len(collections) == 0 {
		return nil
	}
	tables := make([]string, 0, len(collections))
	for _, dc := range collections {
		table := dc
		if idx := strings.LastIndex(dc, "."); idx >= 0 {
			if !strings.EqualFold(dc[:idx], schema) {
				continue
			}
			table = dc[idx+1:]
		}
		tables = append(tables, table)
	}
	return tables
}
