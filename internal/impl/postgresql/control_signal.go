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

var _ replication.Signaller = (*pgSignaller)(nil)

type pgSignaller struct {
	*replication.ControlSignaller

	schema          string
	signalTableName string
}

// newPGSignaller creates a replication.Signaller that detects signal INSERTs on the given schema.tableName.
func newPGSignaller(schema, tableName string, log *service.Logger) *pgSignaller {
	s := replication.NewControlSignaller(log)
	return &pgSignaller{ControlSignaller: s, schema: schema, signalTableName: tableName}
}

// Listen returns any actionable signal found. Signal rows are always
// forwarded downstream as normal messages regardless of the outcome here.
//
// An empty tableName means signalling is disabled: every message is ignored.
//
// Only validated execute-snapshot signals return non-nil - everything else
// (an unsupported type, or a validated no-op) returns (nil, nil) and is
// acked immediately like any other message.
func (s *pgSignaller) Listen(_ context.Context, signal any) (*replication.ControlSignal, error) {
	if s.signalTableName == "" {
		return nil, nil
	}
	msg, ok := signal.(pglogicalstream.StreamMessage)
	if !ok {
		return nil, nil
	}
	if msg.Schema != s.schema || msg.Table != s.signalTableName {
		return nil, nil
	}
	if msg.Operation != pglogicalstream.InsertOpType {
		return nil, nil
	}

	// deserialise control signal
	row, ok := msg.Data.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected map for %s message data, got %T", s.signalTableName, msg.Data)
	}
	dataStr, ok := row["data"].(string)
	if !ok {
		return nil, fmt.Errorf("expected string for %s.data column, got %T", s.signalTableName, row["data"])
	}
	var sig replication.ControlSignal
	if err := json.Unmarshal([]byte(dataStr), &sig); err != nil {
		return nil, fmt.Errorf("unmarshaling signal %s.data: %w", s.signalTableName, err)
	}
	sig.ID = fmt.Sprintf("%v", row["id"])
	if sig.Type, ok = row["type"].(string); !ok {
		return nil, errors.New("parsing 'type' data")
	}

	log := s.Log.With("id", sig.ID, "type", sig.Type)

	if !sig.IsSnapshot() {
		log.Infof("Signal %q received but not a recognized action, forwarding as a regular message", sig.Type)
		return nil, nil
	}

	// Invalid or no-op signals are not returned as actionable.
	if len(sig.DataCollections) == 0 {
		log.Warnf("Signal %q received but data-collections is empty — ignoring, streaming continues uninterrupted", sig.Type)
		return nil, nil
	}
	if len(tableNamesFromSchema(sig.DataCollections, s.schema)) == 0 {
		log.Warnf("Signal %q received but data-collections %v matched no tables for schema %q — ignoring, streaming continues uninterrupted", sig.Type, sig.DataCollections, s.schema)
		return nil, nil
	}

	log.Infof("Signal %q received: operation=%s lsn=%v", sig.Type, msg.Operation, msg.LSN)

	if msg.LSN != nil {
		sig.LSN = []byte(*msg.LSN)
	}
	return &sig, nil
}

// awaitCheckpointLSN blocks until checkpointer's highest resolved offset has reached or passed target,
// meaning every message up to and including it has been acknowledged downstream. An empty target is a no-op.
func awaitCheckpointLSN(ctx context.Context, cp *checkpoint.Capped[*string], target []byte, waitInterval time.Duration) error {
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
		if highest := cp.Highest(); highest != nil && *highest != nil {
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
