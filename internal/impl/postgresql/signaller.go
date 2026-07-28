// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package pgstream

import (
	"encoding/json"
	"fmt"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream"
	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream/sanitize"
	"github.com/redpanda-data/connect/v4/internal/replication"
)

type postgresSignaller struct {
	Log *service.Logger

	schema    string
	tableName string
}

// NewControlSignaller creates a postgresSignaller that detects signal
// INSERTs on the given schema.tableName.
func NewControlSignaller(schema, tableName string, log *service.Logger) (*postgresSignaller, error) {
	normalizedSchema, err := wireFormPostgresIdentifier(schema)
	if err != nil {
		return nil, fmt.Errorf("invalid schema %q: %w", schema, err)
	}
	normalizedTableName := tableName
	if tableName != "" {
		if normalizedTableName, err = wireFormPostgresIdentifier(tableName); err != nil {
			return nil, fmt.Errorf("invalid signal table name %q: %w", tableName, err)
		}
	}
	return &postgresSignaller{Log: log, schema: normalizedSchema, tableName: normalizedTableName}, nil
}

// Enabled reports whether a signal table was configured.
func (s *postgresSignaller) Enabled() bool {
	return s.tableName != ""
}

// Listen returns any actionable signal found; signal rows should always be
// forwarded downstream as normal messages regardless of the outcome here.
func (s *postgresSignaller) Listen(msg *pglogicalstream.StreamMessage) (*replication.ControlSignal, error) {
	if msg.Operation != pglogicalstream.InsertOpType {
		return nil, nil
	}
	if msg.Schema != s.schema || msg.Table != s.tableName {
		return nil, nil
	}

	row, ok := msg.Data.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected map for %s message data, got %T", s.tableName, msg.Data)
	}
	dataStr, ok := row["data"].(string)
	if !ok {
		return nil, fmt.Errorf("expected string for %s.data column, got %T", s.tableName, row["data"])
	}

	var sig replication.ControlSignal
	if sig.SignalType, ok = row["type"].(string); !ok {
		return nil, fmt.Errorf("expected string for %s.type column, got %T", s.tableName, row["type"])
	}

	sig.ID = fmt.Sprintf("%v", row["id"])
	log := s.Log.With("id", sig.ID, "type", sig.SignalType)

	if msg.LSN != nil {
		sig.LSN = []byte(*msg.LSN)
	}

	// validate signal type
	switch sig.SignalType {
	case replication.LogSignalType:
		if err := json.Unmarshal([]byte(dataStr), &sig.LogSignal); err != nil {
			return nil, fmt.Errorf("unmarshaling control signal %s.data: %w", s.tableName, err)
		}
		log.Infof("%s (lsn=%s)", sig.Message, sig.LSN)
	default:
		log.Warnf("Control signal %q received but not a recognized type", sig.SignalType)
	}

	return &sig, nil
}

func wireFormPostgresIdentifier(name string) (string, error) {
	normalized, err := sanitize.NormalizePostgresIdentifier(name)
	if err != nil {
		return "", err
	}
	return sanitize.UnquotePostgresIdentifier(normalized)
}
