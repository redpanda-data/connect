// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication

import (
	"encoding/json"
	"errors"
	"fmt"
)

// ErrSignalRejected marks a signal the connector will never honour (malformed,
// or naming something it cannot act on). Callers log it at Error and carry on;
// any other error means the signal could not be judged and must be redelivered.
var ErrSignalRejected = errors.New("rejected")

// ErrSnapshotDisabled marks a snapshot-execute signal received while incremental
// snapshots are off: a configuration mistake, logged at Warn. Connectors wrap it
// with the config field to set.
var ErrSnapshotDisabled = errors.New("a " + SnapshotSignalType + " signal needs incremental snapshots enabled, so no backfill was queued for it")

// LogSignalType represents a log signal.
const LogSignalType = "log"

// LogSignal is the decoded "data" payload for a LogSignalType signal.
type LogSignal struct {
	Message string `json:"message"`
}

// ControlSignal represents a insert into the signal table.
type ControlSignal struct {
	ID         string
	SignalType string
	LogSignal

	// LSN is the log sequence number/offset the signal was observed at, in
	// whatever raw form the connector's replication stream represents
	// positions (e.g. a decimal/hex string, or raw binary bytes for
	// connectors like Oracle whose SCNs aren't naturally textual). It is
	// populated by the connector's Listen implementation, not part of the
	// signal's own encoded payload.
	LSN []byte `json:"-"`
}

// Type returns the SignalType or an empty string if ControlSignal is nil.
func (s *ControlSignal) Type() string {
	if s != nil {
		return s.SignalType
	}
	return ""
}

// SnapshotSignalType requests an incremental snapshot of the named tables.
const SnapshotSignalType = "snapshot-execute"

// SnapshotSignal is the decoded "data" payload for a SnapshotSignalType
// signal. Names are resolved against the connector's configured schema.
type SnapshotSignal struct {
	Tables []string `json:"tables"`
}

// DecodeSignal builds a ControlSignal from a signal record's id, type and data
// fields. A log signal's data is parsed into LogSignal; a parse failure wraps
// ErrSignalRejected. Other types are returned as-is: IsKnownSignalType tells the
// caller whether to warn. LSN is left for the caller to set.
func DecodeSignal(id, signalType string, data []byte) (*ControlSignal, error) {
	sig := &ControlSignal{ID: id, SignalType: signalType}
	if signalType == LogSignalType {
		if err := json.Unmarshal(data, &sig.LogSignal); err != nil {
			return nil, fmt.Errorf("%w: parsing %s signal data: %w", ErrSignalRejected, LogSignalType, err)
		}
	}
	return sig, nil
}

// IsKnownSignalType reports whether t is a signal type this package defines.
func IsKnownSignalType(t string) bool {
	return t == LogSignalType || t == SnapshotSignalType
}

// ParseSnapshotSignal parses and validates a snapshot-execute payload. Bad JSON
// or an empty tables list wraps ErrSignalRejected.
func ParseSnapshotSignal(data []byte) (SnapshotSignal, error) {
	var signal SnapshotSignal
	if err := json.Unmarshal(data, &signal); err != nil {
		return SnapshotSignal{}, fmt.Errorf("%w: parsing %s payload: %w", ErrSignalRejected, SnapshotSignalType, err)
	}
	if len(signal.Tables) == 0 {
		return SnapshotSignal{}, fmt.Errorf("%w: %s payload lists no tables", ErrSignalRejected, SnapshotSignalType)
	}
	return signal, nil
}
