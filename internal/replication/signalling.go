// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication

import "context"

// LogSignalType represents a log signal.
const LogSignalType = "log"

// ControlSignal represents a insert into the signal table.
type ControlSignal struct {
	ID         string
	SignalType string
	Message    string `json:"message"`

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

// Signaller is implemented by connector-specific control signal handlers.
// Listen inspects a decoded replication message and, if it recognizes an
// actionable signal, returns it directly - in the same call that detected it
// - so the caller can flush exactly that batch before acting on it, rather
// than reacting to a separately-scheduled notification a differently-timed
// flush could race ahead of.
type Signaller interface {
	Listen(ctx context.Context, event any) (*ControlSignal, error)
}
