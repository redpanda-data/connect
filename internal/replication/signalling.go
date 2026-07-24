// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication

import (
	"context"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// ControlSignal represents a insert into the signal table.
type ControlSignal struct {
	ID              string
	Type            string
	DataCollections []string `json:"data-collections"`

	// LSN is the position the signal was observed at, in whatever raw form
	// the connector represents positions. Populated by Listen, not part of
	// the signal's own payload.
	LSN []byte `json:"-"`
}

// IsSnapshot returns true if the ControlSignal is a snapshot signal.
func (s *ControlSignal) IsSnapshot() bool {
	if s == nil {
		return false
	}

	return s.Type == "execute-snapshot"
}

// Signaller is implemented by connector-specific control signal handlers.
// Listen inspects a decoded replication message and, if it recognizes an
// actionable signal, returns it so the caller can flush and hold back
// acking exactly that message before running the signal's action. Only
// signals whose action could be interrupted by a crash need this: anything
// else should return (nil, nil) and be acknowledged like an ordinary message.
//
// ControlSignaller does not implement Listen itself, since it has no way to
// know the shape of a connector's replication messages - connectors must
// embed it and provide their own Listen (see
// internal/impl/postgresql/signaller.go for an example).
type Signaller interface {
	Listen(ctx context.Context, event any) (*ControlSignal, error)
}

// ControlSignaller can be used to handle and process control signals.
// Currently just a thin holder for the logger; embed it in a
// connector-specific type that implements Listen to obtain a full Signaller.
type ControlSignaller struct {
	Log *service.Logger
}

// NewControlSignaller creates a ControlSignaller. Embed the result in a
// connector-specific type that implements Listen to obtain a full Signaller.
func NewControlSignaller(log *service.Logger) *ControlSignaller {
	return &ControlSignaller{
		Log: log,
	}
}
