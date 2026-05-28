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
	"sync/atomic"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// ControlSignal represents a insert into the signal table.
type ControlSignal struct {
	ID              string
	Type            string
	DataCollections []string `json:"data-collections"`

	// LSN is the log sequence number/offset the signal was observed at, in
	// whatever raw form the connector's replication stream represents
	// positions (e.g. a decimal/hex string, or raw binary bytes for
	// connectors like Oracle whose SCNs aren't naturally textual). It is
	// populated by the connector's Listen implementation, not part of the
	// signal's own encoded payload.
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
// Listen inspects a decoded replication message in whatever shape that
// connector's stream produces and, if it recognizes a signal insert, records
// it as pending and notifies OnSignal.
//
// ControlSignaller intentionally does not implement Listen itself: it has no
// way to know the shape of a connector's replication messages, so embedding
// it alone does not satisfy this interface. Connectors must embed
// ControlSignaller and provide their own Listen (see
// internal/impl/postgresql/signaller.go for an example) — this way, a
// connector that forgets to do so fails to compile rather than panicking at
// runtime.
type Signaller interface {
	Listen(ctx context.Context, event any) error
	OnSignal() <-chan *ControlSignal
	IsPending() (bool, *ControlSignal)
	Reset()
}

// ControlSignaller can be used to handle and process control signals.
//
// Listen implementations should notify via SignalChan as soon as a signal is
// observed, but must not call StoreSignal until the signal (and anything
// batched ahead of it) has actually been acknowledged downstream — otherwise
// a caller relying on IsPending to decide whether to act on the signal (e.g.
// re-running a snapshot) could act on it before it's durably delivered.
type ControlSignaller struct {
	SignalChan chan *ControlSignal
	Log        *service.Logger

	signalPending atomic.Pointer[ControlSignal]
}

// NewControlSignaller creates a ControlSignaller for detecting signal INSERTs
// on the given schema.tableName. Embed the result in a connector-specific type
// that implements Listen to obtain a full Signaller.
func NewControlSignaller(log *service.Logger) *ControlSignaller {
	return &ControlSignaller{
		SignalChan: make(chan *ControlSignal, 1),
		Log:        log,
	}
}

// OnSignal indicates whether a signal has been received.
func (o *ControlSignaller) OnSignal() <-chan *ControlSignal {
	return o.SignalChan
}

// IsPending determines if there's a signal current pending.
func (o *ControlSignaller) IsPending() (bool, *ControlSignal) {
	sig := o.signalPending.Load()
	return sig != nil, sig
}

// Reset resets the current captured signal.
func (o *ControlSignaller) Reset() {
	o.signalPending.Store(nil)
}

// StoreSignal stores the published signal ready for reading when needed.
func (o *ControlSignaller) StoreSignal(sig *ControlSignal) {
	o.signalPending.Store(sig)
}
