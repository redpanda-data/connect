// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication

import "context"

var (
	DefaultSignalTableName = "rpcn_signal_table"
)

type EventSignal struct {
	ID              string
	Type            string
	DataCollections []string `json:"data-collections"`
}

type Signaller interface {
	// ValidateChannel validates the signal channel exists during connector startup.
	ValidateChannel(ctx context.Context) error
	// Listen detects whether a signal has been received from the channel.
	// Returns true if the event was a signal and should not be published into the pipeline.
	Listen(ctx context.Context, event any) (bool, error)
	// OnSignal returns a channel that receives the LSN of the triggering event each time a signal is detected.
	OnSignal() <-chan *string

	IsPending() bool
	Reset()
}
