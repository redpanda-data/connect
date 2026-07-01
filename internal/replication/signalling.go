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
	"strings"
)

// ControlSignal represents a insert into the signal table.
type ControlSignal struct {
	ID              string
	Type            string
	DataCollections []string `json:"data-collections"`
}

// IsSnapshot returns true if the ControlSignal is a snapshot signal.
func (s *ControlSignal) IsSnapshot() bool {
	if s == nil {
		return false
	}

	return s.Type == "execute-snapshot"
}

// TableNames extracts the table name portion from each DataCollections entry that
// belongs to the given schema. Entries for a different schema are skipped. If
// DataCollections is empty, nil is returned.
func (s *ControlSignal) TableNames(schema string) []string {
	if len(s.DataCollections) == 0 {
		return nil
	}
	tables := make([]string, 0, len(s.DataCollections))
	for _, dc := range s.DataCollections {
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

// Signaller detects and communicates signal events from a configured signal channel.
type Signaller interface {
	// Listen detects whether a signal has been received from the channel.
	// Returns true if the event was a signal and should not be published into the pipeline.
	Listen(ctx context.Context, signal any) (bool, error)
	// OnSignal returns a channel that receives the LSN of the triggering event each time a signal is detected.
	OnSignal() <-chan *string
	// IsPending informs the caller whether a signal is being processed.
	IsPending() (bool, *ControlSignal)
	// Reset indicates the control signal has been handled and resets the signaller ready for the next signal.
	Reset()
}
