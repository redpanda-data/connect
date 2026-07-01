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
	return s.Type == "execute-snapshot"
}

// FilterTables returns the subset of tables from the provided list that appear
// in DataCollections for the given schema. Matching is case-insensitive.
// If DataCollections is empty, all tables are returned unchanged.
func (s *ControlSignal) FilterTables(schema string, tables []string) []string {
	if len(s.DataCollections) == 0 {
		return tables
	}
	target := make(map[string]struct{}, len(s.DataCollections))
	for _, dc := range s.DataCollections {
		// DataCollections entries are "schema.table"; strip the schema prefix.
		table := dc
		if idx := strings.LastIndex(dc, "."); idx >= 0 {
			if !strings.EqualFold(dc[:idx], schema) {
				continue
			}
			table = dc[idx+1:]
		}
		target[strings.ToLower(table)] = struct{}{}
	}
	filtered := make([]string, 0, len(tables))
	for _, t := range tables {
		if _, ok := target[strings.ToLower(t)]; ok {
			filtered = append(filtered, t)
		}
	}
	return filtered
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
