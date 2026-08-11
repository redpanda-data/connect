// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package snapshot

// currentStateVersion is bumped manually whenever the State shape changes in
// a way that requires migration handling by callers.
const currentStateVersion = 1

// State is the resumable, persistable state of a Coordinator. Callers are
// expected to serialize this (e.g. to JSON) as a checkpoint and pass it back
// into NewCoordinator to resume after a restart. Watermarks are deliberately
// not part of this struct: they must always be re-derived fresh on resume,
// never reused, since a persisted watermark could be arbitrarily stale.
type State struct {
	Version         int        `json:"version"`
	Done            bool       `json:"done"`
	CurrentTable    *TableID   `json:"current_table,omitempty"`
	LastSentPK      PrimaryKey `json:"last_sent_pk,omitempty"`
	MaxPK           PrimaryKey `json:"max_pk,omitempty"`
	RemainingTables []TableID  `json:"remaining_tables,omitempty"`
}

// Clone returns a deep-enough copy of the State for safe internal use. New
// slices/pointers are allocated, but PrimaryKey elements themselves are
// copied by value since they're expected to be JSON scalars (numbers,
// strings, bools, nil).
func (s *State) Clone() *State {
	if s == nil {
		return nil
	}

	clone := &State{
		Version: s.Version,
		Done:    s.Done,
	}

	if s.CurrentTable != nil {
		table := *s.CurrentTable
		clone.CurrentTable = &table
	}
	if s.LastSentPK != nil {
		clone.LastSentPK = append(PrimaryKey{}, s.LastSentPK...)
	}
	if s.MaxPK != nil {
		clone.MaxPK = append(PrimaryKey{}, s.MaxPK...)
	}
	if s.RemainingTables != nil {
		clone.RemainingTables = append([]TableID{}, s.RemainingTables...)
	}

	return clone
}
