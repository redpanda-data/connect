// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package incrementalsnapshot

import (
	"bytes"
	"encoding/json"
)

// CurrentStateVersion is bumped manually whenever the State shape changes in
// a way that requires migration handling by callers.
const CurrentStateVersion = 1

// State is the resumable, persistable state of an incremental snapshot
// coordinator, serialized (e.g. to JSON) as a checkpoint. Watermarks are
// deliberately excluded: they must always be re-derived fresh on resume,
// never reused, since a persisted one could be arbitrarily stale.
type State struct {
	Version         int        `json:"version"`
	Done            bool       `json:"done"`
	CurrentTable    *TableID   `json:"current_table,omitempty"`
	LastSentPK      PrimaryKey `json:"last_sent_pk,omitempty"`
	MaxPK           PrimaryKey `json:"max_pk,omitempty"`
	RemainingTables []TableID  `json:"remaining_tables,omitempty"`
}

// UnmarshalJSON decodes a checkpoint, preserving integer primary keys
// exactly.
//
// PrimaryKey is []any, so the stock decoder turns every JSON number into a
// float64 and silently rounds anything above 2^53 -- which is the ordinary
// case for a bigint key (snowflake ids and the like). Encoding is exact, so
// the damage only appears on resume: a MaxPK that rounds down excludes every
// row between the rounded and true maximum from `pk <= max` for good, and
// they are never backfilled. A rounded LastSentPK re-delivers rows instead.
//
// Decoding with UseNumber and converting back to int64 keeps integer keys
// bit-exact. Keys that don't fit an int64 (a wide NUMERIC, say) still fall
// back to float64 and remain lossy; storing a typed encoding would be the
// fix if such a key ever needs supporting.
func (s *State) UnmarshalJSON(data []byte) error {
	// Shadow type without this method, to avoid recursing.
	type stateJSON State

	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()

	var raw stateJSON
	if err := dec.Decode(&raw); err != nil {
		return err
	}

	*s = State(raw)
	narrowPrimaryKey(s.LastSentPK)
	narrowPrimaryKey(s.MaxPK)
	return nil
}

// narrowPrimaryKey converts the json.Number elements UseNumber leaves behind
// into the Go types the write path produced, in place.
func narrowPrimaryKey(pk PrimaryKey) {
	for i, v := range pk {
		num, ok := v.(json.Number)
		if !ok {
			continue
		}
		if intVal, err := num.Int64(); err == nil {
			pk[i] = intVal
			continue
		}
		if floatVal, err := num.Float64(); err == nil {
			pk[i] = floatVal
			continue
		}
		// Neither representation fits. Keep the digits rather than drop the
		// element, so a bad checkpoint surfaces as a query error instead of a
		// silently shifted bound.
		pk[i] = num.String()
	}
}

// Clone returns a deep-enough copy for safe internal use: new
// slices/pointers, but PrimaryKey elements are copied by value since they're
// expected to be JSON scalars.
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
