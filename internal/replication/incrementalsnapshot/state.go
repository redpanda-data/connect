// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package incrementalsnapshot

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

// CurrentStateVersion contains the version of the state.
const CurrentStateVersion = 1

// ErrUnsupportedStateVersion reports a checkpoint written by a build using a
// different State layout.
var ErrUnsupportedStateVersion = errors.New("unsupported incremental snapshot state version")

// State is a coordinator's resumable state, stored as a checkpoint. It holds
// no watermark: a persisted one could be arbitrarily stale, so watermarks are
// always re-derived on resume.
type State struct {
	Version         int        `json:"version"`
	Done            bool       `json:"done"`
	CurrentTable    *TableID   `json:"current_table,omitempty"`
	LastSentPK      PrimaryKey `json:"last_sent_pk,omitempty"`
	MaxPK           PrimaryKey `json:"max_pk,omitempty"`
	RemainingTables []TableID  `json:"remaining_tables,omitempty"`
}

// UnmarshalJSON decodes a checkpoint, keeping integer keys exact.
//
// PrimaryKey is []any, so the stock decoder makes a float64 of every JSON
// number and rounds anything above 2^53 -- ordinary for a bigint key.
// Encoding is exact, so the damage only shows on resume: a MaxPK that rounds
// down excludes rows from `pk <= max` for good, while a rounded LastSentPK
// re-delivers them.
//
// UseNumber plus a conversion back to int64 fixes integer keys. A key too
// large for int64, such as a wide NUMERIC, stays a lossy float64; that would
// need a typed encoding.
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

	if s.Version != CurrentStateVersion {
		return fmt.Errorf("%w: got %d, want %d", ErrUnsupportedStateVersion, s.Version, CurrentStateVersion)
	}
	return nil
}

// narrowPrimaryKey converts each json.Number back to the type the encoder
// wrote, in place.
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
		// Fits no number type. Keep the digits so a bad checkpoint fails
		// loudly at query time rather than shifting the bound silently.
		pk[i] = num.String()
	}
}

// Clone returns a deep-enough copy: new slices and pointers, with PrimaryKey
// elements copied by value since they are JSON scalars.
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
