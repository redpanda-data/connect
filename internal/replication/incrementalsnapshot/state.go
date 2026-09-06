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

// CurrentStateVersion increases when the fields of State change and the
// caller must migrate old data. Change it by hand.
const CurrentStateVersion = 1

// State is the state of a coordinator that a later run can resume from. The
// caller stores it as a checkpoint, for example in JSON.
//
// State holds no watermark. A stored watermark can be very old, so the
// coordinator must always read a new watermark after a resume.
type State struct {
	Version         int        `json:"version"`
	Done            bool       `json:"done"`
	CurrentTable    *TableID   `json:"current_table,omitempty"`
	LastSentPK      PrimaryKey `json:"last_sent_pk,omitempty"`
	MaxPK           PrimaryKey `json:"max_pk,omitempty"`
	RemainingTables []TableID  `json:"remaining_tables,omitempty"`
}

// UnmarshalJSON decodes a checkpoint. It keeps integer primary keys exact.
//
// PrimaryKey is a slice of any. The standard decoder therefore makes a
// float64 from each JSON number, and a float64 rounds all values above 2^53.
// Many bigint keys are larger than this value.
//
// The encoder writes exact values, so the error occurs only after a resume.
// A MaxPK that rounds down is too small. The query `pk <= max` then excludes
// all rows between the rounded value and the true maximum, and the snapshot
// never reads them. A LastSentPK that rounds down delivers rows again.
//
// This method decodes with UseNumber and then makes an int64 from each
// number. Integer keys are then exact. A key that is too large for an int64,
// such as a wide NUMERIC key, is still a float64 and is not exact. A typed
// format is necessary to support such a key.
func (s *State) UnmarshalJSON(data []byte) error {
	// This type has no UnmarshalJSON method, so the decoder does not call
	// this method again.
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

// narrowPrimaryKey changes each json.Number element to the Go type that the
// encoder wrote. It changes the elements in place.
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
		// The value fits no number type. Keep the digits as text and do not
		// remove the element. A bad checkpoint then causes a query error
		// and does not move the bound without a message.
		pk[i] = num.String()
	}
}

// Clone returns a copy that is safe for use in this package. It makes new
// slices and pointers. It copies each PrimaryKey element by value, because
// each element is a simple JSON value.
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
