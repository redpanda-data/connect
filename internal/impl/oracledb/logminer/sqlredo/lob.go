// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package sqlredo

import (
	"fmt"
	"sort"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// FormatPKString returns a deterministic string representation of a PK values
// map suitable for use as a map key. Keys are sorted alphabetically and values
// are formatted as "K1=V1;K2=V2".
func FormatPKString(pkValues map[string]any) string {
	keys := make([]string, 0, len(pkValues))
	for k := range pkValues {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+fmt.Sprintf("%v", pkValues[k]))
	}
	return strings.Join(parts, ";")
}

// LobKey uniquely identifies a LOB accumulator within a transaction.
// PKString is a stable string representation of the PK values map used as a map key.
type LobKey struct {
	Schema   string
	Table    string
	Column   string
	PKString string
}

// LobFragment is a single LOB_WRITE chunk with its 1-based Oracle offset.
type LobFragment struct {
	Offset int64
	Data   []byte
}

// LobAccumulator collects LOB_WRITE fragments for a single LOB column value
// and assembles them into the complete value on commit.
type LobAccumulator struct {
	Schema    string
	Table     string
	Column    string
	IsBinary  bool
	PKValues  map[string]any
	Fragments []LobFragment
}

// AddFragment appends a fragment.
func (a *LobAccumulator) AddFragment(offset int64, data []byte) {
	a.Fragments = append(a.Fragments, LobFragment{Offset: offset, Data: data})
}

// Assemble assembles all fragments into the final column value:
//   - BLOB → []byte (raw bytes, gaps zero-filled)
//   - CLOB → string (plain string, gaps space-filled)
//   - NCLOB → string (plain string from LOB_WRITE string literal, gaps space-filled)
//
// Returns nil when no fragments have been added.
func (a *LobAccumulator) Assemble() any {
	if len(a.Fragments) == 0 {
		return nil
	}

	var totalLen int64
	for _, f := range a.Fragments {
		end := (f.Offset - 1) + int64(len(f.Data))
		if end > totalLen {
			totalLen = end
		}
	}

	result := make([]byte, totalLen)
	if !a.IsBinary {
		// Fill with spaces for CLOB/NCLOB gaps.
		for i := range result {
			result[i] = ' '
		}
	}

	for _, f := range a.Fragments {
		start := f.Offset - 1 // convert 1-based offset to 0-based
		copy(result[start:], f.Data)
	}

	switch {
	case a.IsBinary:
		return result
	default:
		// CLOB and NCLOB: Oracle delivers data as plain string literals in LOB_WRITE SQL.
		return string(result)
	}
}

// TxnLOBState tracks LOB accumulation state for a single in-flight transaction.
type TxnLOBState struct {
	ActiveKey    *LobKey
	Accumulators map[LobKey]*LobAccumulator
}

// NewTxnLOBState creates a new TxnLOBState.
func NewTxnLOBState() *TxnLOBState {
	return &TxnLOBState{Accumulators: make(map[LobKey]*LobAccumulator)}
}

// MergeLOBsIntoDMLEvents matches each LOB accumulator to its corresponding DML
// event (by schema, table, and PK values) and overwrites the LOB column value
// with the assembled data.
//
// For small LOBs stored inline, Oracle emits both the original INSERT (with empty
// LOB placeholders) and a subsequent LOB-initialisation UPDATE (with only LOB columns).
// To ensure the LOB values land on the INSERT rather than the UPDATE, this function
// first searches forward for an INSERT event with a matching PK, then falls back to
// the most-recent matching DML event of any type.
func MergeLOBsIntoDMLEvents(state *TxnLOBState, events []*DMLEvent, log *service.Logger) {
	for _, acc := range state.Accumulators {
		assembled := acc.Assemble()
		if assembled == nil {
			if log != nil {
				log.Debugf("LOB merge: skipping %s.%s.%s — no fragments accumulated", acc.Schema, acc.Table, acc.Column)
			}
			continue
		}

		merged := false

		// Prefer merging into an INSERT so that Oracle's internal LOB-initialisation
		// UPDATE (which only carries LOB columns) does not shadow the original INSERT.
		for i := 0; i < len(events); i++ {
			ev := events[i]
			if ev.Operation != OpInsert {
				continue
			}
			if ev.Schema != acc.Schema || ev.Table != acc.Table {
				continue
			}
			if pkMatches(ev.Data, acc.PKValues) {
				ev.Data[acc.Column] = assembled
				merged = true
				if log != nil {
					log.Debugf("LOB merge: set %s.%s.%s into INSERT (pks=%v, fragments=%d)", acc.Schema, acc.Table, acc.Column, acc.PKValues, len(acc.Fragments))
				}
				break
			}
		}

		if merged {
			continue
		}

		// Fall back to the most-recent matching DML event of any operation type.
		for i := len(events) - 1; i >= 0; i-- {
			ev := events[i]
			if ev.Schema != acc.Schema || ev.Table != acc.Table {
				continue
			}
			if pkMatches(ev.Data, acc.PKValues) {
				ev.Data[acc.Column] = assembled
				merged = true
				if log != nil {
					log.Debugf("LOB merge: set %s.%s.%s (pks=%v, fragments=%d)", acc.Schema, acc.Table, acc.Column, acc.PKValues, len(acc.Fragments))
				}
				break
			}
		}

		if !merged && log != nil {
			log.Debugf("LOB merge: no matching DML event found for %s.%s.%s (pks=%v)", acc.Schema, acc.Table, acc.Column, acc.PKValues)
		}
	}
}

// MergeInlineLOBValues merges LOB column values from an inline-LOB-only UPDATE into the
// matching INSERT event(s) for the same row. Oracle emits exactly one LOB-init UPDATE
// per INSERT row, so PK matching is skipped — values are merged into all INSERT events
// for the same schema.table.
//
// This handles Oracle's behaviour of omitting LOB columns from INSERT SQL_REDO and
// instead emitting a separate UPDATE whose SET clause carries the actual LOB data.
func MergeInlineLOBValues(lobData map[string]any, schema, table string, events []*DMLEvent, log *service.Logger) {
	for _, ev := range events {
		if ev.Operation != OpInsert {
			continue
		}
		if ev.Schema != schema || ev.Table != table {
			continue
		}
		for col, val := range lobData {
			ev.Data[col] = val
		}
		if log != nil {
			log.Debugf("inline LOB merge: set %d LOB columns into INSERT for %s.%s", len(lobData), schema, table)
		}
	}
}

// pkMatches returns true when every key in pkValues is present in data and the
// string representations are equal.
func pkMatches(data map[string]any, pkValues map[string]any) bool {
	for k, pkVal := range pkValues {
		dataVal, ok := data[k]
		if !ok {
			return false
		}
		if fmt.Sprintf("%v", dataVal) != fmt.Sprintf("%v", pkVal) {
			return false
		}
	}
	return true
}
