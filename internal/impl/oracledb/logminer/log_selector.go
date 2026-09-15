// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package logminer

import "slices"

type logKey struct {
	thread   int
	sequence int64
}

type logFileSelector struct {
	minCount  int
	growthMax int
	count     int
	prevKeys  []logKey
}

func (s *logFileSelector) selectForSession(files []*LogFile, dbCurrentSCN uint64) (selected []*LogFile, endSCN uint64, capped bool) {
	if s.count == 0 {
		s.count = s.minCount
	}

	if len(files) <= s.count {
		// Nothing to cap - the whole overlapping range fits within budget.
		s.prevKeys = nil
		return files, dbCurrentSCN, false
	}

	candidate := files[:s.count]

	if logKeysEqual(logKeysOf(candidate), s.prevKeys) {
		// Same files selected again without progress (see the stall note
		// above) - grow the budget so a future cycle can advance.
		growTo := s.count + 1
		if ceiling := max(s.growthMax, s.minCount); growTo > ceiling {
			growTo = ceiling
		}
		s.count = growTo

		if len(files) <= s.count {
			s.prevKeys = nil
			return files, dbCurrentSCN, false
		}
		candidate = files[:s.count]
	}

	s.prevKeys = logKeysOf(candidate)

	last := candidate[len(candidate)-1]
	if last.IsOpenCurrent() {
		return candidate, dbCurrentSCN, false
	}

	return candidate, last.NextSCN - 1, true
}

func logKeysOf(files []*LogFile) []logKey {
	keys := make([]logKey, len(files))
	for i, f := range files {
		keys[i] = logKey{thread: f.Thread, sequence: f.Sequence}
	}
	return keys
}

func logKeysEqual(a, b []logKey) bool {
	return slices.Equal(a, b)
}
