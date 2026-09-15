// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package logminer

import "slices"

// logKey identifies a redo log file by its physical identity rather than the
// SCN range it happens to cover, so the same file can be recognised across
// cycles even as FirstSCN/NextSCN shift for the still-open current log.
type logKey struct {
	thread   int
	sequence int64
}

// logFileSelector sizes the SCN range mined per LogMiner cycle by a bounded
// count of redo log files, instead of growing/shrinking an SCN-count window
// (see adaptWindowSize). This matters on databases where
// V$DATABASE.CURRENT_SCN can advance with little or no real transaction
// volume behind it (RAC cross-instance SCN sync, a CDB-shared SCN advanced
// by another PDB, Oracle's automatic maintenance window): the SCN-window
// strategy burns many cycles ramping up to its ceiling on ranges that are
// mostly empty, whereas a file-count budget absorbs the same churn in a
// single cycle regardless of the SCN span those files cover.
//
// The file budget only grows when the same set of files is selected two
// cycles in a row - a sign a long-running open transaction is pinning the
// window's start SCN - and is capped so a genuine transaction burst still
// stays bounded (each redo log file has a bounded max size).
//
// This is a single global budget, not RAC-aware: files aren't grouped by
// redo thread. Extending to RAC would need per-thread grouping and capping.
type logFileSelector struct {
	// minCount is the file budget's floor, and its starting point.
	minCount int
	// growthMax is the file budget's ceiling once grown due to a lack of
	// forward progress.
	growthMax int

	// count is the current file budget. Zero means uninitialized.
	count int
	// prevKeys holds the (thread, sequence) identities of the files selected
	// on the previous call, used to detect a lack of forward progress.
	prevKeys []logKey
}

// selectForSession picks the subset of files (already sorted ascending by
// SEQ and deduplicated between online/archived copies) to mine this cycle,
// bounded by the file-count budget. dbCurrentSCN becomes endSCN when the
// selection reaches the still-open current online log, whose NextSCN keeps
// advancing.
//
// capped reports whether endSCN came from a closed, bounded log file (true)
// rather than the database's live current SCN (false) - mirrors the hitCap
// signal used by the SCN-window strategy.
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
		// Same files selected again without the window advancing - likely a
		// long-open transaction pinning the start SCN. Grow the budget so a
		// future cycle can make progress.
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
