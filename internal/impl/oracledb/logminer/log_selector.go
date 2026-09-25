// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package logminer

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
)

type logKey struct {
	thread   int
	sequence int64
}

// logFileSelector implements the redo_volume window strategy's file budget.
// count is a config-level file count, but denotes online-redo-log-sized
// bytes internally (count * maxRedoLogSizeInBytes) - archived log sizes vary
// too much for a flat file count to give a predictable amount of redo per
// cycle.
//
// count is shared across threads rather than grown per thread, so one
// thread's backlog can't starve the others, and the config stays a single
// knob rather than one per thread on a topology that changes over time.
//
// Follow-up: seed count from a checkpointed SCN across restarts instead of
// rediscovering it via stall/growth each time.
//
// prevUpperBoundSCN is the last endSCN returned (0 = none yet). It only
// ratchets forward and is used by extendThreadPastBoundary to stop a
// thread's backlog being skipped once currentSCN passes a boundary its own
// budget never reached.
type logFileSelector struct {
	minCount          int
	growthMax         int
	count             int
	prevKeys          []logKey
	prevUpperBoundSCN uint64
}

// selectForSession picks the redo/archive log files to mine next. files is
// the candidate set across all threads (GetLogsBySCNRange); openThreads are
// Oracle's OPEN threads (GetOpenThreads); maxRedoLogSizeInBytes sizes the
// per-thread threshold (s.count * maxRedoLogSizeInBytes).
//
// An open thread with no files is an error, not a skip. A thread is
// complete once an open thread reaches its genuinely open current log, or a
// closed thread covers everything available. The session is capped unless
// every thread is complete; endSCN is then the smallest per-thread
// tightened boundary.
func (s *logFileSelector) selectForSession(files []*LogFile, openThreads []int, dbCurrentSCN, maxRedoLogSizeInBytes uint64) (selected []*LogFile, endSCN uint64, capped bool, err error) {
	if s.count == 0 {
		s.count = s.minCount
	}

	selected, endSCN, capped, truncated, budgetKeys, err := s.budgetPerThread(files, openThreads, dbCurrentSCN, maxRedoLogSizeInBytes)
	if err != nil {
		return nil, 0, false, err
	}

	// Compare the pre-extension selection, not the extended one - extension
	// is progress, not a stall.
	if truncated && slices.Equal(budgetKeys, s.prevKeys) {
		// No progress - grow the budget. Derive the jump that would clear
		// the backlog in one step, rather than a flat +1.
		derived := s.deriveGrowthCount(files, maxRedoLogSizeInBytes)
		growTo := max(derived, s.count+1)
		if ceiling := max(s.growthMax, s.minCount); growTo > ceiling {
			growTo = ceiling
		}
		s.count = growTo

		if selected, endSCN, capped, truncated, budgetKeys, err = s.budgetPerThread(files, openThreads, dbCurrentSCN, maxRedoLogSizeInBytes); err != nil {
			return nil, 0, false, err
		}
	}

	if truncated {
		s.prevKeys = budgetKeys
	} else {
		s.prevKeys = nil
	}

	// Ratchet forward only - the floor extendThreadPastBoundary must clear.
	if endSCN > s.prevUpperBoundSCN {
		s.prevUpperBoundSCN = endSCN
	}

	return selected, endSCN, capped, nil
}

// deriveGrowthCount computes the count that would clear, in one step, the
// largest backlog any thread has below prevUpperBoundSCN.
//
// FirstSCN decides whether a file counts, not NextSCN - the question is
// whether the file lies within already-committed ground, and NextSCN would
// exclude the files closest to the boundary.
//
// prevUpperBoundSCN == 0 means nothing to derive; minCount lets the flat +1
// fallback apply instead.
func (s *logFileSelector) deriveGrowthCount(files []*LogFile, maxRedoLogSizeInBytes uint64) int {
	if s.prevUpperBoundSCN == 0 {
		return s.minCount
	}

	var maxThreadBytes uint64
	for _, threadFiles := range groupFilesByThread(files) {
		var threadBytes uint64
		for _, f := range threadFiles {
			if f.FirstSCN >= s.prevUpperBoundSCN {
				break
			}
			threadBytes += f.SizeBytes
		}
		maxThreadBytes = max(maxThreadBytes, threadBytes)
	}

	return max(s.minCount, ceilDiv(maxThreadBytes, maxRedoLogSizeInBytes))
}

// budgetPerThread applies the shared byte budget per thread, extends past
// prevUpperBoundSCN, then combines the results. truncated reports whether
// the budget alone cut any thread's files - via the pre-extension
// budgetKeys, this is what stall detection compares against, since
// extension catching a thread up is progress, not a stall.
func (s *logFileSelector) budgetPerThread(files []*LogFile, openThreads []int, dbCurrentSCN, maxRedoLogSizeInBytes uint64) (selected []*LogFile, endSCN uint64, capped, truncated bool, budgetKeys []logKey, err error) {
	byThread := groupFilesByThread(files)

	// A missing thread here means the collector missed its logs - erroring
	// beats mining an incomplete view silently.
	for _, t := range openThreads {
		if len(byThread[t]) == 0 {
			return nil, 0, false, false, nil, fmt.Errorf("open redo thread %d has no log files in the collected SCN range", t)
		}
	}

	openSet := make(map[int]struct{}, len(openThreads))
	for _, t := range openThreads {
		openSet[t] = struct{}{}
	}

	threshold := uint64(s.count) * maxRedoLogSizeInBytes

	var (
		combined       []*LogFile
		budgetCombined []*LogFile
		tightestEndSCN uint64
		haveTightest   bool
		allCaughtUp    = true
	)
	for _, t := range slices.Sorted(maps.Keys(byThread)) {
		threadFiles := byThread[t]

		// Stop once bytes reach the threshold, inclusive of the crossing
		// file, so one oversized file still selects itself.
		var accumulated uint64
		stopIdx := len(threadFiles)
		for i, f := range threadFiles {
			accumulated += f.SizeBytes
			if accumulated >= threshold {
				stopIdx = i + 1
				break
			}
		}
		budgetCapped := threadFiles
		if stopIdx < len(threadFiles) {
			truncated = true
			budgetCapped = threadFiles[:stopIdx]
		}
		budgetCombined = append(budgetCombined, budgetCapped...)

		extended := extendThreadPastBoundary(threadFiles, budgetCapped, s.prevUpperBoundSCN)
		combined = append(combined, extended...)

		// A closed thread is complete once fully covered; an open thread
		// needs its current log, since more may be coming. Incomplete
		// threads tighten endSCN so their tail can't drop out later.
		last := extended[len(extended)-1]
		_, open := openSet[t]
		var caughtUp bool
		if open {
			caughtUp = last.IsOpenCurrent()
		} else {
			caughtUp = len(extended) == len(threadFiles)
		}
		if caughtUp {
			continue
		}

		allCaughtUp = false
		if candidateEnd := last.NextSCN - 1; !haveTightest || candidateEnd < tightestEndSCN {
			tightestEndSCN = candidateEnd
			haveTightest = true
		}
	}

	budgetKeys = logKeysOf(budgetCombined)

	if !truncated {
		// Nothing to cap - everyone fits (extension is then a no-op too).
		return files, dbCurrentSCN, false, false, budgetKeys, nil
	}
	if allCaughtUp {
		return combined, dbCurrentSCN, false, true, budgetKeys, nil
	}
	return combined, tightestEndSCN, true, true, budgetKeys, nil
}

// extendThreadPastBoundary extends a thread's capped selection past
// prevUpperBoundSCN, so a thread whose backlog exceeds the budget doesn't
// have its tail drop out of a future window.
//
// Can exceed growthMax: re-covering committed ground is unsafe to skip,
// while exceeding the ceiling only costs extra files for one cycle.
//
// prevUpperBoundSCN == 0 means nothing committed yet, so nothing to extend.
func extendThreadPastBoundary(threadFiles, budgetCapped []*LogFile, prevUpperBoundSCN uint64) []*LogFile {
	if prevUpperBoundSCN == 0 {
		return budgetCapped
	}

	extended := slices.Clone(budgetCapped)
	nextIndex := len(extended)
	for nextIndex < len(threadFiles) {
		last := extended[len(extended)-1]
		if last.IsOpenCurrent() || last.NextSCN > prevUpperBoundSCN {
			break
		}
		extended = append(extended, threadFiles[nextIndex])
		nextIndex++
	}

	// A non-archived (still mutable) last file isn't safe to stop at.
	if last := extended[len(extended)-1]; !last.IsArchived() {
		extended = append(extended, threadFiles[nextIndex:]...)
	}

	return extended
}

// groupFilesByThread buckets files by thread, sorted by Sequence - the logic
// above assumes an ordered prefix per thread, which files alone doesn't
// guarantee (deduplicateLogs can interleave sequences when one is missing
// from the archived branch).
func groupFilesByThread(files []*LogFile) map[int][]*LogFile {
	groups := make(map[int][]*LogFile)
	for _, f := range files {
		groups[f.Thread] = append(groups[f.Thread], f)
	}
	for _, group := range groups {
		slices.SortFunc(group, func(a, b *LogFile) int {
			return cmp.Compare(a.Sequence, b.Sequence)
		})
	}
	return groups
}

func logKeysOf(files []*LogFile) []logKey {
	keys := make([]logKey, len(files))
	for i, f := range files {
		keys[i] = logKey{thread: f.Thread, sequence: f.Sequence}
	}
	return keys
}

// ceilDiv rounds up, so a partial unit of backlog still needs a whole unit.
func ceilDiv(numerator, denominator uint64) int {
	if denominator == 0 {
		return 0
	}
	return int((numerator + denominator - 1) / denominator)
}
