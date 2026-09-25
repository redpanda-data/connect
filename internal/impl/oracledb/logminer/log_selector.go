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

// logFileSelector implements the log_count window strategy's file budget.
// count is a file-count value at the config layer, but internally denotes a
// number of online-redo-log-sized bytes (count * maxRedoLogSizeInBytes,
// passed into every call) rather than a literal file count - archived log
// sizes vary wildly in practice, so a flat file count makes "how much redo a
// cycle covers" unpredictable and often far too small.
//
// count is applied to each thread's file list independently (so bytes/cycle
// scales with open thread count on RAC), but is a single value shared across
// threads rather than one grown per thread: this keeps one thread's backlog
// from starving the others, and keeps the config surface (log_count_min /
// log_count_growth_max) one knob rather than one per thread on a topology
// that changes over time.
//
// Known follow-up: seed count from a previously-committed checkpoint SCN
// across a restart, rather than rediscovering it via stall/growth each time.
//
// prevUpperBoundSCN is the endSCN this selector last returned (0 = none yet,
// same convention as LogMiner.getCurrentSCN). It only ratchets forward (see
// selectForSession) and is used by extendThreadPastBoundary to stop a
// thread's backlog from being permanently skipped once currentSCN advances
// past a boundary its own budget never reached.
type logFileSelector struct {
	minCount          int
	growthMax         int
	count             int
	prevKeys          []logKey
	prevUpperBoundSCN uint64
}

// selectForSession picks the redo/archive log files to mine next. files is
// the SCN-overlap-filtered candidate set across all threads (see
// GetLogsBySCNRange); openThreads are the threads Oracle reports OPEN (see
// GetOpenThreads); maxRedoLogSizeInBytes is what the shared budget (s.count)
// is denominated in - the per-thread threshold is s.count * maxRedoLogSizeInBytes.
//
// An open thread with no files is an error, not a skip - it means the
// collector missed that thread's logs. A thread is complete once an open
// thread lands on its genuinely open current log (more may still be coming)
// or a closed thread covers everything available (it produces no more). The
// session is capped unless every thread is complete; when capped, endSCN is
// the smallest per-thread tightened boundary, so no thread's mined range
// outruns what was actually selected for it.
func (s *logFileSelector) selectForSession(files []*LogFile, openThreads []int, dbCurrentSCN, maxRedoLogSizeInBytes uint64) (selected []*LogFile, endSCN uint64, capped bool, err error) {
	if s.count == 0 {
		s.count = s.minCount
	}

	selected, endSCN, capped, truncated, budgetKeys, err := s.budgetPerThread(files, openThreads, dbCurrentSCN, maxRedoLogSizeInBytes)
	if err != nil {
		return nil, 0, false, err
	}

	// Stall detection compares the pre-extension budget selection, not the
	// extended one - extension is real progress, not a stall.
	if truncated && slices.Equal(budgetKeys, s.prevKeys) {
		// Same selection again with no progress - grow the budget. Rather
		// than a flat +1 (slow to clear a large backlog), derive the jump
		// that would clear it in one step when that's bigger than +1.
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

	// Ratchet prevUpperBoundSCN forward only - it's the floor the next
	// call's extension must reach past (see extendThreadPastBoundary).
	if endSCN > s.prevUpperBoundSCN {
		s.prevUpperBoundSCN = endSCN
	}

	return selected, endSCN, capped, nil
}

// deriveGrowthCount computes the count-equivalent budget that would clear,
// in one step, the largest backlog any thread has below prevUpperBoundSCN -
// so a stall doesn't take one increment per cycle to work through.
//
// FirstSCN, not NextSCN, decides whether a file counts toward that backlog:
// the question is whether the file lies within ground already committed to
// (the same question extendThreadPastBoundary asks), and comparing NextSCN
// would exclude exactly the files closest to the boundary that matter most.
//
// prevUpperBoundSCN == 0 means there's nothing to derive a jump from;
// returning minCount just lets selectForSession's own max() fall back to
// the flat +1 step.
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
			threadBytes += f.Bytes
		}
		maxThreadBytes = max(maxThreadBytes, threadBytes)
	}

	return max(s.minCount, ceilDiv(maxThreadBytes, maxRedoLogSizeInBytes))
}

// budgetPerThread applies the shared byte budget (s.count *
// maxRedoLogSizeInBytes) per thread, extends past prevUpperBoundSCN (see
// extendThreadPastBoundary), then combines the results. truncated reports
// whether the budget alone (before extension) cut any thread's files,
// regardless of whether the session ends up capped - this, via the
// pre-extension budgetKeys, is what stall detection compares against in
// selectForSession, since extension catching a thread up is progress, not a
// stall.
func (s *logFileSelector) budgetPerThread(files []*LogFile, openThreads []int, dbCurrentSCN, maxRedoLogSizeInBytes uint64) (selected []*LogFile, endSCN uint64, capped, truncated bool, budgetKeys []logKey, err error) {
	byThread := groupFilesByThread(files)

	// An open thread with zero files means the collector missed its logs -
	// mining an incomplete view silently would be worse than erroring.
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

		// Accumulate bytes in order, stopping once the running total reaches
		// the threshold - inclusive of the crossing file, so one oversized
		// file still selects itself rather than nothing.
		var accumulated uint64
		stopIdx := len(threadFiles)
		for i, f := range threadFiles {
			accumulated += f.Bytes
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

		// A closed thread is complete once it covers everything available;
		// an open thread needs its genuinely open current log, since more
		// redo may still be coming. An incomplete thread must tighten
		// endSCN, or its unselected tail could drop out of a future
		// GetLogsBySCNRange window.
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
		// Nothing to cap - every thread fits within budget (extension is
		// then a no-op too).
		return files, dbCurrentSCN, false, false, budgetKeys, nil
	}
	if allCaughtUp {
		return combined, dbCurrentSCN, false, true, budgetKeys, nil
	}
	return combined, tightestEndSCN, true, true, budgetKeys, nil
}

// extendThreadPastBoundary extends a thread's capped selection once a prior
// cycle has committed to mining up through prevUpperBoundSCN, so a thread
// whose backlog exceeds the budget doesn't have its unselected tail
// permanently drop out of a future GetLogsBySCNRange window.
//
// This can push a selection past growthMax: re-covering committed ground is
// unsafe to skip, whereas exceeding the growth ceiling only costs extra
// files for one cycle. growthMax bounds automatic growth, not the total.
//
// prevUpperBoundSCN == 0 means no boundary committed yet (first call), so
// there's nothing to extend past.
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

	// A non-archived last file (still online, still mutable) isn't a safe
	// stopping point - pull in the rest of this thread's files too.
	if last := extended[len(extended)-1]; !last.IsArchived() {
		extended = append(extended, threadFiles[nextIndex:]...)
	}

	return extended
}

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

// ceilDiv rounds numerator/denominator up, so a partially-filled unit of
// backlog still counts as needing a whole extra unit of budget.
func ceilDiv(numerator, denominator uint64) int {
	if denominator == 0 {
		return 0
	}
	return int((numerator + denominator - 1) / denominator)
}
