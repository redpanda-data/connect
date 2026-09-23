// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package logminer

import (
	"fmt"
	"maps"
	"slices"
)

type logKey struct {
	thread   int
	sequence int64
}

// logFileSelector implements the log_count window strategy's file budget.
// count is a file-count value at the config layer (log_count_min /
// log_count_growth_max), but internally it is interpreted as a number of
// online-redo-log-sized bytes, not a literal file count: count *
// maxRedoLogSizeInBytes (passed into every call, see selectForSession) gives
// the byte threshold each thread's selection is capped to. This matters
// because archived log file sizes in practice are wildly non-uniform - short
// bursts of activity between log switches produce tiny archived files,
// while busy periods fill a file to the configured online redo log size -
// so truncating to a flat file count made "how much real redo a cycle
// covers" unpredictable and, in the common case of many small files, far too
// small. Budgeting by bytes instead targets the thing that actually matters
// for session-restart overhead: how much real log data one cycle covers,
// independent of how many discrete files that happens to span.
//
// count is applied to every redo thread's own sorted file list independently
// (so the effective bytes-per-cycle scales with the number of open threads
// on RAC), but is itself a single shared value rather than one sized and
// grown per thread. Applying the budget per thread, rather than splitting one
// global total across threads, keeps every thread's progress independent: a
// single global total could let one thread's backlog consume the whole
// budget and stall every other thread's selection down to nothing, which a
// per-thread application can't do. Sharing one value across threads, rather
// than growing each thread's own counter independently, keeps the growth
// state and the user-facing config surface (log_count_min/log_count_growth_max)
// simple - a single knob users reason about, not one per thread on a
// topology (RAC) that changes over time as threads open and close.
//
// Known follow-up (not implemented here): seeding count from a
// previously-committed checkpoint SCN across a connector restart, so a fresh
// selector doesn't have to re-discover an appropriate budget via stall/growth
// from scratch.
//
// prevUpperBoundSCN is the endSCN this selector last returned (0 means none
// yet, following the same "0 is never a real SCN" convention used elsewhere
// in this package, e.g. LogMiner.getCurrentSCN's zero check). It only ever
// ratchets forward (see selectForSession) and is used by
// extendThreadPastBoundary to stop a thread's backlog - open or closed -
// from being permanently skipped once currentSCN advances past a boundary
// that thread's own budget-capped selection never reached.
type logFileSelector struct {
	minCount          int
	growthMax         int
	count             int
	prevKeys          []logKey
	prevUpperBoundSCN uint64
}

// selectForSession picks the redo/archive log files to mine for the next
// LogMiner session. files is the full, SCN-overlap-filtered candidate set
// across all redo threads (see GetLogsBySCNRange); openThreads lists the
// thread numbers Oracle currently reports as OPEN (see
// LogFileCollector.GetOpenThreads). maxRedoLogSizeInBytes is the online redo
// log size (see LogFileCollector.GetMaxRedoLogSize) that the shared budget
// (s.count) is denominated in - the byte threshold applied per thread is
// s.count * maxRedoLogSizeInBytes. On a single-thread database this reduces
// to truncating files to that byte threshold and capping unless the
// truncated selection's last file is the genuinely open current log.
//
// On RAC, the budget is applied independently to each thread's own sorted
// file list. An open thread with no files in files is an error - it means
// the collector query missed logs for an active thread, not that the thread
// has nothing to mine. The overall session is capped unless every thread's
// selection is complete - an open thread only counts as complete once it
// lands on its genuinely open current log (more redo may still be coming
// beyond what this range collected), while a closed thread counts as
// complete simply by covering everything currently available for it (it
// will never produce more). When capped, endSCN is the smallest of the
// per-thread tightened boundaries, across every thread and not just open
// ones, so no thread's mined range - including a closed thread's - outruns
// what was actually selected for it.
func (s *logFileSelector) selectForSession(files []*LogFile, openThreads []int, dbCurrentSCN, maxRedoLogSizeInBytes uint64) (selected []*LogFile, endSCN uint64, capped bool, err error) {
	if s.count == 0 {
		s.count = s.minCount
	}

	selected, endSCN, capped, truncated, budgetKeys, err := s.budgetPerThread(files, openThreads, dbCurrentSCN, maxRedoLogSizeInBytes)
	if err != nil {
		return nil, 0, false, err
	}

	// Stall detection compares the pre-extension budget selection, not the
	// (possibly much larger) extended one - extension catching up a
	// lagging thread's backlog is real forward progress, not a stall.
	if truncated && logKeysEqual(budgetKeys, s.prevKeys) {
		// Same combined selection across every thread again without progress
		// - grow the shared budget so a future cycle can advance. Rather than
		// always growing by a flat +1 (which, if a thread's backlog spans
		// many bytes-equivalent of budget, could take many stalled cycles to
		// clear), derive how large a jump would actually clear the backlog
		// already committed to (below prevUpperBoundSCN) and jump straight
		// there in one step when that is larger than +1.
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

	// Ratchet prevUpperBoundSCN forward only, never letting it regress - it
	// becomes the floor the next call's extension must reach past (see
	// extendThreadPastBoundary), and a lower boundary would let a thread's
	// backlog fall behind again.
	if endSCN > s.prevUpperBoundSCN {
		s.prevUpperBoundSCN = endSCN
	}

	return selected, endSCN, capped, nil
}

// deriveGrowthCount computes the count-equivalent budget that would clear,
// in one step, the largest backlog any single thread already has sitting
// below the previously committed boundary (prevUpperBoundSCN) - so a stall
// caused by one thread's byte-heavy backlog doesn't have to be worked
// through one file-sized-count increment per cycle.
//
// A file is only counted toward that backlog if its FirstSCN is strictly
// below prevUpperBoundSCN: FirstSCN, not NextSCN, is compared because the
// question being asked is "does this file lie entirely within ground a
// previous cycle already committed to covering", the same question
// extendThreadPastBoundary asks when deciding whether a file is safe to pull
// in automatically - a file that starts before the boundary but ends after
// it is still ground that cycle committed to, whereas comparing NextSCN
// would exclude exactly the files closest to the boundary that matter most.
//
// prevUpperBoundSCN == 0 means no boundary has been committed yet, so there
// is nothing to derive a jump from - this must not be treated as "every
// file is below the boundary" (which FirstSCN < 0 could never be true of
// anyway, but the loop still exits early for clarity). Returning minCount
// here simply leaves growTo's own max() in selectForSession to fall back to
// a flat +1 step, the same as it always has when there's no boundary to
// measure a backlog against.
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

// budgetPerThread applies the current shared byte budget (s.count *
// maxRedoLogSizeInBytes) to every redo thread present in files, extends each
// thread's capped selection past prevUpperBoundSCN (see
// extendThreadPastBoundary), then combines the per-thread results into one
// selection. truncated reports whether the budget alone (before extension)
// cut any thread's file list, independent of whether the overall selection
// ends up capped - a truncated thread whose selection is nonetheless
// complete (see the completeness rule in selectForSession's doc comment)
// does not cap the session, but the selection is still a partial (truncated)
// one for stall-detection purposes. budgetKeys identifies the pre-extension
// selection for that same stall-detection comparison - extension catching a
// lagging thread up is real progress, not a stall, so it must not be judged
// against the grown-budget/stall logic in selectForSession.
func (s *logFileSelector) budgetPerThread(files []*LogFile, openThreads []int, dbCurrentSCN, maxRedoLogSizeInBytes uint64) (selected []*LogFile, endSCN uint64, capped, truncated bool, budgetKeys []logKey, err error) {
	byThread := groupFilesByThread(files)

	// An open thread with zero files here means the collector's SCN-range
	// query missed that thread's logs - silently continuing would mine an
	// incomplete view of the database, so this is a hard error rather than
	// a skip.
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

		// Accumulate bytes in sequence order, stopping as soon as the running
		// total reaches the threshold - inclusive of the file that crosses
		// it, so a single file whose own size already meets the threshold
		// still selects that one file rather than none. This mirrors why the
		// check happens after adding the current file, not before.
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

		// A closed thread produces no further redo, so covering everything
		// currently available for it is sufficient to call it caught up. An
		// open thread cannot use that same shortcut - more redo may still be
		// coming beyond what this SCN range collected, so it must land on
		// its genuinely open current log specifically. A thread that is NOT
		// caught up - open or closed - must tighten endSCN to its last
		// selected file's NextSCN, or its unselected tail's SCN range would
		// permanently drop out of a future GetLogsBySCNRange window once
		// currentSCN advances past it.
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
		// Nothing to cap - every thread's whole overlapping range fits
		// within budget (extension is then necessarily a no-op too).
		return files, dbCurrentSCN, false, false, budgetKeys, nil
	}
	if allCaughtUp {
		return combined, dbCurrentSCN, false, true, budgetKeys, nil
	}
	return combined, tightestEndSCN, true, true, budgetKeys, nil
}

// extendThreadPastBoundary extends a thread's budget-capped file list, once
// a previous cycle has already committed to mining up through
// prevUpperBoundSCN, so this thread's selection - whether its redo thread is
// currently open or closed - keeps pace with that boundary. Without this, a
// thread whose own backlog exceeds the shared budget could have its
// unselected tail's SCN range permanently drop out of a future
// GetLogsBySCNRange window once currentSCN advances past it.
//
// This can push a thread's selection past growthMax: re-covering
// already-committed ground takes priority over the budget, since leaving a
// gap there is unsafe, whereas exceeding the configured growth ceiling here
// only costs extra files for one cycle. growthMax bounds how far automatic
// stall-driven growth climbs, not the number of files a thread can end up
// with.
//
// prevUpperBoundSCN == 0 means no boundary has been committed yet (this is
// the selector's first call), so there is nothing to extend past.
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

	// A last file that isn't a fully-archived, immutable copy (i.e. it's
	// online - ACTIVE, INACTIVE, or the genuinely open CURRENT log) can't be
	// treated as a safe stopping point, so pull in whatever remains of this
	// thread's files too.
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
	return groups
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

// ceilDiv divides numerator by denominator, rounding up - used to convert a
// byte-denominated backlog back into a count-equivalent number of
// maxRedoLogSizeInBytes-sized units, so a partially-filled unit of backlog
// still counts as needing a whole extra unit of budget to clear.
func ceilDiv(numerator, denominator uint64) int {
	if denominator == 0 {
		return 0
	}
	return int((numerator + denominator - 1) / denominator)
}
