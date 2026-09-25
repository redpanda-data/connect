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

type logFileSelector struct {
	minCount          int
	growthMax         int
	count             int
	prevKeys          []logKey
	prevUpperBoundSCN uint64
}

// selectForSession picks the log files to mine next, erroring if an open
// thread has no files, and capping the session (endSCN = the smallest
// per-thread tightened boundary) unless every thread is complete - open by
// reaching its current log, closed by covering everything available.
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
		// No progress - grow the budget by deriving the jump that would clear the backlog in one step, rather than a flat +1.
		derived := s.deriveGrowthCount(files, maxRedoLogSizeInBytes)
		growTo := max(derived, s.count+1)
		// MinRedoVolumeGrowthCeiling guards against a permanent stall where a
		// budget of 1 always reselects its own single file forever
		if ceiling := max(s.growthMax, s.minCount, MinRedoVolumeGrowthCeiling); growTo > ceiling {
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

// deriveGrowthCount computes, in one step, the count that would clear the
// largest backlog any thread has below prevUpperBoundSCN (via FirstSCN, not
// NextSCN, so boundary-adjacent files still count), or minCount if there's
// no boundary yet to derive from.
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
// prevUpperBoundSCN, and combines the results; truncated (via the
// pre-extension budgetKeys) reports whether the budget alone cut any
// thread's files, since extension is progress, not a stall.
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

		// A closed thread completes once fully covered, an open thread once
		// it reaches its current log (more may still be coming); an
		// incomplete thread tightens endSCN so its tail can't drop out later.
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

// extendThreadPastBoundary extends past prevUpperBoundSCN (a no-op at 0)
// so a thread's tail can't drop out of a future window, even past
// growthMax, since re-covering committed ground is unsafe to skip.
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
