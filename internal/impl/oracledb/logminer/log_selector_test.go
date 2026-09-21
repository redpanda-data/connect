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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// openThread1 is the common single-thread openThreads argument used by every
// test that doesn't care about RAC.
var openThread1 = []int{1}

func mkLogFile(thread int, sequence int64, nextSCN uint64, status string) *LogFile {
	return &LogFile{
		FileName: fmt.Sprintf("log_t%d_%d.arc", thread, sequence),
		NextSCN:  nextSCN,
		Sequence: sequence,
		Status:   status,
		Thread:   thread,
	}
}

func TestLogFileSelectorSelectForSession(t *testing.T) {
	t.Run("fits within budget returns all files uncapped", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{mkLogFile(1, 1, 1000, "ARCHIVED")}

		selected, endSCN, capped, err := s.selectForSession(files, openThread1, 5000)

		require.NoError(t, err)
		assert.Equal(t, files, selected)
		assert.Equal(t, uint64(5000), endSCN)
		assert.False(t, capped)
	})

	t.Run("capped selection when the last budgeted file is archived", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 1, 1000, "ARCHIVED"),
			mkLogFile(1, 2, 2000, "ARCHIVED"),
			mkLogFile(1, 3, 3000, "ARCHIVED"),
		}

		selected, endSCN, capped, err := s.selectForSession(files, openThread1, 5000)

		require.NoError(t, err)
		require.Len(t, selected, 2)
		assert.Equal(t, files[:2], selected)
		assert.Equal(t, uint64(1999), endSCN, "endSCN should be the last selected file's NextSCN minus 1 - NextSCN belongs to the following, unselected file")
		assert.True(t, capped)
	})

	t.Run("last budgeted file being current keeps selection uncapped despite truncation", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 1, 1000, "ARCHIVED"),
			mkLogFile(1, 2, 2000, logStatusCurrent), // still-open current online log, within budget
			mkLogFile(1, 3, 3000, "ARCHIVED"),
		}

		selected, endSCN, capped, err := s.selectForSession(files, openThread1, 5000)

		require.NoError(t, err)
		require.Len(t, selected, 2, "file list should still be truncated to the budget")
		assert.Equal(t, files[:2], selected)
		assert.Equal(t, uint64(5000), endSCN, "endSCN should fall back to the live current SCN")
		assert.False(t, capped, "a current last file must not be reported as capped")
	})

	t.Run("repeated identical selection stalls and grows the budget up to growthMax then plateaus", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 1, 1000, "ARCHIVED"),
			mkLogFile(1, 2, 2000, "ARCHIVED"),
			mkLogFile(1, 3, 3000, "ARCHIVED"),
			mkLogFile(1, 4, 4000, "ARCHIVED"),
			mkLogFile(1, 5, 5000, "ARCHIVED"),
		}

		// Cycle 1: fresh selector, budget starts at minCount (2), no stall detected yet.
		selected, _, capped, err := s.selectForSession(files, openThread1, 9000)
		require.NoError(t, err)
		require.Len(t, selected, 2)
		assert.True(t, capped)
		assert.Equal(t, 2, s.count)

		// Cycle 2: identical file set selected again -> stall detected, budget grows to 3.
		selected, _, capped, err = s.selectForSession(files, openThread1, 9000)
		require.NoError(t, err)
		require.Len(t, selected, 3)
		assert.True(t, capped)
		assert.Equal(t, 3, s.count)

		// Cycle 3: still stalled -> budget grows to 4 (growthMax).
		selected, _, capped, err = s.selectForSession(files, openThread1, 9000)
		require.NoError(t, err)
		require.Len(t, selected, 4)
		assert.True(t, capped)
		assert.Equal(t, 4, s.count)

		// Cycle 4: still stalled, but growthMax is already reached -> budget plateaus at 4
		// rather than growing unboundedly to 5.
		selected, _, capped, err = s.selectForSession(files, openThread1, 9000)
		require.NoError(t, err)
		require.Len(t, selected, 4)
		assert.True(t, capped)
		assert.Equal(t, 4, s.count)

		// Cycle 5: one more stalled cycle for good measure, confirming the plateau holds.
		selected, _, capped, err = s.selectForSession(files, openThread1, 9000)
		require.NoError(t, err)
		require.Len(t, selected, 4)
		assert.True(t, capped)
		assert.Equal(t, 4, s.count)
	})

	t.Run("growth that ends up covering all available files returns everything uncapped", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 1, 1000, "ARCHIVED"),
			mkLogFile(1, 2, 2000, "ARCHIVED"),
			mkLogFile(1, 3, 3000, "ARCHIVED"),
		}

		// Cycle 1: budget starts at minCount (2), capped selection of the first two files.
		selected, _, capped, err := s.selectForSession(files, openThread1, 9000)
		require.NoError(t, err)
		require.Len(t, selected, 2)
		assert.True(t, capped)

		// Cycle 2: stall grows the budget to 3, which now covers every file - this
		// must behave like the "fits within budget" case, not a partial, truncated one.
		selected, endSCN, capped, err := s.selectForSession(files, openThread1, 9000)
		require.NoError(t, err)
		assert.Equal(t, files, selected)
		assert.Equal(t, uint64(9000), endSCN)
		assert.False(t, capped)
	})

	t.Run("different non-overlapping file sets do not trigger growth", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		firstFiles := []*LogFile{
			mkLogFile(1, 1, 1000, "ARCHIVED"),
			mkLogFile(1, 2, 2000, "ARCHIVED"),
			mkLogFile(1, 3, 3000, "ARCHIVED"),
		}
		secondFiles := []*LogFile{
			mkLogFile(1, 4, 4000, "ARCHIVED"),
			mkLogFile(1, 5, 5000, "ARCHIVED"),
			mkLogFile(1, 6, 6000, "ARCHIVED"),
		}

		selected, _, capped, err := s.selectForSession(firstFiles, openThread1, 9000)
		require.NoError(t, err)
		require.Len(t, selected, 2)
		assert.True(t, capped)
		assert.Equal(t, 2, s.count, "budget should remain at minCount after real forward progress")

		selected, _, capped, err = s.selectForSession(secondFiles, openThread1, 9000)
		require.NoError(t, err)
		require.Len(t, selected, 2)
		assert.Equal(t, secondFiles[:2], selected)
		assert.True(t, capped)
		assert.Equal(t, 2, s.count, "a different file set must not be mistaken for a stall")
	})

	t.Run("last budgeted file switched away but not archived must still be capped", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 8, 8000, "ARCHIVED"),
			mkLogFile(1, 9, 9000, "ACTIVE"),           // switched away from, not yet archived - NOT open
			mkLogFile(1, 10, 10000, logStatusCurrent), // the real, genuinely open current log
		}

		selected, endSCN, capped, err := s.selectForSession(files, openThread1, 20000)

		require.NoError(t, err)
		require.Len(t, selected, 2, "budget should truncate to [arch#8, online#9], excluding online#10")
		assert.Equal(t, files[:2], selected)
		assert.Equal(t, uint64(8999), endSCN, "endSCN must be online#9's NextSCN minus 1, not dbCurrentSCN")
		assert.True(t, capped, "must be capped - online#10 was not actually selected or mined")
	})

	t.Run("genuinely open current log as the last budgeted file stays uncapped", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 8, 8000, "ARCHIVED"),
			mkLogFile(1, 9, 9000, logStatusCurrent), // genuinely open current log
		}

		selected, endSCN, capped, err := s.selectForSession(files, openThread1, 20000)

		require.NoError(t, err)
		assert.Equal(t, files, selected)
		assert.Equal(t, uint64(20000), endSCN, "endSCN should fall back to the live current SCN")
		assert.False(t, capped)
	})

	t.Run("endSCN must not land on the next unselected file's first SCN", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 1, 1000, "ARCHIVED"),
			mkLogFile(1, 2, 2000, "ARCHIVED"),
			mkLogFile(1, 3, 3000, "ARCHIVED"),
		}

		_, endSCN, capped, err := s.selectForSession(files, openThread1, 9000)

		require.NoError(t, err)
		require.True(t, capped)
		assert.Less(t, endSCN, files[1].NextSCN, "endSCN must stop strictly before the unselected next file's first SCN")
		assert.Equal(t, files[1].NextSCN-1, endSCN)
	})

	t.Run("count reset to minCount after a successful cycle takes effect on the next selection", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 1, 1000, "ARCHIVED"),
			mkLogFile(1, 2, 2000, "ARCHIVED"),
			mkLogFile(1, 3, 3000, "ARCHIVED"),
			mkLogFile(1, 4, 4000, "ARCHIVED"),
		}

		// Grow the budget to 3 via a stalled repeat, mirroring what miningCycle would see
		// across two capped cycles.
		_, _, _, err := s.selectForSession(files, openThread1, 9000)
		require.NoError(t, err)
		selected, _, capped, err := s.selectForSession(files, openThread1, 9000)
		require.NoError(t, err)
		require.Len(t, selected, 3)
		assert.True(t, capped)
		assert.Equal(t, 3, s.count)

		// miningCycle resets count to LogCountMin whenever a cycle completes uncapped;
		// simulate that directly (no exported method exists).
		s.count = s.minCount

		// The boundary ratchet (prevUpperBoundSCN) is untouched by the reset above,
		// and cycle 2 already committed to mining up through file #3's boundary - so
		// resetting count alone must not silently re-drop that coverage: the
		// selection still extends to 3 files, not back down to 2.
		selected, _, capped, err = s.selectForSession(files, openThread1, 9000)
		require.NoError(t, err)
		require.Len(t, selected, 3, "the boundary ratchet must keep the previously committed coverage even after count is reset")
		assert.Equal(t, files[:3], selected)
		assert.True(t, capped)

		// With no boundary committed yet and no stalled selection remembered
		// (simulating a fresh selector), the reset budget alone does take effect
		// exactly as before this feature existed.
		s.prevUpperBoundSCN = 0
		s.prevKeys = nil

		selected, _, capped, err = s.selectForSession(files, openThread1, 9000)
		require.NoError(t, err)
		require.Len(t, selected, 2, "selection should honour the reset budget, not the stale grown one, once there's no boundary to ratchet against")
		assert.Equal(t, files[:2], selected)
		assert.True(t, capped)
	})

	// --- RAC (multi-thread) scenarios ---

	t.Run("two open threads both caught up on their current log returns everything uncapped", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 1, 1000, "ARCHIVED"),
			mkLogFile(1, 2, 2000, logStatusCurrent),
			mkLogFile(2, 1, 1500, "ARCHIVED"),
			mkLogFile(2, 2, 2500, logStatusCurrent),
		}

		selected, endSCN, capped, err := s.selectForSession(files, []int{1, 2}, 9000)

		require.NoError(t, err)
		assert.ElementsMatch(t, files, selected)
		assert.Equal(t, uint64(9000), endSCN)
		assert.False(t, capped)
	})

	t.Run("one of two open threads ends capped on an archived file tightens endSCN to that thread", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			// thread 1: caught up, ends on its genuinely open current log.
			mkLogFile(1, 1, 1000, "ARCHIVED"),
			mkLogFile(1, 2, 2000, logStatusCurrent),
			// thread 2: falls behind budget, ends capped on an archived file.
			mkLogFile(2, 1, 1500, "ARCHIVED"),
			mkLogFile(2, 2, 2500, "ARCHIVED"),
			mkLogFile(2, 3, 3500, logStatusCurrent),
		}

		selected, endSCN, capped, err := s.selectForSession(files, []int{1, 2}, 9000)

		require.NoError(t, err)
		require.Len(t, selected, 4, "thread 1's 2 files plus thread 2's budgeted 2 files")
		assert.True(t, capped)
		assert.Equal(t, uint64(2499), endSCN, "endSCN must be tightened to thread 2's last selected file's NextSCN minus 1")
	})

	t.Run("two open threads both capped on archived files tightens endSCN to the smaller boundary", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			// thread 1: capped, last selected NextSCN 2000 -> boundary 1999.
			mkLogFile(1, 1, 1000, "ARCHIVED"),
			mkLogFile(1, 2, 2000, "ARCHIVED"),
			mkLogFile(1, 3, 3000, logStatusCurrent),
			// thread 2: capped, last selected NextSCN 2500 -> boundary 2499 (smaller).
			mkLogFile(2, 1, 1500, "ARCHIVED"),
			mkLogFile(2, 2, 2500, "ARCHIVED"),
			mkLogFile(2, 3, 3500, logStatusCurrent),
		}

		selected, endSCN, capped, err := s.selectForSession(files, []int{1, 2}, 9000)

		require.NoError(t, err)
		require.Len(t, selected, 4)
		assert.True(t, capped)
		assert.Equal(t, uint64(1999), endSCN, "endSCN must be the smaller of the two threads' tightened boundaries")
	})

	t.Run("open thread with zero matching files returns an error", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 1, 1000, "ARCHIVED"),
			mkLogFile(1, 2, 2000, logStatusCurrent),
		}

		selected, _, _, err := s.selectForSession(files, []int{1, 2}, 9000)

		require.Error(t, err)
		assert.Nil(t, selected)
		assert.ErrorContains(t, err, "thread 2")
	})

	t.Run("a closed thread with sparse files does not error and is still included", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 1, 1000, "ARCHIVED"),
			mkLogFile(1, 2, 2000, logStatusCurrent),
			// thread 2 is closed (not in openThreads) but still has a leftover archived log
			// overlapping this SCN range from before it was shut down.
			mkLogFile(2, 1, 1500, "ARCHIVED"),
		}

		selected, endSCN, capped, err := s.selectForSession(files, openThread1, 9000)

		require.NoError(t, err)
		assert.ElementsMatch(t, files, selected)
		assert.Equal(t, uint64(9000), endSCN)
		assert.False(t, capped)
	})

	t.Run("a closed thread with zero files does not error", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 1, 1000, "ARCHIVED"),
			mkLogFile(1, 2, 2000, logStatusCurrent),
		}

		selected, endSCN, capped, err := s.selectForSession(files, openThread1, 9000)

		require.NoError(t, err)
		assert.Equal(t, files, selected)
		assert.Equal(t, uint64(9000), endSCN)
		assert.False(t, capped)
	})

	t.Run("stall detection compares the combined multi-thread selection, not a single thread", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 1, 1000, "ARCHIVED"),
			mkLogFile(1, 2, 2000, "ARCHIVED"),
			mkLogFile(1, 3, 3000, "ARCHIVED"),
			mkLogFile(2, 1, 1500, "ARCHIVED"),
			mkLogFile(2, 2, 2500, "ARCHIVED"),
			mkLogFile(2, 3, 3500, "ARCHIVED"),
		}

		// Cycle 1: budget starts at minCount (2) for each thread, both capped.
		selected, _, capped, err := s.selectForSession(files, []int{1, 2}, 9000)
		require.NoError(t, err)
		require.Len(t, selected, 4)
		assert.True(t, capped)
		assert.Equal(t, 2, s.count)

		// Cycle 2: identical combined selection across both threads -> stall, budget grows to 3.
		selected, _, capped, err = s.selectForSession(files, []int{1, 2}, 9000)
		require.NoError(t, err)
		require.Len(t, selected, 6, "growth to 3 now covers every file in both threads")
		assert.False(t, capped)
		assert.Equal(t, 3, s.count)
	})

	t.Run("growth does not falsely trigger when only one thread's selection coincides", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		firstFiles := []*LogFile{
			mkLogFile(1, 1, 1000, "ARCHIVED"),
			mkLogFile(1, 2, 2000, "ARCHIVED"),
			mkLogFile(1, 3, 3000, "ARCHIVED"),
			mkLogFile(2, 1, 1500, "ARCHIVED"),
			mkLogFile(2, 2, 2500, "ARCHIVED"),
			mkLogFile(2, 3, 3500, "ARCHIVED"),
		}
		// Thread 1's files are identical to the first cycle; thread 2's are entirely
		// different (real forward progress on that thread). The combined selection
		// therefore differs cycle-to-cycle and must not be mistaken for a stall.
		secondFiles := []*LogFile{
			mkLogFile(1, 1, 1000, "ARCHIVED"),
			mkLogFile(1, 2, 2000, "ARCHIVED"),
			mkLogFile(1, 3, 3000, "ARCHIVED"),
			mkLogFile(2, 4, 4500, "ARCHIVED"),
			mkLogFile(2, 5, 5500, "ARCHIVED"),
			mkLogFile(2, 6, 6500, "ARCHIVED"),
		}

		selected, _, capped, err := s.selectForSession(firstFiles, []int{1, 2}, 9000)
		require.NoError(t, err)
		require.Len(t, selected, 4)
		assert.True(t, capped)
		assert.Equal(t, 2, s.count)

		selected, _, capped, err = s.selectForSession(secondFiles, []int{1, 2}, 9000)
		require.NoError(t, err)
		require.Len(t, selected, 4)
		assert.True(t, capped)
		assert.Equal(t, 2, s.count, "a different combined selection must not be mistaken for a stall")
	})

	// --- boundary ratchet (anti-regression) scenarios ---

	t.Run("closed thread's backlog beyond budget tightens endSCN and grows toward it cycle by cycle", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}

		// Thread 1 (open) is fully caught up on its own genuinely open current log.
		openThreadFiles := []*LogFile{
			mkLogFile(1, 1, 100, "ARCHIVED"),
			mkLogFile(1, 2, 200, logStatusCurrent),
		}
		// Thread 2 (closed) has a substantial backlog of small archived files, far
		// exceeding the shared budget of 2 (and, deliberately, exceeding growthMax
		// too - see the plateau at cycle 3/4 below).
		closedThreadBacklog := []*LogFile{
			mkLogFile(2, 1, 1100, "ARCHIVED"),
			mkLogFile(2, 2, 1200, "ARCHIVED"),
			mkLogFile(2, 3, 1300, "ARCHIVED"),
			mkLogFile(2, 4, 1400, "ARCHIVED"),
			mkLogFile(2, 5, 1500, "ARCHIVED"),
		}
		files := append(append([]*LogFile{}, openThreadFiles...), closedThreadBacklog...)

		// Cycle 1: thread 1 (the only open thread) is caught up, but thread 2's
		// backlog exceeds its budget of 2 - this must tighten endSCN to thread 2's
		// last selected file, not fall back to dbCurrentSCN. This is the exact bug:
		// previously, a truncated closed thread never influenced endSCN, so
		// currentSCN could jump straight to dbCurrentSCN leaving files #3-#5
		// permanently unreachable once the next cycle's SCN range moved past them.
		selected, endSCN, capped, err := s.selectForSession(files, openThread1, 9000)
		require.NoError(t, err)
		assert.True(t, capped, "thread 2's untightened backlog must cap the session")
		assert.Equal(t, uint64(1199), endSCN, "endSCN tightened to thread 2's 2nd (budgeted) file's NextSCN minus 1")
		require.Len(t, selected, 4, "thread 1's 2 files plus thread 2's budgeted 2 files")
		assert.Equal(t, uint64(1199), s.prevUpperBoundSCN)

		// Cycle 2: same inputs (thread 2 produced nothing new since it's closed).
		// The identical selection stalls, growing the shared budget to 3 - thread 2
		// now gets 3 files, tightening endSCN further out.
		selected, endSCN, capped, err = s.selectForSession(files, openThread1, 9000)
		require.NoError(t, err)
		assert.True(t, capped)
		assert.Equal(t, uint64(1299), endSCN)
		assert.Len(t, selected, 5, "thread 1's 2 files plus thread 2's grown budget of 3")
		assert.Equal(t, 3, s.count)

		// Cycle 3: stalled again, budget grows to 4 - the growthMax ceiling.
		selected, endSCN, capped, err = s.selectForSession(files, openThread1, 9000)
		require.NoError(t, err)
		assert.True(t, capped)
		assert.Equal(t, uint64(1399), endSCN)
		assert.Len(t, selected, 6, "thread 1's 2 files plus thread 2's grown budget of 4")
		assert.Equal(t, 4, s.count)

		// Cycle 4: budget has plateaued at growthMax (4), one short of thread 2's
		// full 5-file backlog. This is a safe stall, not silent data loss: endSCN
		// never advances past what was actually selected, so file #5 stays pending
		// (retried every cycle) rather than being skipped - it only advances once
		// something else (e.g. thread 1 falling behind too, or a config change)
		// pushes the boundary past it. See the next test for the case where the
		// boundary is already established before the backlog is even considered,
		// which does sweep it up in one shot via extension rather than growth.
		selected, endSCN, capped, err = s.selectForSession(files, openThread1, 9000)
		require.NoError(t, err)
		assert.True(t, capped)
		assert.Equal(t, uint64(1399), endSCN, "plateaued - growth cannot exceed growthMax, and nothing else pushes the boundary further")
		assert.Len(t, selected, 6)
		assert.Equal(t, 4, s.count)
		assert.NotContains(t, selected, closedThreadBacklog[4], "file #5 is never silently included without either budget covering it or a boundary already past it")
	})

	t.Run("closed thread's backlog is swept up in one shot once a boundary already exceeds it", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}

		// Cycle 1: only thread 1 (open) is present, already on its current log -
		// this legitimately commits the ratchet all the way to dbCurrentSCN, before
		// thread 2's backlog is even in the picture (e.g. thread 2's shutdown and
		// this connector noticing its stale backlog happen independently).
		openThreadFiles := []*LogFile{
			mkLogFile(1, 1, 100, "ARCHIVED"),
			mkLogFile(1, 2, 200, logStatusCurrent),
		}
		_, endSCN, capped, err := s.selectForSession(openThreadFiles, openThread1, 9000)
		require.NoError(t, err)
		require.False(t, capped)
		require.Equal(t, uint64(9000), endSCN)
		require.Equal(t, uint64(9000), s.prevUpperBoundSCN)

		// Cycle 2: thread 2's 5-file closed backlog now appears in the collected
		// range for the first time. Even though its budget is still only 2, every
		// one of its files has a NextSCN well below the already-committed boundary
		// of 9000, so extension sweeps in the entire backlog in a single cycle -
		// far faster than growing the budget one file per stalled cycle, and not
		// limited by growthMax (see extendThreadPastBoundary's doc comment).
		closedThreadBacklog := []*LogFile{
			mkLogFile(2, 1, 1100, "ARCHIVED"),
			mkLogFile(2, 2, 1200, "ARCHIVED"),
			mkLogFile(2, 3, 1300, "ARCHIVED"),
			mkLogFile(2, 4, 1400, "ARCHIVED"),
			mkLogFile(2, 5, 1500, "ARCHIVED"),
		}
		files := append(append([]*LogFile{}, openThreadFiles...), closedThreadBacklog...)

		selected, endSCN, capped, err := s.selectForSession(files, openThread1, 9000)
		require.NoError(t, err)
		assert.False(t, capped, "the fully-extended closed thread has nothing left to tighten endSCN against")
		assert.Equal(t, uint64(9000), endSCN)
		assert.Len(t, selected, 7, "thread 1's 2 files plus all 5 of thread 2's backlog, swept up in one shot")
		for _, f := range closedThreadBacklog {
			assert.Contains(t, selected, f, "no file in the closed thread's backlog may be left behind once a boundary was already committed past it")
		}
	})

	t.Run("boundary ratchet never regresses even when a later cycle legitimately computes a lower endSCN", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}

		// Cycle 1: a single open thread, already on its genuinely open current log -
		// uncapped, committing the ratchet forward to dbCurrentSCN (9000).
		firstCycleFiles := []*LogFile{mkLogFile(1, 1, 500, logStatusCurrent)}
		_, endSCN, capped, err := s.selectForSession(firstCycleFiles, openThread1, 9000)
		require.NoError(t, err)
		require.False(t, capped)
		require.Equal(t, uint64(9000), endSCN)
		require.Equal(t, uint64(9000), s.prevUpperBoundSCN)

		// Cycle 2: the same thread now has a genuine backlog of its own (its true
		// current log isn't even in this collected range), so this cycle legitimately
		// caps well below the previously committed boundary of 9000.
		secondCycleFiles := []*LogFile{
			mkLogFile(1, 2, 300, "ARCHIVED"),
			mkLogFile(1, 3, 500, "ARCHIVED"),
			mkLogFile(1, 4, 700, "ARCHIVED"),
			mkLogFile(1, 5, 900, "ARCHIVED"),
		}
		_, endSCN, capped, err = s.selectForSession(secondCycleFiles, openThread1, 9500)
		require.NoError(t, err)
		require.True(t, capped)
		require.Less(t, endSCN, uint64(9000), "this cycle's own boundary is legitimately lower than what was previously committed")

		// The recorded ratchet must not have regressed to match it.
		assert.Equal(t, uint64(9000), s.prevUpperBoundSCN, "the ratchet must only ever go up, regardless of what a later cycle itself returns")
	})

	t.Run("no previous boundary yet means extension is a no-op on the selector's first call", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		require.Zero(t, s.prevUpperBoundSCN)

		openThreadFiles := []*LogFile{
			mkLogFile(1, 1, 100, "ARCHIVED"),
			mkLogFile(1, 2, 200, logStatusCurrent),
		}
		closedThreadBacklog := []*LogFile{
			mkLogFile(2, 1, 1100, "ARCHIVED"),
			mkLogFile(2, 2, 1200, "ARCHIVED"),
			mkLogFile(2, 3, 1300, "ARCHIVED"),
		}
		files := append(append([]*LogFile{}, openThreadFiles...), closedThreadBacklog...)

		selected, endSCN, capped, err := s.selectForSession(files, openThread1, 9000)

		require.NoError(t, err)
		require.Len(t, selected, 4, "with no boundary to ratchet against, thread 2 is still limited to its budgeted 2 files - behaviour unchanged from before this feature existed")
		assert.NotContains(t, selected, closedThreadBacklog[2], "the 3rd backlog file must not be pulled in without a previously committed boundary")
		assert.True(t, capped, "thread 2's unselected backlog (files #3-#5) must cap the session - it must not be silently dropped just because thread 1 is caught up")
		assert.Equal(t, uint64(1199), endSCN, "endSCN must be tightened to thread 2's last selected file's NextSCN minus 1, not fall back to dbCurrentSCN")
	})

	t.Run("extension stops once it reaches the thread's genuinely open current log", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}

		// Cycle 1: establish a large previous boundary via an unrelated closed thread
		// reaching all the way to dbCurrentSCN.
		seedFiles := []*LogFile{
			mkLogFile(1, 1, 100, "ARCHIVED"),
			mkLogFile(1, 2, 200, logStatusCurrent),
		}
		_, endSCN, capped, err := s.selectForSession(seedFiles, openThread1, 9000)
		require.NoError(t, err)
		require.False(t, capped)
		require.Equal(t, uint64(9000), s.prevUpperBoundSCN)
		_ = endSCN

		// Cycle 2: the open thread now has a short backlog of its own that reaches
		// its genuinely open current log well before the previously committed
		// boundary of 9000 - extension must land on it and stop there, rather than
		// erroring or trying to walk past a thread's own last file.
		files := []*LogFile{
			mkLogFile(1, 3, 300, "ARCHIVED"),
			mkLogFile(1, 4, 500, "ARCHIVED"),
			mkLogFile(1, 5, 600, logStatusCurrent),
		}

		selected, endSCN, capped, err := s.selectForSession(files, openThread1, 9500)

		require.NoError(t, err)
		assert.False(t, capped, "landing on the genuinely open current log makes the thread caught up")
		assert.Equal(t, uint64(9500), endSCN)
		assert.Equal(t, files, selected, "extension must include exactly this thread's files, stopping at its current log without error")
	})
}
