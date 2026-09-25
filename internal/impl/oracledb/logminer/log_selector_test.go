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

// openThread1 is the common single-thread openThreads argument for tests that don't care about RAC.
var openThread1 = []int{1}

// testRedoLogSize stands in for the real max redo log size; most files below are sized to exactly one, so a budget of N behaves like "N files" unless a test varies sizes deliberately.
const testRedoLogSize = 1_000_000

// mkLogFile builds a *LogFile for selector tests; bytes is its on-disk size - see testRedoLogSize.
func mkLogFile(thread int, sequence int64, firstSCN, nextSCN uint64, status string, bytes uint64) *LogFile {
	return &LogFile{
		FileName:  fmt.Sprintf("log_t%d_%d.arc", thread, sequence),
		FirstSCN:  firstSCN,
		NextSCN:   nextSCN,
		Sequence:  sequence,
		Status:    status,
		Thread:    thread,
		SizeBytes: bytes,
	}
}

func TestLogFileSelectorSelectForSession(t *testing.T) {
	t.Run("fits within budget returns all files uncapped", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{mkLogFile(1, 1, 0, 1000, "ARCHIVED", testRedoLogSize)}

		selected, endSCN, capped, err := s.selectForSession(files, openThread1, 5000, testRedoLogSize)

		require.NoError(t, err)
		assert.Equal(t, files, selected)
		assert.Equal(t, uint64(5000), endSCN)
		assert.False(t, capped)
	})

	t.Run("capped selection when the last budgeted file is archived", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 1, 0, 1000, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 2, 1000, 2000, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 3, 2000, 3000, "ARCHIVED", testRedoLogSize),
		}

		selected, endSCN, capped, err := s.selectForSession(files, openThread1, 5000, testRedoLogSize)

		require.NoError(t, err)
		require.Len(t, selected, 2, "2 files x 1 redo-log-size each reaches the 2-unit budget threshold")
		assert.Equal(t, files[:2], selected)
		assert.Equal(t, uint64(1999), endSCN, "endSCN should be the last selected file's NextSCN minus 1 - NextSCN belongs to the following, unselected file")
		assert.True(t, capped)
	})

	t.Run("a single file whose own size already meets the byte threshold caps immediately", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 1, 0, 1000, "ARCHIVED", 3*testRedoLogSize), // one oversized file alone exceeds the 2-unit threshold
			mkLogFile(1, 2, 1000, 2000, "ARCHIVED", testRedoLogSize),
		}

		selected, endSCN, capped, err := s.selectForSession(files, openThread1, 5000, testRedoLogSize)

		require.NoError(t, err)
		require.Len(t, selected, 1, "the byte threshold is met by the first file alone, so accumulation stops immediately after it")
		assert.Equal(t, files[:1], selected)
		assert.Equal(t, uint64(999), endSCN)
		assert.True(t, capped)
	})

	t.Run("last budgeted file being current keeps selection uncapped despite truncation", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 1, 0, 1000, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 2, 1000, 2000, logStatusCurrent, testRedoLogSize), // still-open current online log, within budget
			mkLogFile(1, 3, 2000, 3000, "ARCHIVED", testRedoLogSize),
		}

		selected, endSCN, capped, err := s.selectForSession(files, openThread1, 5000, testRedoLogSize)

		require.NoError(t, err)
		require.Len(t, selected, 2, "file list should still be truncated to the byte budget")
		assert.Equal(t, files[:2], selected)
		assert.Equal(t, uint64(5000), endSCN, "endSCN should fall back to the live current SCN")
		assert.False(t, capped, "a current last file must not be reported as capped")
	})

	t.Run("repeated identical selection stalls and grows the budget up to growthMax then plateaus", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 1, 0, 1000, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 2, 1000, 2000, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 3, 2000, 3000, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 4, 3000, 4000, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 5, 4000, 5000, "ARCHIVED", testRedoLogSize),
		}

		// Cycle 1: fresh selector, budget starts at minCount (2), no stall detected yet.
		selected, _, capped, err := s.selectForSession(files, openThread1, 9000, testRedoLogSize)
		require.NoError(t, err)
		require.Len(t, selected, 2)
		assert.True(t, capped)
		assert.Equal(t, 2, s.count)

		// Cycle 2: identical file set selected again -> stall detected, budget grows to 3 (every file here is 1 redo-log-size, so the derived jump agrees with a plain +1).
		selected, _, capped, err = s.selectForSession(files, openThread1, 9000, testRedoLogSize)
		require.NoError(t, err)
		require.Len(t, selected, 3)
		assert.True(t, capped)
		assert.Equal(t, 3, s.count)

		// Cycle 3: still stalled -> budget grows to 4 (growthMax).
		selected, _, capped, err = s.selectForSession(files, openThread1, 9000, testRedoLogSize)
		require.NoError(t, err)
		require.Len(t, selected, 4)
		assert.True(t, capped)
		assert.Equal(t, 4, s.count)

		// Cycle 4: still stalled, but growthMax is already reached -> budget plateaus at 4 rather than growing to 5.
		selected, _, capped, err = s.selectForSession(files, openThread1, 9000, testRedoLogSize)
		require.NoError(t, err)
		require.Len(t, selected, 4)
		assert.True(t, capped)
		assert.Equal(t, 4, s.count)

		// Cycle 5: one more stalled cycle for good measure, confirming the plateau holds.
		selected, _, capped, err = s.selectForSession(files, openThread1, 9000, testRedoLogSize)
		require.NoError(t, err)
		require.Len(t, selected, 4)
		assert.True(t, capped)
		assert.Equal(t, 4, s.count)
	})

	t.Run("growth that ends up covering all available files returns everything uncapped", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 1, 0, 1000, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 2, 1000, 2000, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 3, 2000, 3000, "ARCHIVED", testRedoLogSize),
		}

		// Cycle 1: budget starts at minCount (2), capped selection of the first two files.
		selected, _, capped, err := s.selectForSession(files, openThread1, 9000, testRedoLogSize)
		require.NoError(t, err)
		require.Len(t, selected, 2)
		assert.True(t, capped)

		// Cycle 2: stall grows the budget to 3, which now covers every file - behaves like "fits within budget", not a partial truncation.
		selected, endSCN, capped, err := s.selectForSession(files, openThread1, 9000, testRedoLogSize)
		require.NoError(t, err)
		assert.Equal(t, files, selected)
		assert.Equal(t, uint64(9000), endSCN)
		assert.False(t, capped)
	})

	t.Run("different non-overlapping file sets do not trigger growth", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		firstFiles := []*LogFile{
			mkLogFile(1, 1, 0, 1000, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 2, 1000, 2000, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 3, 2000, 3000, "ARCHIVED", testRedoLogSize),
		}
		secondFiles := []*LogFile{
			mkLogFile(1, 4, 3000, 4000, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 5, 4000, 5000, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 6, 5000, 6000, "ARCHIVED", testRedoLogSize),
		}

		selected, _, capped, err := s.selectForSession(firstFiles, openThread1, 9000, testRedoLogSize)
		require.NoError(t, err)
		require.Len(t, selected, 2)
		assert.True(t, capped)
		assert.Equal(t, 2, s.count, "budget should remain at minCount after real forward progress")

		selected, _, capped, err = s.selectForSession(secondFiles, openThread1, 9000, testRedoLogSize)
		require.NoError(t, err)
		require.Len(t, selected, 2)
		assert.Equal(t, secondFiles[:2], selected)
		assert.True(t, capped)
		assert.Equal(t, 2, s.count, "a different file set must not be mistaken for a stall")
	})

	t.Run("last budgeted file switched away but not archived must still be capped", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 8, 7000, 8000, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 9, 8000, 9000, "ACTIVE", testRedoLogSize),           // switched away from, not yet archived - NOT open
			mkLogFile(1, 10, 9000, 10000, logStatusCurrent, testRedoLogSize), // the real, genuinely open current log
		}

		selected, endSCN, capped, err := s.selectForSession(files, openThread1, 20000, testRedoLogSize)

		require.NoError(t, err)
		require.Len(t, selected, 2, "budget should truncate to [arch#8, online#9], excluding online#10")
		assert.Equal(t, files[:2], selected)
		assert.Equal(t, uint64(8999), endSCN, "endSCN must be online#9's NextSCN minus 1, not dbCurrentSCN")
		assert.True(t, capped, "must be capped - online#10 was not actually selected or mined")
	})

	t.Run("genuinely open current log as the last budgeted file stays uncapped", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 8, 7000, 8000, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 9, 8000, 9000, logStatusCurrent, testRedoLogSize), // genuinely open current log
		}

		selected, endSCN, capped, err := s.selectForSession(files, openThread1, 20000, testRedoLogSize)

		require.NoError(t, err)
		assert.Equal(t, files, selected)
		assert.Equal(t, uint64(20000), endSCN, "endSCN should fall back to the live current SCN")
		assert.False(t, capped)
	})

	t.Run("endSCN must not land on the next unselected file's first SCN", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 1, 0, 1000, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 2, 1000, 2000, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 3, 2000, 3000, "ARCHIVED", testRedoLogSize),
		}

		_, endSCN, capped, err := s.selectForSession(files, openThread1, 9000, testRedoLogSize)

		require.NoError(t, err)
		require.True(t, capped)
		assert.Less(t, endSCN, files[1].NextSCN, "endSCN must stop strictly before the unselected next file's first SCN")
		assert.Equal(t, files[1].NextSCN-1, endSCN)
	})

	t.Run("count reset to minCount after a successful cycle takes effect on the next selection", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 1, 0, 1000, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 2, 1000, 2000, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 3, 2000, 3000, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 4, 3000, 4000, "ARCHIVED", testRedoLogSize),
		}

		// Grow the budget to 3 via a stalled repeat, mirroring two capped miningCycle calls.
		_, _, _, err := s.selectForSession(files, openThread1, 9000, testRedoLogSize)
		require.NoError(t, err)
		selected, _, capped, err := s.selectForSession(files, openThread1, 9000, testRedoLogSize)
		require.NoError(t, err)
		require.Len(t, selected, 3)
		assert.True(t, capped)
		assert.Equal(t, 3, s.count)

		// miningCycle resets count to LogCountMin on an uncapped cycle; simulate that directly (no exported method exists).
		s.count = s.minCount

		// prevUpperBoundSCN is untouched by the reset above, so the ratchet must keep 3 files' coverage rather than silently dropping back to 2.
		selected, _, capped, err = s.selectForSession(files, openThread1, 9000, testRedoLogSize)
		require.NoError(t, err)
		require.Len(t, selected, 3, "the boundary ratchet must keep the previously committed coverage even after count is reset")
		assert.Equal(t, files[:3], selected)
		assert.True(t, capped)

		// With no boundary or stalled selection remembered (a fresh selector), the reset budget alone takes effect as before this feature existed.
		s.prevUpperBoundSCN = 0
		s.prevKeys = nil

		selected, _, capped, err = s.selectForSession(files, openThread1, 9000, testRedoLogSize)
		require.NoError(t, err)
		require.Len(t, selected, 2, "selection should honour the reset budget, not the stale grown one, once there's no boundary to ratchet against")
		assert.Equal(t, files[:2], selected)
		assert.True(t, capped)
	})

	// --- RAC (multi-thread) scenarios ---

	t.Run("two open threads both caught up on their current log returns everything uncapped", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 1, 0, 1000, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 2, 1000, 2000, logStatusCurrent, testRedoLogSize),
			mkLogFile(2, 1, 500, 1500, "ARCHIVED", testRedoLogSize),
			mkLogFile(2, 2, 1500, 2500, logStatusCurrent, testRedoLogSize),
		}

		selected, endSCN, capped, err := s.selectForSession(files, []int{1, 2}, 9000, testRedoLogSize)

		require.NoError(t, err)
		assert.ElementsMatch(t, files, selected)
		assert.Equal(t, uint64(9000), endSCN)
		assert.False(t, capped)
	})

	t.Run("one of two open threads ends capped on an archived file tightens endSCN to that thread", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			// thread 1: caught up, ends on its genuinely open current log.
			mkLogFile(1, 1, 0, 1000, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 2, 1000, 2000, logStatusCurrent, testRedoLogSize),
			// thread 2: falls behind budget, ends capped on an archived file.
			mkLogFile(2, 1, 500, 1500, "ARCHIVED", testRedoLogSize),
			mkLogFile(2, 2, 1500, 2500, "ARCHIVED", testRedoLogSize),
			mkLogFile(2, 3, 2500, 3500, logStatusCurrent, testRedoLogSize),
		}

		selected, endSCN, capped, err := s.selectForSession(files, []int{1, 2}, 9000, testRedoLogSize)

		require.NoError(t, err)
		require.Len(t, selected, 4, "thread 1's 2 files plus thread 2's budgeted 2 files")
		assert.True(t, capped)
		assert.Equal(t, uint64(2499), endSCN, "endSCN must be tightened to thread 2's last selected file's NextSCN minus 1")
	})

	t.Run("two open threads both capped on archived files tightens endSCN to the smaller boundary", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			// thread 1: capped, last selected NextSCN 2000 -> boundary 1999.
			mkLogFile(1, 1, 0, 1000, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 2, 1000, 2000, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 3, 2000, 3000, logStatusCurrent, testRedoLogSize),
			// thread 2: capped, last selected NextSCN 2500 -> boundary 2499 (smaller).
			mkLogFile(2, 1, 500, 1500, "ARCHIVED", testRedoLogSize),
			mkLogFile(2, 2, 1500, 2500, "ARCHIVED", testRedoLogSize),
			mkLogFile(2, 3, 2500, 3500, logStatusCurrent, testRedoLogSize),
		}

		selected, endSCN, capped, err := s.selectForSession(files, []int{1, 2}, 9000, testRedoLogSize)

		require.NoError(t, err)
		require.Len(t, selected, 4)
		assert.True(t, capped)
		assert.Equal(t, uint64(1999), endSCN, "endSCN must be the smaller of the two threads' tightened boundaries")
	})

	t.Run("open thread with zero matching files returns an error", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 1, 0, 1000, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 2, 1000, 2000, logStatusCurrent, testRedoLogSize),
		}

		selected, _, _, err := s.selectForSession(files, []int{1, 2}, 9000, testRedoLogSize)

		require.Error(t, err)
		assert.Nil(t, selected)
		assert.ErrorContains(t, err, "thread 2")
	})

	t.Run("a closed thread with sparse files does not error and is still included", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 1, 0, 1000, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 2, 1000, 2000, logStatusCurrent, testRedoLogSize),
			// thread 2 is closed but still has a leftover archived log from before it was shut down.
			mkLogFile(2, 1, 500, 1500, "ARCHIVED", testRedoLogSize),
		}

		selected, endSCN, capped, err := s.selectForSession(files, openThread1, 9000, testRedoLogSize)

		require.NoError(t, err)
		assert.ElementsMatch(t, files, selected)
		assert.Equal(t, uint64(9000), endSCN)
		assert.False(t, capped)
	})

	t.Run("a closed thread with zero files does not error", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 1, 0, 1000, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 2, 1000, 2000, logStatusCurrent, testRedoLogSize),
		}

		selected, endSCN, capped, err := s.selectForSession(files, openThread1, 9000, testRedoLogSize)

		require.NoError(t, err)
		assert.Equal(t, files, selected)
		assert.Equal(t, uint64(9000), endSCN)
		assert.False(t, capped)
	})

	t.Run("stall detection compares the combined multi-thread selection, not a single thread", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 1, 0, 1000, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 2, 1000, 2000, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 3, 2000, 3000, "ARCHIVED", testRedoLogSize),
			mkLogFile(2, 1, 500, 1500, "ARCHIVED", testRedoLogSize),
			mkLogFile(2, 2, 1500, 2500, "ARCHIVED", testRedoLogSize),
			mkLogFile(2, 3, 2500, 3500, "ARCHIVED", testRedoLogSize),
		}

		// Cycle 1: budget starts at minCount (2) for each thread, both capped.
		selected, _, capped, err := s.selectForSession(files, []int{1, 2}, 9000, testRedoLogSize)
		require.NoError(t, err)
		require.Len(t, selected, 4)
		assert.True(t, capped)
		assert.Equal(t, 2, s.count)

		// Cycle 2: identical combined selection across both threads -> stall, budget grows to 3.
		selected, _, capped, err = s.selectForSession(files, []int{1, 2}, 9000, testRedoLogSize)
		require.NoError(t, err)
		require.Len(t, selected, 6, "growth to 3 now covers every file in both threads")
		assert.False(t, capped)
		assert.Equal(t, 3, s.count)
	})

	t.Run("growth does not falsely trigger when only one thread's selection coincides", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		firstFiles := []*LogFile{
			mkLogFile(1, 1, 0, 1000, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 2, 1000, 2000, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 3, 2000, 3000, "ARCHIVED", testRedoLogSize),
			mkLogFile(2, 1, 500, 1500, "ARCHIVED", testRedoLogSize),
			mkLogFile(2, 2, 1500, 2500, "ARCHIVED", testRedoLogSize),
			mkLogFile(2, 3, 2500, 3500, "ARCHIVED", testRedoLogSize),
		}
		// Thread 1's files repeat, but thread 2's are entirely different (real progress), so the combined selection must not be mistaken for a stall.
		secondFiles := []*LogFile{
			mkLogFile(1, 1, 0, 1000, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 2, 1000, 2000, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 3, 2000, 3000, "ARCHIVED", testRedoLogSize),
			mkLogFile(2, 4, 3500, 4500, "ARCHIVED", testRedoLogSize),
			mkLogFile(2, 5, 4500, 5500, "ARCHIVED", testRedoLogSize),
			mkLogFile(2, 6, 5500, 6500, "ARCHIVED", testRedoLogSize),
		}

		selected, _, capped, err := s.selectForSession(firstFiles, []int{1, 2}, 9000, testRedoLogSize)
		require.NoError(t, err)
		require.Len(t, selected, 4)
		assert.True(t, capped)
		assert.Equal(t, 2, s.count)

		selected, _, capped, err = s.selectForSession(secondFiles, []int{1, 2}, 9000, testRedoLogSize)
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
			mkLogFile(1, 1, 0, 100, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 2, 100, 200, logStatusCurrent, testRedoLogSize),
		}
		// Thread 2 (closed) has a backlog far exceeding the shared budget of 2, and deliberately exceeding growthMax too - see the plateau at cycle 3/4 below.
		closedThreadBacklog := []*LogFile{
			mkLogFile(2, 1, 1000, 1100, "ARCHIVED", testRedoLogSize),
			mkLogFile(2, 2, 1100, 1200, "ARCHIVED", testRedoLogSize),
			mkLogFile(2, 3, 1200, 1300, "ARCHIVED", testRedoLogSize),
			mkLogFile(2, 4, 1300, 1400, "ARCHIVED", testRedoLogSize),
			mkLogFile(2, 5, 1400, 1500, "ARCHIVED", testRedoLogSize),
		}
		files := append(append([]*LogFile{}, openThreadFiles...), closedThreadBacklog...)

		// Cycle 1: thread 1 is caught up, but thread 2's backlog exceeds its budget - endSCN must tighten to thread 2's last file, not fall back to dbCurrentSCN.
		selected, endSCN, capped, err := s.selectForSession(files, openThread1, 9000, testRedoLogSize)
		require.NoError(t, err)
		assert.True(t, capped, "thread 2's untightened backlog must cap the session")
		assert.Equal(t, uint64(1199), endSCN, "endSCN tightened to thread 2's 2nd (budgeted) file's NextSCN minus 1")
		require.Len(t, selected, 4, "thread 1's 2 files plus thread 2's budgeted 2 files")
		assert.Equal(t, uint64(1199), s.prevUpperBoundSCN)

		// Cycle 2: same inputs stall, growing the budget to 3 - every file here is 1 redo-log-size, so the derived jump agrees with a plain +1.
		selected, endSCN, capped, err = s.selectForSession(files, openThread1, 9000, testRedoLogSize)
		require.NoError(t, err)
		assert.True(t, capped)
		assert.Equal(t, uint64(1299), endSCN)
		assert.Len(t, selected, 5, "thread 1's 2 files plus thread 2's grown budget of 3")
		assert.Equal(t, 3, s.count)

		// Cycle 3: stalled again, budget grows to 4 - the growthMax ceiling.
		selected, endSCN, capped, err = s.selectForSession(files, openThread1, 9000, testRedoLogSize)
		require.NoError(t, err)
		assert.True(t, capped)
		assert.Equal(t, uint64(1399), endSCN)
		assert.Len(t, selected, 6, "thread 1's 2 files plus thread 2's grown budget of 4")
		assert.Equal(t, 4, s.count)

		// Cycle 4: budget plateaus at growthMax (4), one short of the 5-file backlog - a safe stall (file #5 stays pending, not skipped), not silent data loss; see the next test for the one-shot-sweep case.
		selected, endSCN, capped, err = s.selectForSession(files, openThread1, 9000, testRedoLogSize)
		require.NoError(t, err)
		assert.True(t, capped)
		assert.Equal(t, uint64(1399), endSCN, "plateaued - growth cannot exceed growthMax, and nothing else pushes the boundary further")
		assert.Len(t, selected, 6)
		assert.Equal(t, 4, s.count)
		assert.NotContains(t, selected, closedThreadBacklog[4], "file #5 is never silently included without either budget covering it or a boundary already past it")
	})

	t.Run("closed thread's backlog is swept up in one shot once a boundary already exceeds it", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}

		// Cycle 1: only thread 1 is present, already caught up - legitimately commits the ratchet to dbCurrentSCN before thread 2's backlog is even in the picture.
		openThreadFiles := []*LogFile{
			mkLogFile(1, 1, 0, 100, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 2, 100, 200, logStatusCurrent, testRedoLogSize),
		}
		_, endSCN, capped, err := s.selectForSession(openThreadFiles, openThread1, 9000, testRedoLogSize)
		require.NoError(t, err)
		require.False(t, capped)
		require.Equal(t, uint64(9000), endSCN)
		require.Equal(t, uint64(9000), s.prevUpperBoundSCN)

		// Cycle 2: thread 2's backlog appears for the first time - every file is below the already-committed boundary of 9000, so extension sweeps it all in one cycle, unbounded by growthMax.
		closedThreadBacklog := []*LogFile{
			mkLogFile(2, 1, 1000, 1100, "ARCHIVED", testRedoLogSize),
			mkLogFile(2, 2, 1100, 1200, "ARCHIVED", testRedoLogSize),
			mkLogFile(2, 3, 1200, 1300, "ARCHIVED", testRedoLogSize),
			mkLogFile(2, 4, 1300, 1400, "ARCHIVED", testRedoLogSize),
			mkLogFile(2, 5, 1400, 1500, "ARCHIVED", testRedoLogSize),
		}
		files := append(append([]*LogFile{}, openThreadFiles...), closedThreadBacklog...)

		selected, endSCN, capped, err := s.selectForSession(files, openThread1, 9000, testRedoLogSize)
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

		// Cycle 1: a single open thread, already caught up - commits the ratchet forward to dbCurrentSCN (9000).
		firstCycleFiles := []*LogFile{mkLogFile(1, 1, 0, 500, logStatusCurrent, testRedoLogSize)}
		_, endSCN, capped, err := s.selectForSession(firstCycleFiles, openThread1, 9000, testRedoLogSize)
		require.NoError(t, err)
		require.False(t, capped)
		require.Equal(t, uint64(9000), endSCN)
		require.Equal(t, uint64(9000), s.prevUpperBoundSCN)

		// Cycle 2: the same thread now has a genuine backlog, legitimately capping well below the previously committed boundary of 9000.
		secondCycleFiles := []*LogFile{
			mkLogFile(1, 2, 100, 300, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 3, 300, 500, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 4, 500, 700, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 5, 700, 900, "ARCHIVED", testRedoLogSize),
		}
		_, endSCN, capped, err = s.selectForSession(secondCycleFiles, openThread1, 9500, testRedoLogSize)
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
			mkLogFile(1, 1, 0, 100, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 2, 100, 200, logStatusCurrent, testRedoLogSize),
		}
		closedThreadBacklog := []*LogFile{
			mkLogFile(2, 1, 1000, 1100, "ARCHIVED", testRedoLogSize),
			mkLogFile(2, 2, 1100, 1200, "ARCHIVED", testRedoLogSize),
			mkLogFile(2, 3, 1200, 1300, "ARCHIVED", testRedoLogSize),
		}
		files := append(append([]*LogFile{}, openThreadFiles...), closedThreadBacklog...)

		selected, endSCN, capped, err := s.selectForSession(files, openThread1, 9000, testRedoLogSize)

		require.NoError(t, err)
		require.Len(t, selected, 4, "with no boundary to ratchet against, thread 2 is still limited to its budgeted 2 files - behaviour unchanged from before this feature existed")
		assert.NotContains(t, selected, closedThreadBacklog[2], "the 3rd backlog file must not be pulled in without a previously committed boundary")
		assert.True(t, capped, "thread 2's unselected backlog (files #3-#5) must cap the session - it must not be silently dropped just because thread 1 is caught up")
		assert.Equal(t, uint64(1199), endSCN, "endSCN must be tightened to thread 2's last selected file's NextSCN minus 1, not fall back to dbCurrentSCN")
	})

	t.Run("extension stops once it reaches the thread's genuinely open current log", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}

		// Cycle 1: establish a large previous boundary via an unrelated thread reaching dbCurrentSCN.
		seedFiles := []*LogFile{
			mkLogFile(1, 1, 0, 100, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 2, 100, 200, logStatusCurrent, testRedoLogSize),
		}
		_, endSCN, capped, err := s.selectForSession(seedFiles, openThread1, 9000, testRedoLogSize)
		require.NoError(t, err)
		require.False(t, capped)
		require.Equal(t, uint64(9000), s.prevUpperBoundSCN)
		_ = endSCN

		// Cycle 2: the thread's own short backlog reaches its current log before the boundary of 9000 - extension must land on it and stop, not walk past.
		files := []*LogFile{
			mkLogFile(1, 3, 200, 300, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 4, 300, 500, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 5, 500, 600, logStatusCurrent, testRedoLogSize),
		}

		selected, endSCN, capped, err := s.selectForSession(files, openThread1, 9500, testRedoLogSize)

		require.NoError(t, err)
		assert.False(t, capped, "landing on the genuinely open current log makes the thread caught up")
		assert.Equal(t, uint64(9500), endSCN)
		assert.Equal(t, files, selected, "extension must include exactly this thread's files, stopping at its current log without error")
	})

	// --- derived-jump growth (byte-budget-specific) scenarios ---

	// derivedJumpBacklogFiles is a single-thread backlog of 6 archived files plus a current log, used by the two tests below to exercise deriveGrowthCount's jump-size recovery.
	derivedJumpBacklogFiles := func() []*LogFile {
		return []*LogFile{
			mkLogFile(1, 1, 100, 300, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 2, 300, 500, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 3, 500, 700, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 4, 700, 900, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 5, 900, 1100, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 6, 1100, 1300, "ARCHIVED", testRedoLogSize),
			mkLogFile(1, 7, 1300, 1400, logStatusCurrent, testRedoLogSize),
		}
	}

	t.Run("a stall whose derived jump exceeds +1 grows the budget to that size in a single step", func(t *testing.T) {
		s := &logFileSelector{minCount: 1, growthMax: 16}

		// Cycle 1 (seed): a tiny selection commits a boundary of 1000 without truncating, so growth starts from a clean slate.
		seedFiles := []*LogFile{
			mkLogFile(1, 1, 0, 50, "ARCHIVED", testRedoLogSize/2),
			mkLogFile(1, 2, 50, 100, logStatusCurrent, testRedoLogSize),
		}
		_, _, capped, err := s.selectForSession(seedFiles, openThread1, 1000, testRedoLogSize)
		require.NoError(t, err)
		require.False(t, capped)
		require.Equal(t, 1, s.count)
		require.Equal(t, uint64(1000), s.prevUpperBoundSCN)

		files := derivedJumpBacklogFiles()

		// Cycle 2: budget of 1 truncates to arc#1, but extension pulls in arc#2-#5 too, landing on arc#5 - not yet a stall, first time seeing this backlog.
		selected, endSCN, capped, err := s.selectForSession(files, openThread1, 2000, testRedoLogSize)
		require.NoError(t, err)
		require.True(t, capped)
		require.Len(t, selected, 5)
		require.Equal(t, uint64(1099), endSCN)
		require.Equal(t, 1, s.count, "no stall yet - budget must not have grown")
		require.Equal(t, uint64(1099), s.prevUpperBoundSCN)

		// Cycle 3: identical inputs -> stall. 5 units sit below the boundary unconsumed, so the derived jump takes count straight from 1 to 5, not the +1 step's 2.
		selected, endSCN, capped, err = s.selectForSession(files, openThread1, 2000, testRedoLogSize)
		require.NoError(t, err)
		assert.True(t, capped)
		assert.Equal(t, uint64(1099), endSCN)
		assert.Len(t, selected, 5)
		assert.Equal(t, 5, s.count, "the derived jump must land directly on 5, not climb to 2")
	})

	t.Run("growth ceiling still clamps a derived jump that would otherwise exceed it", func(t *testing.T) {
		s := &logFileSelector{minCount: 1, growthMax: 3}

		seedFiles := []*LogFile{
			mkLogFile(1, 1, 0, 50, "ARCHIVED", testRedoLogSize/2),
			mkLogFile(1, 2, 50, 100, logStatusCurrent, testRedoLogSize),
		}
		_, _, capped, err := s.selectForSession(seedFiles, openThread1, 1000, testRedoLogSize)
		require.NoError(t, err)
		require.False(t, capped)
		require.Equal(t, 1, s.count)

		files := derivedJumpBacklogFiles()

		_, _, _, err = s.selectForSession(files, openThread1, 2000, testRedoLogSize)
		require.NoError(t, err)
		require.Equal(t, 1, s.count, "no stall yet on the first sighting of this backlog")

		// Stall: the derived jump (5) exceeds growthMax (3), so it must clamp to 3.
		_, _, capped, err = s.selectForSession(files, openThread1, 2000, testRedoLogSize)
		require.NoError(t, err)
		assert.True(t, capped)
		assert.Equal(t, 3, s.count, "growthMax must clamp the derived jump, not just a flat +1 step")
	})

	t.Run("a sequence missing from the archived branch does not get skipped when a later sequence is already archived", func(t *testing.T) {
		s := &logFileSelector{minCount: 1, growthMax: 1}

		// Simulates deduplicateLogs' output when seq 10 fails to archive but seq 11 does: archived-first ordering produces [#11, #10, #12], descending where it should ascend.
		seq10 := mkLogFile(1, 10, 1000, 2000, "ACTIVE", testRedoLogSize)
		seq11 := mkLogFile(1, 11, 2000, 3000, "ARCHIVED", testRedoLogSize)
		seq12 := mkLogFile(1, 12, 3000, 4000, logStatusCurrent, testRedoLogSize)
		files := []*LogFile{seq11, seq10, seq12}

		selected, endSCN, capped, err := s.selectForSession(files, openThread1, 4000, testRedoLogSize)

		require.NoError(t, err)
		require.True(t, capped)
		assert.Equal(t, []*LogFile{seq10}, selected, "sequence 10 must be selected first despite arriving out of order")
		assert.Equal(t, uint64(1999), endSCN, "endSCN must stop at seq 10's boundary, not skip ahead to seq 11's")
		assert.NotEqual(t, seq11.NextSCN-1, endSCN, "endSCN must not jump straight to seq 11, leaving seq 10 unmined and unreachable next cycle")
	})
}
