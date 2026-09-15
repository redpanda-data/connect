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

func mkLogFile(sequence int64, nextSCN uint64, status string) *LogFile {
	return &LogFile{
		FileName: fmt.Sprintf("log_%d.arc", sequence),
		NextSCN:  nextSCN,
		Sequence: sequence,
		Status:   status,
		Thread:   1,
	}
}

func TestLogFileSelectorSelectForSession(t *testing.T) {
	t.Run("fits within budget returns all files uncapped", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{mkLogFile(1, 1000, "ARCHIVED")}

		selected, endSCN, capped := s.selectForSession(files, 5000)

		assert.Equal(t, files, selected)
		assert.Equal(t, uint64(5000), endSCN)
		assert.False(t, capped)
	})

	t.Run("capped selection when the last budgeted file is archived", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 1000, "ARCHIVED"),
			mkLogFile(2, 2000, "ARCHIVED"),
			mkLogFile(3, 3000, "ARCHIVED"),
		}

		selected, endSCN, capped := s.selectForSession(files, 5000)

		require.Len(t, selected, 2)
		assert.Equal(t, files[:2], selected)
		assert.Equal(t, uint64(1999), endSCN, "endSCN should be the last selected file's NextSCN minus 1 - NextSCN belongs to the following, unselected file")
		assert.True(t, capped)
	})

	t.Run("last budgeted file being current keeps selection uncapped despite truncation", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 1000, "ARCHIVED"),
			mkLogFile(2, 2000, logStatusCurrent), // still-open current online log, within budget
			mkLogFile(3, 3000, "ARCHIVED"),
		}

		selected, endSCN, capped := s.selectForSession(files, 5000)

		require.Len(t, selected, 2, "file list should still be truncated to the budget")
		assert.Equal(t, files[:2], selected)
		assert.Equal(t, uint64(5000), endSCN, "endSCN should fall back to the live current SCN")
		assert.False(t, capped, "a current last file must not be reported as capped")
	})

	t.Run("repeated identical selection stalls and grows the budget up to growthMax then plateaus", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 1000, "ARCHIVED"),
			mkLogFile(2, 2000, "ARCHIVED"),
			mkLogFile(3, 3000, "ARCHIVED"),
			mkLogFile(4, 4000, "ARCHIVED"),
			mkLogFile(5, 5000, "ARCHIVED"),
		}

		// Cycle 1: fresh selector, budget starts at minCount (2), no stall detected yet.
		selected, _, capped := s.selectForSession(files, 9000)
		require.Len(t, selected, 2)
		assert.True(t, capped)
		assert.Equal(t, 2, s.count)

		// Cycle 2: identical file set selected again -> stall detected, budget grows to 3.
		selected, _, capped = s.selectForSession(files, 9000)
		require.Len(t, selected, 3)
		assert.True(t, capped)
		assert.Equal(t, 3, s.count)

		// Cycle 3: still stalled -> budget grows to 4 (growthMax).
		selected, _, capped = s.selectForSession(files, 9000)
		require.Len(t, selected, 4)
		assert.True(t, capped)
		assert.Equal(t, 4, s.count)

		// Cycle 4: still stalled, but growthMax is already reached -> budget plateaus at 4
		// rather than growing unboundedly to 5.
		selected, _, capped = s.selectForSession(files, 9000)
		require.Len(t, selected, 4)
		assert.True(t, capped)
		assert.Equal(t, 4, s.count)

		// Cycle 5: one more stalled cycle for good measure, confirming the plateau holds.
		selected, _, capped = s.selectForSession(files, 9000)
		require.Len(t, selected, 4)
		assert.True(t, capped)
		assert.Equal(t, 4, s.count)
	})

	t.Run("growth that ends up covering all available files returns everything uncapped", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 1000, "ARCHIVED"),
			mkLogFile(2, 2000, "ARCHIVED"),
			mkLogFile(3, 3000, "ARCHIVED"),
		}

		// Cycle 1: budget starts at minCount (2), capped selection of the first two files.
		selected, _, capped := s.selectForSession(files, 9000)
		require.Len(t, selected, 2)
		assert.True(t, capped)

		// Cycle 2: stall grows the budget to 3, which now covers every file - this
		// must behave like the "fits within budget" case, not a partial, truncated one.
		selected, endSCN, capped := s.selectForSession(files, 9000)
		assert.Equal(t, files, selected)
		assert.Equal(t, uint64(9000), endSCN)
		assert.False(t, capped)
	})

	t.Run("different non-overlapping file sets do not trigger growth", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		firstFiles := []*LogFile{
			mkLogFile(1, 1000, "ARCHIVED"),
			mkLogFile(2, 2000, "ARCHIVED"),
			mkLogFile(3, 3000, "ARCHIVED"),
		}
		secondFiles := []*LogFile{
			mkLogFile(4, 4000, "ARCHIVED"),
			mkLogFile(5, 5000, "ARCHIVED"),
			mkLogFile(6, 6000, "ARCHIVED"),
		}

		selected, _, capped := s.selectForSession(firstFiles, 9000)
		require.Len(t, selected, 2)
		assert.True(t, capped)
		assert.Equal(t, 2, s.count, "budget should remain at minCount after real forward progress")

		selected, _, capped = s.selectForSession(secondFiles, 9000)
		require.Len(t, selected, 2)
		assert.Equal(t, secondFiles[:2], selected)
		assert.True(t, capped)
		assert.Equal(t, 2, s.count, "a different file set must not be mistaken for a stall")
	})

	t.Run("last budgeted file switched away but not archived must still be capped", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(8, 8000, "ARCHIVED"),
			mkLogFile(9, 9000, "ACTIVE"),           // switched away from, not yet archived - NOT open
			mkLogFile(10, 10000, logStatusCurrent), // the real, genuinely open current log
		}

		selected, endSCN, capped := s.selectForSession(files, 20000)

		require.Len(t, selected, 2, "budget should truncate to [arch#8, online#9], excluding online#10")
		assert.Equal(t, files[:2], selected)
		assert.Equal(t, uint64(8999), endSCN, "endSCN must be online#9's NextSCN minus 1, not dbCurrentSCN")
		assert.True(t, capped, "must be capped - online#10 was not actually selected or mined")
	})

	t.Run("genuinely open current log as the last budgeted file stays uncapped", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(8, 8000, "ARCHIVED"),
			mkLogFile(9, 9000, logStatusCurrent), // genuinely open current log
		}

		selected, endSCN, capped := s.selectForSession(files, 20000)

		assert.Equal(t, files, selected)
		assert.Equal(t, uint64(20000), endSCN, "endSCN should fall back to the live current SCN")
		assert.False(t, capped)
	})

	t.Run("endSCN must not land on the next unselected file's first SCN", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 1000, "ARCHIVED"),
			mkLogFile(2, 2000, "ARCHIVED"),
			mkLogFile(3, 3000, "ARCHIVED"),
		}

		_, endSCN, capped := s.selectForSession(files, 9000)

		require.True(t, capped)
		assert.Less(t, endSCN, files[1].NextSCN, "endSCN must stop strictly before the unselected next file's first SCN")
		assert.Equal(t, files[1].NextSCN-1, endSCN)
	})

	t.Run("count reset to minCount after a successful cycle takes effect on the next selection", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 1000, "ARCHIVED"),
			mkLogFile(2, 2000, "ARCHIVED"),
			mkLogFile(3, 3000, "ARCHIVED"),
			mkLogFile(4, 4000, "ARCHIVED"),
		}

		// Grow the budget to 3 via a stalled repeat, mirroring what miningCycle would see
		// across two capped cycles.
		_, _, _ = s.selectForSession(files, 9000)
		selected, _, capped := s.selectForSession(files, 9000)
		require.Len(t, selected, 3)
		assert.True(t, capped)
		assert.Equal(t, 3, s.count)

		// miningCycle resets count to LogCountMin whenever a cycle completes uncapped;
		// simulate that directly (no exported method exists) and confirm the next
		// selection honours the reset budget rather than the stale grown one.
		s.count = s.minCount

		selected, _, capped = s.selectForSession(files, 9000)
		require.Len(t, selected, 2, "selection should honour the reset budget, not the stale grown one")
		assert.Equal(t, files[:2], selected)
		assert.True(t, capped)
	})
}
