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

// mkLogFile builds a minimal *LogFile fixture for exercising selectForSession.
// FirstSCN/Type are irrelevant to the selection logic and are omitted.
func mkLogFile(sequence int64, nextSCN uint64, isCurrent bool) *LogFile {
	return &LogFile{
		FileName:  fmt.Sprintf("log_%d.arc", sequence),
		NextSCN:   nextSCN,
		Sequence:  sequence,
		IsCurrent: isCurrent,
		Thread:    1,
	}
}

func TestLogFileSelectorSelectForSession(t *testing.T) {
	t.Run("fits within budget returns all files uncapped", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{mkLogFile(1, 1000, false)}

		selected, endSCN, capped := s.selectForSession(files, 5000)

		assert.Equal(t, files, selected)
		assert.Equal(t, uint64(5000), endSCN)
		assert.False(t, capped)
	})

	t.Run("capped selection when the last budgeted file is archived", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 1000, false),
			mkLogFile(2, 2000, false),
			mkLogFile(3, 3000, false),
		}

		selected, endSCN, capped := s.selectForSession(files, 5000)

		require.Len(t, selected, 2)
		assert.Equal(t, files[:2], selected)
		assert.Equal(t, uint64(2000), endSCN, "endSCN should be the last selected file's NextSCN")
		assert.True(t, capped)
	})

	t.Run("last budgeted file being current keeps selection uncapped despite truncation", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 1000, false),
			mkLogFile(2, 2000, true), // still-open current online log, within budget
			mkLogFile(3, 3000, false),
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
			mkLogFile(1, 1000, false),
			mkLogFile(2, 2000, false),
			mkLogFile(3, 3000, false),
			mkLogFile(4, 4000, false),
			mkLogFile(5, 5000, false),
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
			mkLogFile(1, 1000, false),
			mkLogFile(2, 2000, false),
			mkLogFile(3, 3000, false),
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
			mkLogFile(1, 1000, false),
			mkLogFile(2, 2000, false),
			mkLogFile(3, 3000, false),
		}
		secondFiles := []*LogFile{
			mkLogFile(4, 4000, false),
			mkLogFile(5, 5000, false),
			mkLogFile(6, 6000, false),
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

	t.Run("count reset to minCount after a successful cycle takes effect on the next selection", func(t *testing.T) {
		s := &logFileSelector{minCount: 2, growthMax: 4}
		files := []*LogFile{
			mkLogFile(1, 1000, false),
			mkLogFile(2, 2000, false),
			mkLogFile(3, 3000, false),
			mkLogFile(4, 4000, false),
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
