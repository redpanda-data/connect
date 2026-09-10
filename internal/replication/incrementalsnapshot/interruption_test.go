// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package incrementalsnapshot

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file holds one property test rather than a case per bug.
//
// Every durability defect found in the signal-driven snapshot has had the
// same shape: a request or a chunk that reached the coordinator but not a
// checkpoint, so an interruption lost it. Each was fixed with a test for
// that one point. This instead enumerates the interruption points and
// asserts the property at all of them, which is the only shape that covers
// the ones nobody has thought of yet.

// interruptDeps serves rows per table and honours the lower bound the way a
// real keyset query does, so a resumed coordinator reads from its checkpoint
// rather than from a cursor the double keeps -- which is what makes an
// interruption observable at all.
type interruptDeps struct {
	depsFaults

	tables    map[string]*mockTable
	chunkSize int
	watermark testWatermark
	fetches   int
}

func newInterruptDeps(tables map[string]*mockTable, chunkSize int) *interruptDeps {
	return &interruptDeps{
		tables:    tables,
		chunkSize: chunkSize,
		// Quiesced, so every read looks undisturbed and the drain runs.
		// Interruption points then fall on chunk boundaries rather than on
		// whenever a watermark happens to clear.
		watermark: testWatermark{Xmin: 100, Xmax: 100},
	}
}

func (d *interruptDeps) ResolvePrimaryKey(_ context.Context, table TableID) ([]string, error) {
	if err := d.check("ResolvePrimaryKey"); err != nil {
		return nil, err
	}
	return d.tables[table.String()].pkCols, nil
}

func (d *interruptDeps) ResolveMaxKey(_ context.Context, table TableID, _ []string) (PrimaryKey, error) {
	if err := d.check("ResolveMaxKey"); err != nil {
		return nil, err
	}
	return d.tables[table.String()].maxPK, nil
}

func (d *interruptDeps) ResolveWatermark(context.Context) (testWatermark, error) {
	if err := d.check("ResolveWatermark"); err != nil {
		return testWatermark{}, err
	}
	return d.watermark, nil
}

func (d *interruptDeps) ForceFreshTransaction(context.Context) error {
	return d.check("ForceFreshTransaction")
}

func (d *interruptDeps) FetchChunk(_ context.Context, table TableID, _ []string, lower, _ PrimaryKey, limit int) ([]Row, error) {
	if err := d.check("FetchChunk"); err != nil {
		return nil, err
	}
	d.fetches++
	return sortedRowsAfter(d.tables[table.String()].rows, lower, limit), nil
}

// snapshotRun is one process lifetime: it drives commits until the queue
// empties or the interruption point arrives, and records what a caller could
// have kept.
type snapshotRun struct {
	delivered []int
	// persisted is the newest state the caller saw from inside emit, so the
	// newest it could have written durably. State read anywhere else does
	// not survive a crash, which is the whole point.
	persisted *State
	emits     int
}

// drive runs commits until the queue empties or stopAfterEmits emits have
// happened, whichever comes first. A zero stopAfterEmits runs to completion.
func drive(t *testing.T, coord *Coordinator[uint64, testWatermark], from uint64, stopAfterEmits int) *snapshotRun {
	t.Helper()

	run := &snapshotRun{}
	emit := func(rows []Row) error {
		run.emits++
		for _, row := range rows {
			run.delivered = append(run.delivered, row.PK[0].(int))
		}
		run.persisted = coord.State()
		return nil
	}

	// A generous bound: the loop exits on idle, and this only stops a test
	// from hanging if that ever stops happening.
	for pos := from; pos < from+100; pos++ {
		if stopAfterEmits > 0 && run.emits >= stopAfterEmits {
			return run
		}
		if _, err := coord.OnCommit(t.Context(), pos, emit); err != nil {
			require.NoError(t, err)
		}
		if coord.Idle() {
			return run
		}
	}
	return run
}

// TestCoordinatorLosesNothingWhenInterrupted interrupts a signalled backfill
// after every possible number of emits and checks the same property each
// time: resuming from the last state the caller could have persisted
// delivers every remaining row.
//
// Rows may be re-delivered. State never reports an unflushed chunk, so a
// resume re-reads the chunk that was in flight -- at-least-once is the
// contract, and consumers are told to treat rows as idempotent upserts. What
// must never happen is a row, or a whole requested table, going missing.
func TestCoordinatorLosesNothingWhenInterrupted(t *testing.T) {
	tableA := TableID{Schema: "public", Table: "a"}
	tableB := TableID{Schema: "public", Table: "b"}
	// tableC is only ever requested on a resumed coordinator, before Start.
	tableC := TableID{Schema: "public", Table: "c"}

	const chunkSize = 2
	rowsA := []Row{rowFor(tableA, 1), rowFor(tableA, 2), rowFor(tableA, 3), rowFor(tableA, 4), rowFor(tableA, 5)}
	rowsB := []Row{rowFor(tableB, 10), rowFor(tableB, 11), rowFor(tableB, 12)}
	rowsC := []Row{rowFor(tableC, 20), rowFor(tableC, 21)}
	rowsCIDs := []int{20, 21}

	newDeps := func() *interruptDeps {
		return newInterruptDeps(map[string]*mockTable{
			tableA.String(): {pkCols: []string{"id"}, rows: rowsA, maxPK: PrimaryKey{5}},
			tableB.String(): {pkCols: []string{"id"}, rows: rowsB, maxPK: PrimaryKey{12}},
			tableC.String(): {pkCols: []string{"id"}, rows: rowsC, maxPK: PrimaryKey{21}},
		}, chunkSize)
	}

	newCoordinator := func(t *testing.T, resume *State) *Coordinator[uint64, testWatermark] {
		t.Helper()
		coord, err := NewCoordinator(testConfig{ChunkSize: chunkSize, Deps: newDeps()}, resume)
		require.NoError(t, err)
		return coord
	}

	wantA := []int{1, 2, 3, 4, 5}
	wantAB := []int{1, 2, 3, 4, 5, 10, 11, 12}

	// How many emits a complete, uninterrupted run takes, so the loop below
	// covers every point inside one rather than a guessed range.
	// The one case where re-requesting is legitimate: nothing was
	// checkpointed, so nothing acknowledged the signal's position either and
	// the connector reads that row again.
	t.Run("interrupted before any checkpoint", func(t *testing.T) {
		first := newCoordinator(t, nil)
		require.NotEmpty(t, first.AddTables([]TableID{tableA}))
		require.NoError(t, first.Start(t.Context()))

		resumed := newCoordinator(t, nil)
		require.NotEmpty(t, resumed.AddTables([]TableID{tableA}))
		require.NoError(t, resumed.Start(t.Context()))

		run := drive(t, resumed, 300, 0)
		assert.ElementsMatch(t, wantA, dedupe(run.delivered))
	})

	t.Run("uninterrupted", func(t *testing.T) {
		coord := newCoordinator(t, nil)
		coord.AddTables([]TableID{tableA, tableB})
		require.NoError(t, coord.Start(t.Context()))

		run := drive(t, coord, 200, 0)
		assert.ElementsMatch(t, wantAB, run.delivered)
		assert.True(t, coord.Idle())
	})

	// requestAfterStart makes the request the way a signal does: once the
	// coordinator is already running. Seeding before Start is a different
	// case -- nothing has been acknowledged yet, so no checkpoint is owed.
	//
	// arriving names when the request lands relative to a backfill.
	for _, arriving := range []struct {
		name     string
		queued   []TableID
		request  []TableID
		expected []int
	}{
		{
			name:     "while idle",
			request:  []TableID{tableA, tableB},
			expected: wantAB,
		},
		{
			name:     "mid-backfill",
			queued:   []TableID{tableA},
			request:  []TableID{tableB},
			expected: wantAB,
		},
	} {
		t.Run("requested "+arriving.name, func(t *testing.T) {
			// The request's own commit must checkpoint it. The connector
			// acknowledges that row's position with whatever state
			// accompanies it, so a request absent from that state is
			// unrecoverable: the row never streams again.
			t.Run("its own commit checkpoints it", func(t *testing.T) {
				// An in-flight watermark, so no window can close on the
				// commit under test. Otherwise a chunk release would happen
				// to coincide and mask a missing queue checkpoint.
				deps := newDeps()
				deps.watermark = testWatermark{Xmin: 100, Xmax: 1 << 40}
				coord, err := NewCoordinator(testConfig{ChunkSize: chunkSize, Deps: deps}, nil)
				require.NoError(t, err)

				coord.AddTables(arriving.queued)
				require.NoError(t, coord.Start(t.Context()))
				if len(arriving.queued) > 0 {
					// Get the backfill genuinely under way first: the chunk
					// is buffered, its window still open.
					_, err := coord.OnCommit(t.Context(), 200, func([]Row) error { return nil })
					require.NoError(t, err)
				}

				require.NotEmpty(t, coord.AddTables(arriving.request))

				before := &snapshotRun{}
				emit := func([]Row) error {
					before.emits++
					before.persisted = coord.State()
					return nil
				}
				_, err = coord.OnCommit(t.Context(), 300, emit)
				require.NoError(t, err)

				require.Positive(t, before.emits,
					"the commit carrying the request emitted no checkpoint, so the request is lost once its position is acknowledged")
				require.NotNil(t, before.persisted)
				for _, table := range arriving.request {
					assert.Contains(t, before.persisted.Tables, table)
					assert.Contains(t, before.persisted.RemainingTables, table)
				}
			})

			// And every interruption point after that must still deliver
			// everything, resuming only from what was checkpointed. No
			// re-request here: the position that carried it is acknowledged.
			t.Run("interrupted at every point after it", func(t *testing.T) {
				for stopAfter := 1; stopAfter <= 12; stopAfter++ {
					t.Run(label(stopAfter), func(t *testing.T) {
						first := newCoordinator(t, nil)
						first.AddTables(arriving.queued)
						require.NoError(t, first.Start(t.Context()))

						pre := &snapshotRun{}
						if len(arriving.queued) > 0 {
							pre = drive(t, first, 200, 1)
						}
						require.NotEmpty(t, first.AddTables(arriving.request))

						run := drive(t, first, 300, stopAfter)
						require.NotNil(t, run.persisted,
							"the request's commit must have checkpointed something to resume from")

						// Requesting a table before Start, on a resumed
						// coordinator, must survive the checkpoint load.
						resumed := newCoordinator(t, run.persisted)
						require.Equal(t, []TableID{tableC}, resumed.AddTables([]TableID{tableC}))
						require.NoError(t, resumed.Start(t.Context()))
						after := drive(t, resumed, 500, 0)

						var got []int
						got = append(got, pre.delivered...)
						got = append(got, run.delivered...)
						got = append(got, after.delivered...)

						assert.ElementsMatch(t, append(append([]int{}, arriving.expected...), rowsCIDs...), dedupe(got),
							"every row must arrive across the interruption, the table seeded on resume included")
						assert.True(t, resumed.Idle(), "the resumed run must finish the queue")
						assert.LessOrEqual(t, len(got)-len(dedupe(got)), chunkSize*2,
							"a resume must not re-read more than the chunks in flight")
					})
				}
			})
		})
	}
}

func label(n int) string {
	return "after " + strconv.Itoa(n) + " emits"
}

func dedupe(in []int) []int {
	seen := make(map[int]struct{}, len(in))
	out := make([]int, 0, len(in))
	for _, v := range in {
		if _, dup := seen[v]; dup {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}
