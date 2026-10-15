// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package logminer

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/logminer/sqlredo"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/replication"
)

func TestLOBReplayQueryFilter(t *testing.T) {
	tests := []struct {
		name   string
		txns   []sqlredo.TransactionID
		want   string
		wantOK bool
	}{
		{
			name:   "one txn",
			txns:   []sqlredo.TransactionID{"1.2.3"},
			want:   " AND (SCN > :3 OR (XIDUSN = 1 AND XIDSLT = 2 AND XIDSQN = 3))",
			wantOK: true,
		},
		{
			name:   "two txns sorted",
			txns:   []sqlredo.TransactionID{"9.8.7", "10.2.30", "1.2.3"},
			want:   " AND (SCN > :3 OR (XIDUSN = 1 AND XIDSLT = 2 AND XIDSQN = 3) OR (XIDUSN = 10 AND XIDSLT = 2 AND XIDSQN = 30) OR (XIDUSN = 9 AND XIDSLT = 8 AND XIDSQN = 7))",
			wantOK: true,
		},
		{
			name:   "max uint32",
			txns:   []sqlredo.TransactionID{"4294967295.0.1"},
			want:   " AND (SCN > :3 OR (XIDUSN = 4294967295 AND XIDSLT = 0 AND XIDSQN = 1))",
			wantOK: true,
		},
		{name: "non-numeric part", txns: []sqlredo.TransactionID{"1.x.3"}},
		{name: "injection attempt", txns: []sqlredo.TransactionID{"1.2.3 OR 1=1"}},
		{name: "negative", txns: []sqlredo.TransactionID{"1.-2.3"}},
		{name: "too few parts", txns: []sqlredo.TransactionID{"1.2"}},
		{name: "too many parts", txns: []sqlredo.TransactionID{"1.2.3.4"}},
		{name: "empty id", txns: []sqlredo.TransactionID{""}},
		{name: "out of uint32 range", txns: []sqlredo.TransactionID{"4294967296.2.3"}},
		{name: "one bad among good", txns: []sqlredo.TransactionID{"1.2.3", "bad", "4.5.6"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r := &lobReplay{txns: map[sqlredo.TransactionID]struct{}{}}
			for _, id := range test.txns {
				r.txns[id] = struct{}{}
			}
			// Repeat so a map-order dependent result would surface.
			for range 20 {
				pred, ok := r.queryFilter()
				assert.Equal(t, test.wantOK, ok)
				assert.Equal(t, test.want, pred)
			}
		})
	}
}

func TestQueryLogMinerContentsLOBReplayFilter(t *testing.T) {
	tests := []struct {
		name     string
		replay   *lobReplay
		wantArgs []any
		wantPred string // empty means the plain query
	}{
		{name: "no replay", wantArgs: []any{int64(10), int64(20)}},
		{
			name: "replay filtered",
			replay: &lobReplay{
				txns:         map[sqlredo.TransactionID]struct{}{"2.1.1": {}, "1.2.3": {}},
				processedSCN: 15,
			},
			wantArgs: []any{int64(10), int64(20), int64(15)},
			wantPred: " AND (SCN > :3 OR (XIDUSN = 1 AND XIDSLT = 2 AND XIDSQN = 3) OR (XIDUSN = 2 AND XIDSLT = 1 AND XIDSQN = 1))",
		},
		{
			name: "replay with unparseable id falls back",
			replay: &lobReplay{
				txns:         map[sqlredo.TransactionID]struct{}{"1.2.3": {}, "oops": {}},
				processedSCN: 15,
			},
			wantArgs: []any{int64(10), int64(20)},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conn, fc := newFakeSQLConn(t, nil)
			lm := NewMiner(nil, []replication.UserTable{{Schema: "S", Name: "T"}}, &publisherStub{}, NewDefaultConfig(), nil, service.MockResources().Metrics(), service.NewLoggerFromSlog(slog.Default()))
			lm.lobReplay = test.replay

			// Two windows, to check the per-window statement is not cached.
			for range 2 {
				_, err := lm.queryLogMinerContents(t.Context(), conn, 10, 20, 1, func(context.Context, *sqlredo.RedoEvent) error { return nil })
				require.NoError(t, err)
			}

			fc.mu.Lock()
			calls := slices.Clone(fc.contentCalls)
			fc.mu.Unlock()
			require.Len(t, calls, 2)

			for _, call := range calls {
				assert.Equal(t, test.wantArgs, call.args)
				if test.wantPred == "" {
					assert.Equal(t, lm.logMinerQuery, call.query)
					assert.NotContains(t, call.query, ":3")
				} else {
					assert.Equal(t, strings.Replace(lm.logMinerQuery, logMinerRangeClause, logMinerRangeClause+test.wantPred, 1), call.query)
					assert.Contains(t, call.query, "SCN > :1 AND SCN <= :2 AND (SCN > :3 OR")
				}
			}

			if test.wantPred == "" {
				assert.Equal(t, 1, fc.prepareCount("V$LOGMNR_CONTENTS"), "plain query is prepared once and cached")
				require.NotNil(t, lm.contentStmt)
				assert.Zero(t, fc.closeCount("V$LOGMNR_CONTENTS"), "cached statement stays open")
			} else {
				assert.Equal(t, 2, fc.prepareCount("V$LOGMNR_CONTENTS"), "one filtered statement per window")
				assert.Equal(t, 2, fc.closeCount("V$LOGMNR_CONTENTS"), "per-window statements are closed")
				assert.Nil(t, lm.contentStmt, "the cached plain statement is untouched")
			}
		})
	}
}

func TestLOBReplaySkip(t *testing.T) {
	r := &lobReplay{
		txns:         map[sqlredo.TransactionID]struct{}{"replayed": {}},
		processedSCN: 100,
	}
	tests := []struct {
		name string
		txn  sqlredo.TransactionID
		scn  uint64
		want bool
	}{
		{name: "other txn below processed", txn: "other", scn: 50, want: true},
		{name: "other txn at processed", txn: "other", scn: 100, want: true},
		{name: "other txn above processed", txn: "other", scn: 101, want: false},
		{name: "replayed txn below processed", txn: "replayed", scn: 50, want: false},
		{name: "replayed txn at processed", txn: "replayed", scn: 100, want: false},
		{name: "replayed txn above processed", txn: "replayed", scn: 101, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, r.skip(&sqlredo.RedoEvent{SCN: test.scn, TransactionID: test.txn}))
		})
	}
}

func TestPlanLOBReplay(t *testing.T) {
	ctx := t.Context()
	locator := `declare lob_1 clob; begin select "DOC" into lob_1 from "TESTDB"."T" where "ID" = '42';`

	t.Run("no LOB state", func(t *testing.T) {
		lm, _, cache := newLOBReplayMiner(t)
		require.NoError(t, cache.StartTransaction(ctx, "plain", 10))
		replay, err := lm.planLOBReplay(ctx)
		require.NoError(t, err)
		assert.Nil(t, replay)
		txn, err := cache.GetTransaction(ctx, "plain")
		require.NoError(t, err)
		assert.NotNil(t, txn)
	})

	t.Run("selects and resets LOB transactions", func(t *testing.T) {
		lm, _, cache := newLOBReplayMiner(t)
		lm.currentSCN = 100
		for id, scn := range map[sqlredo.TransactionID]uint64{"withState": 30, "pendingOnly": 20, "plain": 5} {
			require.NoError(t, cache.StartTransaction(ctx, id, scn))
		}
		require.NoError(t, lm.processRedoEvent(ctx, lobEv(31, "withState", sqlredo.OpSelectLobLocator, locator)))
		require.NoError(t, lm.processRedoEvent(ctx, lobWriteEv(32, "withState", "abc", 1)))
		require.NoError(t, lm.processRedoEvent(ctx, lobWriteEv(21, "pendingOnly", "xyz", 1)))
		require.Contains(t, lm.lobStates, sqlredo.TransactionID("withState"))
		require.Contains(t, lm.pendingLOBWrites, sqlredo.TransactionID("pendingOnly"))

		replay, err := lm.planLOBReplay(ctx)
		require.NoError(t, err)
		require.NotNil(t, replay)
		assert.Equal(t, map[sqlredo.TransactionID]struct{}{"withState": {}, "pendingOnly": {}}, replay.txns)
		assert.Equal(t, uint64(20), replay.fromSCN)
		assert.Equal(t, uint64(100), replay.processedSCN)

		for _, id := range []sqlredo.TransactionID{"withState", "pendingOnly"} {
			txn, err := cache.GetTransaction(ctx, id)
			require.NoError(t, err)
			assert.Nil(t, txn, id)
		}
		assert.Empty(t, lm.lobStates)
		assert.Empty(t, lm.pendingLOBWrites)

		txn, err := cache.GetTransaction(ctx, "plain")
		require.NoError(t, err)
		assert.NotNil(t, txn, "transaction without LOB state must be untouched")
	})

	t.Run("candidate missing from cache is not selected", func(t *testing.T) {
		lm, _, _ := newLOBReplayMiner(t)
		lm.currentSCN = 100
		lm.pendingLOBWrites["ghost"] = []*sqlredo.RedoEvent{lobWriteEv(21, "ghost", "xyz", 1)}
		replay, err := lm.planLOBReplay(ctx)
		require.NoError(t, err)
		assert.Nil(t, replay)
	})

	t.Run("carried-over replay is kept", func(t *testing.T) {
		lm, _, cache := newLOBReplayMiner(t)
		lm.currentSCN = 150
		lm.lobReplay = &lobReplay{
			txns:         map[sqlredo.TransactionID]struct{}{"carried": {}},
			fromSCN:      10,
			processedSCN: 100,
		}
		// A fresh LOB transaction with a higher start SCN joins the replay.
		require.NoError(t, cache.StartTransaction(ctx, "fresh", 120))
		require.NoError(t, lm.processRedoEvent(ctx, lobWriteEv(121, "fresh", "xyz", 1)))

		replay, err := lm.planLOBReplay(ctx)
		require.NoError(t, err)
		require.NotNil(t, replay)
		assert.Equal(t, map[sqlredo.TransactionID]struct{}{"carried": {}, "fresh": {}}, replay.txns)
		assert.Equal(t, uint64(10), replay.fromSCN, "carried-over fromSCN must be kept")
		assert.Equal(t, uint64(150), replay.processedSCN, "max of previous and current SCN")

		lm.lobReplay = replay
		lm.currentSCN = 120
		again, err := lm.planLOBReplay(ctx)
		require.NoError(t, err)
		require.NotNil(t, again, "carried txns stay even when absent from the cache")
		assert.Equal(t, uint64(150), again.processedSCN)
	})

	t.Run("partial rollback failure carries all transactions over", func(t *testing.T) {
		lm, _, inner := newLOBReplayMiner(t)
		cache := &failingRollbackCache{TransactionCache: inner, failID: "t2", fail: true}
		lm.txnCache = cache
		lm.currentSCN = 100
		for id, scn := range map[sqlredo.TransactionID]uint64{"t1": 30, "t2": 20, "t3": 40} {
			require.NoError(t, cache.StartTransaction(ctx, id, scn))
			require.NoError(t, lm.processRedoEvent(ctx, lobEv(scn+1, id, sqlredo.OpSelectLobLocator, locator)))
			require.NoError(t, lm.processRedoEvent(ctx, lobWriteEv(scn+2, id, "abc", 1)))
		}
		want := map[sqlredo.TransactionID]struct{}{"t1": {}, "t2": {}, "t3": {}}

		replay, err := lm.planLOBReplay(ctx)
		require.ErrorContains(t, err, "resetting transaction t2")
		assert.Nil(t, replay)
		require.NotNil(t, lm.lobReplay)
		assert.Equal(t, want, lm.lobReplay.txns)
		assert.Equal(t, uint64(20), lm.lobReplay.fromSCN)

		cache.fail = false
		replay, err = lm.planLOBReplay(ctx)
		require.NoError(t, err)
		require.NotNil(t, replay)
		assert.Equal(t, want, replay.txns, "txns already rolled back must be carried over")
		assert.Equal(t, uint64(20), replay.fromSCN)
		assert.Empty(t, lm.lobStates)
		assert.Empty(t, lm.pendingLOBWrites)
	})
}

func TestLOBReplayWarner(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	scn := func(n uint64) *lobReplay { return &lobReplay{fromSCN: n} }

	steps := []struct {
		name     string
		replay   *lobReplay
		offset   time.Duration
		wantWarn bool
		wantAge  time.Duration
	}{
		{"first sight starts the clock", scn(100), 0, false, 0},
		{"just before threshold", scn(100), lobReplayWarnAfter - time.Second, false, lobReplayWarnAfter - time.Second},
		{"exactly at threshold warns", scn(100), lobReplayWarnAfter, true, lobReplayWarnAfter},
		{"same fromSCN does not warn again", scn(100), lobReplayWarnAfter + time.Minute, false, lobReplayWarnAfter + time.Minute},
		{"different fromSCN resets the clock", scn(200), 10 * time.Minute, false, 0},
		{"new fromSCN not warned before its own threshold", scn(200), 10*time.Minute + lobReplayWarnAfter - time.Second, false, lobReplayWarnAfter - time.Second},
		{"new fromSCN warns after its own threshold", scn(200), 10*time.Minute + lobReplayWarnAfter, true, lobReplayWarnAfter},
		{"nil replay clears tracking", nil, 16 * time.Minute, false, 0},
		{"same fromSCN after nil restarts the clock", scn(200), 17 * time.Minute, false, 0},
		{"same fromSCN after nil can warn again", scn(200), 17*time.Minute + lobReplayWarnAfter, true, lobReplayWarnAfter},
	}

	var w lobReplayWarner
	for _, step := range steps {
		warn, age := w.observe(step.replay, base.Add(step.offset))
		assert.Equal(t, step.wantWarn, warn, step.name)
		assert.Equal(t, step.wantAge, age, step.name)
	}
}

// TestLOBReplayAcrossWindowBoundary emulates miningCycle: a LOB write in
// progress at the window boundary is re-mined from its transaction start while
// already-processed rows of other transactions are skipped.
func TestLOBReplayAcrossWindowBoundary(t *testing.T) {
	ctx := t.Context()
	insert := func(scn uint64, txn sqlredo.TransactionID) *sqlredo.RedoEvent {
		return lobEv(scn, txn, sqlredo.OpInsert, `insert into "TESTDB"."T" ("ID") values ('7')`)
	}
	update := func(scn uint64) *sqlredo.RedoEvent {
		return lobEv(scn, "txA", sqlredo.OpUpdate, `update "TESTDB"."T" set "ID" = '42' where "ID" = '42'`)
	}
	locator := func(scn uint64) *sqlredo.RedoEvent {
		return lobEv(scn, "txA", sqlredo.OpSelectLobLocator,
			`declare lob_1 clob; begin select "DOC" into lob_1 from "TESTDB"."T" where "ID" = '42';`)
	}
	start := func(scn uint64, txn sqlredo.TransactionID) *sqlredo.RedoEvent {
		return &sqlredo.RedoEvent{SCN: scn, TransactionID: txn, Operation: sqlredo.OpStart}
	}
	commit := func(scn uint64, txn sqlredo.TransactionID) *sqlredo.RedoEvent {
		return &sqlredo.RedoEvent{SCN: scn, TransactionID: txn, Operation: sqlredo.OpCommit}
	}

	window1 := func() []*sqlredo.RedoEvent {
		return []*sqlredo.RedoEvent{
			start(10, "txA"), update(20), locator(20), lobWriteEv(30, "txA", "hello", 1),
			start(40, "txB"), insert(50, "txB"), commit(60, "txB"),
		}
	}
	// Window 2 starts at SCN 9, so it re-mines everything from txA's start.
	window2 := func() []*sqlredo.RedoEvent {
		return append(window1(),
			lobWriteEv(110, "txA", "world", 6), commit(120, "txA"))
	}
	feed := func(lm *LogMiner, replay *lobReplay, evs []*sqlredo.RedoEvent) {
		for _, ev := range evs {
			if replay == nil || !replay.skip(ev) {
				require.NoError(t, lm.processRedoEvent(ctx, ev))
			}
		}
	}
	t.Run("replay assembles the full LOB once", func(t *testing.T) {
		lm, pub, _ := newLOBReplayMiner(t)
		feed(lm, nil, window1())
		require.Len(t, pub.messages, 1, "txB is published in window 1")

		lm.currentSCN = 100
		replay, err := lm.planLOBReplay(ctx)
		require.NoError(t, err)
		require.NotNil(t, replay)
		assert.Equal(t, uint64(10), replay.fromSCN)
		assert.Equal(t, map[sqlredo.TransactionID]struct{}{"txA": {}}, replay.txns)

		feed(lm, replay, window2())

		var txA, txB int
		for _, m := range pub.messages {
			d, _ := m.Data.(map[string]any)
			if _, ok := d["DOC"]; ok {
				txA++
				assert.Equal(t, "helloworld", d["DOC"])
			} else {
				txB++
			}
		}
		assert.Equal(t, 1, txA, "txA published exactly once")
		assert.Equal(t, 1, txB, "txB published exactly once")
		assert.Len(t, pub.messages, 2)
	})

	t.Run("re-feeding without a reset duplicates output", func(t *testing.T) {
		lm, pub, _ := newLOBReplayMiner(t)
		feed(lm, nil, window1())
		// No planLOBReplay and no skip: the sanity baseline the reset protects against.
		feed(lm, nil, window2())

		assert.Greater(t, len(pub.messages), 2, "re-fed rows are published again without the reset and skip")
	})
}

func newLOBReplayMiner(t *testing.T) (*LogMiner, *publisherStub, TransactionCache) {
	t.Helper()
	cache := NewInMemoryCache(0, service.MockResources().Metrics(), service.NewLoggerFromSlog(slog.Default()))
	pub := &publisherStub{}
	lm := newLogMiner(pub, cache)
	lm.cfg.LOBEnabled = true
	lm.lobColTypes = map[string]string{"TESTDB.T.DOC": "CLOB"}
	return lm, pub, cache
}

func lobEv(scn uint64, txn sqlredo.TransactionID, op sqlredo.Operation, redo string) *sqlredo.RedoEvent {
	ev := &sqlredo.RedoEvent{SCN: scn, TransactionID: txn, Operation: op}
	if redo != "" {
		ev.SchemaName = sql.NullString{String: "TESTDB", Valid: true}
		ev.TableName = sql.NullString{String: "T", Valid: true}
		ev.SQLRedo = sql.NullString{String: redo, Valid: true}
	}
	return ev
}

func lobWriteEv(scn uint64, txn sqlredo.TransactionID, data string, offset int) *sqlredo.RedoEvent {
	return lobEv(scn, txn, sqlredo.OpLobWrite,
		fmt.Sprintf(" buf_c := '%s';\n  dbms_lob.write(loc_c, %d, %d, buf_c);", data, len(data), offset))
}

// failingRollbackCache fails RollbackTransaction for failID while fail is set.
type failingRollbackCache struct {
	TransactionCache
	failID sqlredo.TransactionID
	fail   bool
}

func (c *failingRollbackCache) RollbackTransaction(ctx context.Context, id sqlredo.TransactionID) error {
	if c.fail && id == c.failID {
		return fmt.Errorf("injected rollback failure for %s", id)
	}
	return c.TransactionCache.RollbackTransaction(ctx, id)
}
