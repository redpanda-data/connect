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
	"encoding/hex"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/logminer/sqlredo"
)

// LogMiner cannot decode the LOB_WRITE rows of a LOB whose locator precedes
// the session's STARTSCN, so open LOB transactions are re-mined from their
// start in every window.

// lobReplay is a window that re-mines open transactions with LOB changes from their start.
type lobReplay struct {
	txns         map[sqlredo.TransactionID]struct{}
	fromSCN      uint64 // lowest start SCN of txns
	processedSCN uint64 // rows of other transactions at or below this SCN were already processed
}

// planLOBReplay selects the open transactions with LOB state, resets them so
// they are rebuilt from scratch, and returns the window to re-mine. It returns
// nil when there is nothing to replay.
func (lm *LogMiner) planLOBReplay(ctx context.Context) (*lobReplay, error) {
	replay := &lobReplay{
		txns:         make(map[sqlredo.TransactionID]struct{}),
		fromSCN:      math.MaxUint64,
		processedSCN: lm.currentSCN,
	}
	// A replay left over from a failed attempt already rolled back its
	// transactions, so they may be absent from the cache until their rows are
	// re-mined. They must stay in the set or their rows would be skipped.
	if prev := lm.lobReplay; prev != nil {
		for id := range prev.txns {
			replay.txns[id] = struct{}{}
			lm.log.Debugf("LOB transaction %s is carried over from an interrupted replay; re-mining it from SCN %d", id, prev.fromSCN)
		}
		replay.fromSCN = prev.fromSCN
		replay.processedSCN = max(prev.processedSCN, lm.currentSCN)
	}

	candidates := make(map[sqlredo.TransactionID]struct{}, len(lm.lobStates)+len(lm.pendingLOBWrites))
	for id := range lm.lobStates {
		candidates[id] = struct{}{}
	}
	for id := range lm.pendingLOBWrites {
		candidates[id] = struct{}{}
	}
	for id := range candidates {
		if _, carried := replay.txns[id]; carried {
			continue
		}
		txn, err := lm.txnCache.GetTransaction(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("getting transaction %s for LOB replay: %w", id, err)
		}
		// Missing or discarded transactions have their events dropped at commit.
		if txn == nil {
			continue
		}
		replay.txns[id] = struct{}{}
		replay.fromSCN = min(replay.fromSCN, txn.SCN)
		lm.log.Debugf("LOB transaction %s started at SCN %d is still open at the window boundary (SCN %d); re-mining it from its start", id, txn.SCN, replay.processedSCN)
	}
	if len(replay.txns) == 0 {
		return nil, nil
	}

	// Record the set before resetting it, so a rollback that fails part way
	// leaves the reset transactions carried over to the next attempt.
	lm.lobReplay = replay
	for id := range replay.txns {
		if err := lm.txnCache.RollbackTransaction(ctx, id); err != nil {
			return nil, fmt.Errorf("resetting transaction %s for LOB replay: %w", id, err)
		}
		delete(lm.lobStates, id)
		delete(lm.pendingLOBWrites, id)
	}
	return replay, nil
}

// end removes a committed or rolled back transaction from the replay, so a
// retry that carries the replay over doesn't re-mine and publish it again.
func (r *lobReplay) end(id sqlredo.TransactionID) {
	if r != nil {
		delete(r.txns, id)
	}
}

// skip reports whether ev was already processed by an earlier window.
func (r *lobReplay) skip(ev *sqlredo.RedoEvent) bool {
	if ev.SCN > r.processedSCN {
		return false
	}
	_, replayed := r.txns[ev.TransactionID]
	return !replayed
}

// queryFilter returns a predicate for rows above processedSCN (:3) or of the
// replayed transactions, matched on the raw XID bytes so it holds regardless
// of host byte order. ok is false if an ID can't be converted.
func (r *lobReplay) queryFilter() (pred string, ok bool) {
	ids := make([]string, 0, len(r.txns))
	for id := range r.txns {
		ids = append(ids, string(id))
	}
	slices.Sort(ids)

	hexIDs := make([]string, 0, len(ids))
	for _, id := range ids {
		raw, err := sqlredo.TransactionID(id).RawXID()
		if err != nil {
			return "", false
		}
		hexIDs = append(hexIDs, "HEXTORAW('"+strings.ToUpper(hex.EncodeToString(raw))+"')")
	}

	var sb strings.Builder
	sb.WriteString(" AND (SCN > :3")
	if len(hexIDs) > 0 {
		sb.WriteString(" OR XID IN (" + strings.Join(hexIDs, ", ") + ")")
	}
	sb.WriteString(")")
	return sb.String(), true
}

// lobReplayWarnAfter is how long one transaction may be re-mined before it is
// logged as a warning.
const lobReplayWarnAfter = 3 * time.Minute

// lobReplayWarner tracks the oldest replay start so a long-running
// transaction warns once rather than every window.
type lobReplayWarner struct {
	fromSCN uint64
	since   time.Time
	warned  bool
}

// observe records the current window's replay (nil for none) and reports
// whether to warn, along with how long fromSCN has been re-mined.
func (w *lobReplayWarner) observe(replay *lobReplay, now time.Time) (warn bool, age time.Duration) {
	if replay == nil {
		*w = lobReplayWarner{}
		return false, 0
	}
	if replay.fromSCN != w.fromSCN {
		*w = lobReplayWarner{fromSCN: replay.fromSCN, since: now}
	}
	age = now.Sub(w.since)
	if w.warned || age < lobReplayWarnAfter {
		return false, age
	}
	w.warned = true
	return true, age
}
