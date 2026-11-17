// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// Table maintenance covers the table-level updates the commit.* fields
// imply, today snapshot expiry (commit.max_snapshot_age). They are applied
// as their own commits, after a data commit has landed, and never on the
// data commit itself. No rows ride on a maintenance commit, so
// its failure never fails a data commit, and a catalog that rejects one
// cannot block data. The pass runs synchronously after the data commit,
// under the commit lock, so its round trips (one, or more when a conflict
// is retried) are added to that commit's acknowledgement, at most once per
// interval per table. A pass that stages nothing (nothing eligible to
// expire) makes no catalog call, since
// iceberg-go only commits a transaction that carries an update.
//
// A pass runs at most once per commit.maintenance_interval per table,
// booked when it starts, so a failing pass waits out the interval like a
// successful one. Failures are logged once per pass, counted in the
// maintenance-failure metric, and left for the next pass: the next attempt
// computes the same removals from fresh metadata, so nothing is lost.

// defaultMaintenanceInterval is the commit.maintenance_interval default (the
// field's spec Default must parse to it, which
// TestParseCommitConfigMaintenanceInterval pins) and backs
// CommitConfig.MaintenanceInterval when a direct caller leaves it unset. Ten
// minutes bounds the extra round trips on a table that commits
// every few seconds, and on a table that commits less often than this every
// commit is followed by a pass, which only costs a round trip when something
// is eligible.
const defaultMaintenanceInterval = 10 * time.Minute

// Labels for the maintenance-failure counter, one per maintenance operation.
const maintenanceOpExpireSnapshots = "expire_snapshots"

// slowMaintenanceStage is how long staging one maintenance update may take
// before the pass warns about it. iceberg-go's expiry walk is quadratic in
// the table's snapshot count (a linear snapshot lookup inside the ancestor
// walk), and it runs under the commit lock, so a table with tens of
// thousands of retained snapshots stalls its data commits for seconds once
// per pass. A shorter max_snapshot_age is the remedy until the walk is
// fixed upstream.
const slowMaintenanceStage = time.Second

// maintenanceReloadTimeout bounds the table reload that follows a maintenance
// commit whose caller context ended before its outcome was known. It runs on
// a detached context, so this is also the most a shutdown waits for it.
const maintenanceReloadTimeout = 5 * time.Second

// tableMaintenanceState is the per-table record the maintenance pass keeps
// across writer generations. The router owns one per table and hands it to
// every committer it creates for that table (CommitConfig.Maintenance), so a
// recreated writer neither repeats a pass the interval has not yet allowed
// nor repeats the once-per-table warnings.
type tableMaintenanceState struct {
	// nextPassAt is the earliest time, in unix nanoseconds, the next pass may
	// run. Zero means no pass has run yet, so the first data commit to a
	// table is followed by one.
	nextPassAt atomic.Int64
	// tableAgeWarnOnce: a mismatch between the table's own retention property
	// and the configured field is static for the life of the table, so it is
	// logged once per table.
	tableAgeWarnOnce sync.Once
}

func newTableMaintenanceState() *tableMaintenanceState { return &tableMaintenanceState{} }

// claimPass reports whether a pass may run at now and, if so, books the next
// one for now plus every. Booking happens before the pass runs, not after it
// succeeds, so a failing pass is not retried until the interval has passed.
// The compare-and-swap keeps two committers for the same table (an old and a
// new writer generation overlapping) from both claiming the same slot.
func (s *tableMaintenanceState) claimPass(now time.Time, every time.Duration) bool {
	next := s.nextPassAt.Load()
	if now.UnixNano() < next {
		return false
	}
	return s.nextPassAt.CompareAndSwap(next, now.Add(every).UnixNano())
}

// maintainLocked runs the maintenance pass when one is due. Callers hold
// c.commitMu and have just landed a data commit, so c.table is current and
// nothing else commits through this committer meanwhile. It runs
// synchronously: a pass in its own goroutine would race the next data commit
// for the same branch head and turn into 409s on both sides.
//
// The retained-snapshot gauge is refreshed at the end of the
// pass, not on every commit: reading the snapshot list clones it, which is
// too much for the data path on a table with many snapshots.
//
// The pass is best-effort all the way down: a panic inside it (the walk
// reads metadata other engines wrote) is logged and counted, never allowed
// to take the writer down after a data commit that already landed. The
// slot stays booked, so a repeatable panic costs one log line per interval.
func (c *committer) maintainLocked(ctx context.Context) {
	if ctx.Err() != nil || !c.maint.claimPass(time.Now(), c.cfg.MaintenanceInterval) {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			c.metrics.incrMaintenanceFailure("panic")
			c.logger.Errorf("Table maintenance for %s panicked and is skipped until the next pass in %s: %v\n%s", c.tableName(), c.cfg.MaintenanceInterval, r, debug.Stack())
		}
	}()
	if c.cfg.MaxSnapshotAge > 0 {
		c.warnTableAgeMismatch()
		c.commitMaintenance(ctx, maintenanceOpExpireSnapshots, c.stageSnapshotExpiry)
	}
	c.observeRetainedSnapshots()
}

// commitMaintenance stages one maintenance update on a fresh transaction over
// c.table and commits it. stage reports whether it put anything on the
// transaction; when it did not, there is nothing to commit and the pass costs
// nothing.
//
// A clean conflict (another writer moved the branch head between our data
// commit landing and this commit) is retried with a reload and a fresh stage,
// up to cfg.MaxRetries attempts in total. Every maintenance commit asserts the
// position of every ref on the table (iceberg-go does so for expiry, and for
// the branch on any commit), so on a busy multi-writer table a single attempt
// would often lose the race, and those are the tables that most need expiry.
//
// Anything else ends the pass: a staging error, a definitive rejection, an
// ambiguous outcome, or the retries running out. The committer's table handle
// must be current afterwards, or the next data commit conflicts with the
// output's own landed-but-unreported commit (a nacked batch at max_retries:
// 1). The loop reloads after every conflict, so only an ambiguous outcome
// still needs one; a definitive rejection landed nothing and a staging error
// sent nothing. The failure is logged once and counted.
func (c *committer) commitMaintenance(ctx context.Context, op string, stage func(txn *table.Transaction) (bool, error)) {
	for attempt := 1; attempt <= c.cfg.MaxRetries; attempt++ {
		// Expiry removes history other engines may be reading, so a landed
		// expiry is logged with its count: an operator whose time travel
		// stopped working can find when and where. Counting clones the
		// snapshot list, so only expiry pays for it.
		before := -1
		if op == maintenanceOpExpireSnapshots {
			before = len(c.table.Metadata().Snapshots())
		}
		txn := c.table.NewTransaction()
		start := time.Now()
		staged, err := stage(txn)
		if took := time.Since(start); took > slowMaintenanceStage && attempt == 1 {
			c.logger.Warnf("Staging %s for %s took %s with %d snapshots in the table metadata; data commits wait for it once per %s. A shorter %s keeps the walk short.", op, c.tableName(), took.Round(time.Millisecond), len(c.table.Metadata().Snapshots()), c.cfg.MaintenanceInterval, ioFieldMaxSnapshotAge)
		}
		if err != nil {
			// A local staging error (metadata iceberg-go cannot walk, say a
			// ref left pointing at a snapshot another engine removed).
			c.noteMaintenanceFailure(ctx, op, fmt.Errorf("staging: %w", err), false)
			return
		}
		if !staged {
			return
		}
		// Pin main to the head this transaction read. iceberg-go's own
		// client-side retry (commit.retry.num-retries on the table, off by
		// default) would otherwise replay a 409 against the fresh head with
		// the updates computed from the stale one, so a remove-snapshots
		// list could name a snapshot another writer just pointed a ref at.
		// Pinned, the replay fails as a clean conflict and this loop
		// recomputes from a reload instead.
		if err := txn.AssertRefSnapshotID(table.MainBranch); err != nil {
			c.noteMaintenanceFailure(ctx, op, fmt.Errorf("pinning %s: %w", table.MainBranch, err), false)
			return
		}
		tbl, err := txn.Commit(ctx)
		if err == nil {
			// Only expiry can get here without a catalog call:
			// stageSnapshotExpiry requests expiry without knowing whether
			// anything is eligible, and iceberg-go then commits a
			// transaction with no updates locally and hands back the same
			// table.
			c.table = tbl
			if before >= 0 {
				if after := len(tbl.Metadata().Snapshots()); after < before {
					c.logger.Infof("Expired %d snapshots from %s, %d retained", before-after, c.tableName(), after)
				}
			}
			return
		}
		if !errors.Is(err, table.ErrCommitFailed) {
			c.noteMaintenanceFailure(ctx, op, err, !isDefinitiveCommitRejection(err))
			return
		}
		c.logger.Debugf("Maintenance commit (%s) attempt %d conflicted with another writer; reloading and retrying: %v", op, attempt, err)
		reloaded, reloadErr := c.reloadTable(ctx)
		if reloadErr != nil {
			c.noteMaintenanceFailure(ctx, op, fmt.Errorf("%w (reloading the table after the conflict also failed: %w)", err, reloadErr), true)
			return
		}
		c.table = reloaded
		if attempt == c.cfg.MaxRetries {
			c.noteMaintenanceFailure(ctx, op, fmt.Errorf("%d attempts conflicted with other writers, the last: %w", attempt, err), false)
		}
	}
}

// noteMaintenanceFailure records a failed maintenance pass: the counter, the
// reload when the commit may have landed (see commitMaintenance), and one
// warning that says what the table is missing until the next pass. Warnings
// are bounded by the pass interval, since a pass logs at most one.
//
// A context that ended mid-pass (shutdown, or a deadline on the caller's
// context on the row-delta and overwrite paths) says nothing about the
// catalog or the table: it is neither counted nor warned about. The slot
// stays booked like any other failed pass. iceberg-go's expiry walk takes no
// context and runs to completion however long it takes, so freeing the slot
// would let a commit on a short-deadline context repeat a slow walk on every
// commit. The reload still matters when the commit may have landed (a nacked
// batch at max_retries: 1 otherwise), so it runs on a short detached context.
func (c *committer) noteMaintenanceFailure(ctx context.Context, op string, err error, reload bool) {
	if ctx.Err() != nil {
		c.logger.Debugf("Table maintenance (%s) for %s stopped with the context: %v", op, c.tableName(), err)
		if reload {
			rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), maintenanceReloadTimeout)
			defer cancel()
			if fresh, reloadErr := c.reloadTable(rctx); reloadErr == nil {
				c.table = fresh
			} else {
				c.logger.Debugf("Failed to reload %s after a maintenance commit (%s) was interrupted; the next data commit may conflict once and retry: %v", c.tableName(), op, reloadErr)
			}
		}
		return
	}
	c.metrics.incrMaintenanceFailure(op)
	if reload {
		if fresh, reloadErr := c.reloadTable(ctx); reloadErr == nil {
			c.table = fresh
		} else {
			c.logger.Warnf("Failed to reload %s after a failed maintenance commit (%s); the next data commit may conflict once and retry: %v", c.tableName(), op, reloadErr)
		}
	}
	ageMs, _ := c.effectiveSnapshotAgeMs()
	c.logger.Warnf("Snapshot expiry failed for %s; retrying in %s, and snapshots older than %s accumulate in the table metadata meanwhile. If this catalog does not allow clients to expire snapshots, set %s.%s to 0s and expire them with another tool: %v", c.tableName(), c.cfg.MaintenanceInterval, time.Duration(ageMs)*time.Millisecond, ioFieldCommit, ioFieldMaxSnapshotAge, err)
}

// observeRetainedSnapshots publishes the table's current snapshot count. It is
// the early-warning signal for a table whose expiry keeps failing: the count
// climbs towards the catalog's metadata ceiling long before data commits
// start to fail. Metadata.Snapshots clones every snapshot, so this runs only
// at the end of a pass, never on the data path.
func (c *committer) observeRetainedSnapshots() {
	c.metrics.setRetainedSnapshots(c.tableName(), int64(len(c.table.Metadata().Snapshots())))
}

// tableName is the table's identifier as namespace.table, for logs and the
// gauge label.
func (c *committer) tableName() string { return strings.Join(c.table.Identifier(), ".") }

// stageSnapshotExpiry makes commit.max_snapshot_age take effect by staging
// Transaction.ExpireSnapshots on a maintenance transaction. A table that
// carries its own history.expire.max-snapshot-age-ms keeps that retention: no
// age is passed and iceberg-go reads the property (effectiveSnapshotAgeMs).
// Otherwise the configured age goes in as WithOlderThan. No table property is
// written either way. WithPostCommit(false) leaves the expired snapshots'
// files on storage for orphan-file maintenance: deleting them inline would
// race readers mid-scan on a streaming table, and this output must never
// delete a file a reader may hold. Like Iceberg's expire_snapshots procedure
// the expiry is table-wide; refs with their own retention properties keep
// them. iceberg-go asserts EVERY ref's position for the commit, not just the
// branch being written, so on a table where another writer moves branches or
// tags concurrently the expiry commit can 409; commitMaintenance retries it.
//
// iceberg-go only stages a remove-snapshots (or remove-snapshot-ref) update
// when something is actually eligible, and commits nothing otherwise, so the
// bool is true whenever expiry was requested without error: a request that
// turns out to have nothing to remove costs no catalog call.
func (c *committer) stageSnapshotExpiry(txn *table.Transaction) (bool, error) {
	opts := []table.ExpireSnapshotsOpt{table.WithPostCommit(false)}
	if _, fromTable := c.effectiveSnapshotAgeMs(); !fromTable {
		opts = append(opts, table.WithOlderThan(c.cfg.MaxSnapshotAge))
	}
	if err := txn.ExpireSnapshots(opts...); err != nil {
		return false, err
	}
	return true, nil
}

// warnTableAgeMismatch logs, once per table, that the table's own retention
// property differs from the configured age and wins. It runs on every pass
// with retention enabled, whether or not anything is eligible, so the
// operator learns their setting is ignored even on a table where nothing has
// aged out yet.
func (c *committer) warnTableAgeMismatch() {
	tableMs, fromTable := c.effectiveSnapshotAgeMs()
	if !fromTable || tableMs == c.cfg.MaxSnapshotAge.Milliseconds() {
		return
	}
	c.maint.tableAgeWarnOnce.Do(func() {
		c.logger.Warnf("Table property %s is %dms on %s and takes precedence over %s.%s (%s)", table.MaxSnapshotAgeMsKey, tableMs, c.tableName(), ioFieldCommit, ioFieldMaxSnapshotAge, c.cfg.MaxSnapshotAge)
	})
}

// effectiveSnapshotAgeMs is the snapshot age in force for this table: its
// own history.expire.max-snapshot-age-ms (or the legacy key) when it carries
// one, else the configured max_snapshot_age. The bool reports the former.
// A table-level property is an operator's explicit choice that other engines
// honour too, so it wins over the field, exactly as the table's
// commit.manifest-merge.enabled wins over manifest_merge_enabled.
func (c *committer) effectiveSnapshotAgeMs() (int64, bool) {
	if ms, ok := lookupRetention(c.table.Metadata().Properties(), table.MaxSnapshotAgeMsKey, "max-snapshot-age-ms", table.MaxSnapshotAgeMsDefault); ok {
		return ms, true
	}
	return c.cfg.MaxSnapshotAge.Milliseconds(), false
}

// lookupRetention reads a retention property exactly as iceberg-go's
// unexported getRetentionInt64 does: the standard key when present (an
// unparsable value yields the default), else the legacy unprefixed key that
// tables written by older Go versions carry, else the default. The bool
// reports whether either key was present.
func lookupRetention(props iceberg.Properties, key, legacyKey string, def int64) (int64, bool) {
	if _, ok := props[key]; ok {
		return props.GetInt64(key, def), true
	}
	if _, ok := props[legacyKey]; ok {
		return props.GetInt64(legacyKey, def), true
	}
	return def, false
}
