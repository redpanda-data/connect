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
	"strconv"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/asyncroutine"
)

// CurrentIcebergVersion is the version of iceberg we use when writing.
// TODO(iceberg): When iceberg-go supports v3, add a config knob on moving to v3.
// For now we assume everything works with at least v2
const CurrentIcebergVersion = 2

// CommitInput holds data files and the schema ID they were written with.
//
// Files are inserted (appended) data files. DeleteFiles are equality-delete
// files produced for upsert/delete operations (ContentType EntryContentEqDeletes
// with EqualityFieldIDs set). When DeleteFiles is empty the commit is a pure
// append and takes the optimised AddDataFiles fast path; otherwise the changes
// are applied atomically through a RowDelta.
type CommitInput struct {
	Files       []iceberg.DataFile
	DeleteFiles []iceberg.DataFile
	SchemaID    int
}

// CommitConfig holds configuration for the committer.
type CommitConfig struct {
	ManifestMergeEnabled bool
	MaxSnapshotAge       time.Duration
	MaxRetries           int
}

// StaleSchemaError is returned when data was written with a schema
// that no longer matches the table's current schema.
type StaleSchemaError struct {
	WriterSchemaID  int
	CurrentSchemaID int
}

func (e *StaleSchemaError) Error() string {
	return fmt.Sprintf("stale schema: data written with schema %d but table is at schema %d",
		e.WriterSchemaID, e.CurrentSchemaID)
}

// committer batches data file commits for a single table.
// Commits are serialized - only one commit at a time per committer.
type committer struct {
	table       *table.Table
	cfg         CommitConfig
	reloadTable func(ctx context.Context) (*table.Table, error)
	batcher     *asyncroutine.Batcher[CommitInput, struct{}]
	// commitMu serializes all commits and guards c.table. The batcher's
	// doCommit and the direct commitRowDelta path both take it.
	commitMu        sync.Mutex
	upgradeWarnOnce sync.Once
	metrics         *opMetrics
	logger          *service.Logger
}

// NewCommitter creates a new committer for a specific table.
func NewCommitter(tbl *table.Table, cfg CommitConfig, reloadTable func(ctx context.Context) (*table.Table, error), logger *service.Logger) (*committer, error) {
	c := &committer{
		table:       tbl,
		cfg:         cfg,
		reloadTable: reloadTable,
		logger:      logger,
	}

	batcher, err := asyncroutine.NewBatcher(100, c.doCommit)
	if err != nil {
		return nil, fmt.Errorf("creating batcher: %w", err)
	}
	c.batcher = batcher

	return c, nil
}

// Commit submits files for commit and waits for the result. Pure appends are
// batched for throughput. Commits that carry equality-delete files are applied
// as their own snapshot (never coalesced with other commits): merge-on-read
// equality deletes only remove rows from earlier snapshots, so merging two
// keyed batches into a single snapshot would leave same-key duplicates.
func (c *committer) Commit(ctx context.Context, input CommitInput) error {
	if len(input.DeleteFiles) > 0 {
		return c.commitRowDelta(ctx, input)
	}
	_, err := c.batcher.Submit(ctx, input)
	return err
}

// doCommit processes a batch of append-only commit inputs (the batcher path).
func (c *committer) doCommit(ctx context.Context, inputs []CommitInput) ([]struct{}, error) {
	c.commitMu.Lock()
	defer c.commitMu.Unlock()

	currentSchemaID := c.currentSchemaID()
	var allFiles []iceberg.DataFile
	for _, input := range inputs {
		if input.SchemaID != currentSchemaID {
			return nil, &StaleSchemaError{WriterSchemaID: input.SchemaID, CurrentSchemaID: currentSchemaID}
		}
		allFiles = append(allFiles, input.Files...)
	}

	if err := c.commitLocked(ctx, true, func(txn *table.Transaction, props iceberg.Properties, reloaded bool) error {
		files := allFiles
		if reloaded {
			// A prior attempt can land server-side yet report failure (a lost
			// or ambiguous response). The reload then already references those
			// files, so re-adding them with the duplicate check disabled would
			// register the same path in two snapshots and produce conflicting
			// sequence numbers for downstream readers. Drop any file the
			// reloaded table already has; if all are present the previous
			// attempt succeeded and AddDataFiles + Commit become a no-op.
			remaining, err := c.dropAlreadyCommitted(ctx, allFiles)
			if err != nil {
				return err
			}
			files = remaining
		}
		// WithoutDuplicateCheck: writer.Write stamps each path with a fresh uuid,
		// so iceberg-go's default O(snapshot) manifest collision scan is wasted
		// work on the commit hot path (T6692). On a reloaded retry we run the
		// targeted dropAlreadyCommitted check above instead.
		return txn.AddDataFiles(ctx, files, props,
			table.WithoutAutoNameMapping(),
			table.WithoutDuplicateCheck(),
		)
	}); err != nil {
		return nil, err
	}
	c.logger.Debugf("Committed %d data files", len(allFiles))
	return make([]struct{}, len(inputs)), nil
}

// commitRowDelta applies a single input's inserts and equality deletes as one
// atomic snapshot, outside the batcher so it is never coalesced with another
// keyed commit.
func (c *committer) commitRowDelta(ctx context.Context, input CommitInput) error {
	c.commitMu.Lock()
	defer c.commitMu.Unlock()

	currentSchemaID := c.currentSchemaID()
	if input.SchemaID != currentSchemaID {
		return &StaleSchemaError{WriterSchemaID: input.SchemaID, CurrentSchemaID: currentSchemaID}
	}
	// retryOnUnknownState is false here: this path does not yet dedupe against a
	// reloaded snapshot on retry, so retrying a possibly-landed commit could
	// duplicate it. RowDelta commits carry equality-delete files alongside
	// inserts, so idempotent replay must reconcile both; that is tracked
	// separately.
	if err := c.commitLocked(ctx, false, func(txn *table.Transaction, props iceberg.Properties, _ bool) error {
		// RowDelta derives the snapshot operation automatically
		// (append/delete/overwrite).
		rd := txn.NewRowDelta(props)
		if len(input.Files) > 0 {
			rd.AddRows(input.Files...)
		}
		rd.AddDeletes(input.DeleteFiles...)
		return rd.Commit(ctx)
	}); err != nil {
		return err
	}
	c.logger.Debugf("Committed row delta: %d data files, %d delete files", len(input.Files), len(input.DeleteFiles))
	return nil
}

// commitLocked stages a transaction via stage and commits it, retrying on
// concurrent-commit conflicts and reloading table metadata between attempts.
// The stage callback's reloaded argument is false on the first attempt and
// true once the table has been reloaded after a failed attempt, so callers can
// guard against re-adding files a lost-but-landed attempt already committed.
//
// retryOnUnknownState controls whether an ErrCommitStateUnknown result (the
// commit may have landed server-side, e.g. a 5xx/timeout response) is retried.
// It is only safe to set when stage is idempotent across a reload — i.e. it
// drops files the reloaded snapshot already references — otherwise a retry of a
// commit that actually landed would duplicate it. Callers must hold c.commitMu.
func (c *committer) commitLocked(ctx context.Context, retryOnUnknownState bool, stage func(txn *table.Transaction, props iceberg.Properties, reloaded bool) error) error {
	props := iceberg.Properties{
		table.ManifestMergeEnabledKey: strconv.FormatBool(c.cfg.ManifestMergeEnabled),
	}
	if c.cfg.MaxSnapshotAge > 0 {
		props[table.MaxSnapshotAgeMsKey] = strconv.FormatInt(c.cfg.MaxSnapshotAge.Milliseconds(), 10)
	}

	var commitErr error
	attempt := 0
	reloaded := false
	for range c.cfg.MaxRetries {
		attempt++
		txn := c.table.NewTransaction()
		if c.table.Metadata().Version() < CurrentIcebergVersion {
			c.upgradeWarnOnce.Do(func() {
				c.logger.Warnf("Upgrading iceberg table to format version %d to support row-level deletes; this change is irreversible", CurrentIcebergVersion)
			})
			if err := txn.UpgradeFormatVersion(CurrentIcebergVersion); err != nil {
				return fmt.Errorf("upgrading version: %w", err)
			}
		}
		if err := stage(txn, props, reloaded); err != nil {
			return err
		}
		tbl, err := txn.Commit(ctx)
		// ErrCommitFailed is a clean conflict (our commit did not land), so a
		// reload-and-retry re-adds our files exactly once. ErrCommitStateUnknown
		// means the commit may have landed; retrying is only safe when stage
		// dedupes against the reloaded snapshot (retryOnUnknownState), in which
		// case a landed attempt becomes a no-op and an unlanded one re-adds once.
		if errors.Is(err, rest.ErrCommitFailed) ||
			(retryOnUnknownState && errors.Is(err, rest.ErrCommitStateUnknown)) {
			commitErr = err
			c.logger.Warnf("Commit attempt %d/%d failed: %v", attempt, c.cfg.MaxRetries, err)
			if reloadedTbl, reloadErr := c.reloadTable(ctx); reloadErr == nil {
				c.table = reloadedTbl
				reloaded = true
			} else {
				c.logger.Warnf("Failed to reload table during commit retry: %v", reloadErr)
			}
			continue
		} else if err != nil {
			// Non-retryable error: reload so the next call uses fresh metadata.
			if reloaded, reloadErr := c.reloadTable(ctx); reloadErr == nil {
				c.table = reloaded
			}
			c.incrCommitFailure()
			return fmt.Errorf("committing transaction: %w", err)
		}
		c.table = tbl
		return nil
	}
	c.incrCommitFailure()
	return fmt.Errorf("committing transaction after %d attempts: %w", attempt, commitErr)
}

// dropAlreadyCommitted returns the subset of files whose paths are not already
// referenced by the current snapshot of c.table, which the caller must have just
// reloaded. It exists because commit retries re-add the same DataFile objects
// (identical paths) after a reload: if a prior attempt actually landed
// server-side but reported failure, the reloaded snapshot already contains those
// files, and re-adding them with AddDataFiles' duplicate check disabled would
// register the same path in two snapshots (the "conflicting sequence numbers"
// corruption). The scan is O(current snapshot) but only runs on the rare retry
// path, keeping the first-attempt hot path free of the duplicate scan.
func (c *committer) dropAlreadyCommitted(ctx context.Context, files []iceberg.DataFile) ([]iceberg.DataFile, error) {
	snap := c.table.CurrentSnapshot()
	if snap == nil {
		return files, nil
	}
	fs, err := c.table.FS(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolving table filesystem for duplicate check: %w", err)
	}
	manifests, err := snap.Manifests(fs)
	if err != nil {
		return nil, fmt.Errorf("loading manifests for duplicate check: %w", err)
	}

	// Match against our candidate paths only, so the lookup set stays O(files)
	// regardless of table size.
	want := make(map[string]struct{}, len(files))
	for _, f := range files {
		want[f.FilePath()] = struct{}{}
	}
	committed := make(map[string]struct{}, len(files))
	for _, m := range manifests {
		if m.ManifestContent() != iceberg.ManifestContentData {
			continue
		}
		for entry, err := range m.Entries(fs, true) {
			if err != nil {
				return nil, fmt.Errorf("reading manifest entries for duplicate check: %w", err)
			}
			path := entry.DataFile().FilePath()
			if _, ok := want[path]; ok {
				committed[path] = struct{}{}
			}
		}
		if len(committed) == len(want) {
			break // every candidate already present; no need to scan further.
		}
	}

	if len(committed) == 0 {
		return files, nil
	}
	remaining := make([]iceberg.DataFile, 0, len(files)-len(committed))
	for _, f := range files {
		if _, ok := committed[f.FilePath()]; ok {
			c.logger.Warnf("Skipping re-add of data file already committed by a prior attempt: %s", f.FilePath())
			continue
		}
		remaining = append(remaining, f)
	}
	return remaining, nil
}

func (c *committer) incrCommitFailure() {
	c.metrics.incrCommitFailure()
}

// currentSchemaID returns the table's current schema ID.
func (c *committer) currentSchemaID() int {
	return c.table.Schema().ID
}

// Close shuts down the committer and waits for pending commits.
func (c *committer) Close() {
	c.batcher.Close()
}
