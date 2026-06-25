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

	if err := c.commitLocked(ctx, func(txn *table.Transaction, props iceberg.Properties) error {
		// WithoutDuplicateCheck: writer.Write stamps each path with a fresh uuid,
		// so iceberg-go's default O(snapshot) manifest collision scan is wasted
		// work on the commit hot path (T6692).
		return txn.AddDataFiles(ctx, allFiles, props,
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

	if input.SchemaID != c.currentSchemaID() {
		return &StaleSchemaError{WriterSchemaID: input.SchemaID, CurrentSchemaID: c.currentSchemaID()}
	}
	if err := c.commitLocked(ctx, func(txn *table.Transaction, props iceberg.Properties) error {
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
// Callers must hold c.commitMu.
func (c *committer) commitLocked(ctx context.Context, stage func(*table.Transaction, iceberg.Properties) error) error {
	props := iceberg.Properties{
		table.ManifestMergeEnabledKey: strconv.FormatBool(c.cfg.ManifestMergeEnabled),
	}
	if c.cfg.MaxSnapshotAge > 0 {
		props[table.MaxSnapshotAgeMsKey] = strconv.FormatInt(c.cfg.MaxSnapshotAge.Milliseconds(), 10)
	}

	var commitErr error
	attempt := 0
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
		if err := stage(txn, props); err != nil {
			return err
		}
		tbl, err := txn.Commit(ctx)
		if errors.Is(err, rest.ErrCommitFailed) {
			commitErr = err
			c.logger.Warnf("Commit attempt %d/%d failed: %v", attempt, c.cfg.MaxRetries, err)
			if reloaded, reloadErr := c.reloadTable(ctx); reloadErr == nil {
				c.table = reloaded
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

func (c *committer) incrCommitFailure() {
	if c.metrics != nil {
		c.metrics.commitFailures.Incr(1)
	}
}

// currentSchemaID returns the table's current schema ID.
func (c *committer) currentSchemaID() int {
	return c.table.Schema().ID
}

// Close shuts down the committer and waits for pending commits.
func (c *committer) Close() {
	c.batcher.Close()
}
