// Copyright 2025 Redpanda Data, Inc.
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
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/asyncroutine"
)

// CommitInput holds data files and the schema ID they were written with.
type CommitInput struct {
	Files    []iceberg.DataFile
	SchemaID int
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
	table   *table.Table
	cfg     CommitConfig
	batcher *asyncroutine.Batcher[CommitInput, struct{}]
	logger  *service.Logger
}

// NewCommitter creates a new committer for a specific table.
func NewCommitter(tbl *table.Table, cfg CommitConfig, logger *service.Logger) (*committer, error) {
	c := &committer{
		table:  tbl,
		cfg:    cfg,
		logger: logger,
	}

	batcher, err := asyncroutine.NewBatcher(100, c.doCommit)
	if err != nil {
		return nil, fmt.Errorf("creating batcher: %w", err)
	}
	c.batcher = batcher

	return c, nil
}

// Commit submits data files for commit and waits for the result.
func (c *committer) Commit(ctx context.Context, input CommitInput) error {
	_, err := c.batcher.Submit(ctx, input)
	return err
}

// doCommit processes a batch of commit inputs for this table.
func (c *committer) doCommit(ctx context.Context, inputs []CommitInput) ([]struct{}, error) {
	// Validate schema IDs match the current table schema.
	currentSchemaID := c.currentSchemaID()
	for _, input := range inputs {
		if input.SchemaID != currentSchemaID {
			return nil, &StaleSchemaError{
				WriterSchemaID:  input.SchemaID,
				CurrentSchemaID: currentSchemaID,
			}
		}
	}

	var allFiles []iceberg.DataFile
	for _, input := range inputs {
		allFiles = append(allFiles, input.Files...)
	}

	var commitErr error
	attempt := 0
	for range c.cfg.MaxRetries {
		attempt++
		txn := c.table.NewTransaction()
		if err := txn.AddDataFiles(ctx, allFiles, iceberg.Properties{
			table.ManifestMergeEnabledKey: strconv.FormatBool(c.cfg.ManifestMergeEnabled),
			table.MaxSnapshotAgeMsKey:     strconv.FormatInt(c.cfg.MaxSnapshotAge.Milliseconds(), 10),
		}); err != nil {
			return nil, fmt.Errorf("adding files: %w", err)
		}
		tbl, err := txn.Commit(ctx)
		if errors.Is(err, rest.ErrCommitFailed) {
			commitErr = err
			c.logger.Warnf("Commit attempt %d/%d failed: %v", attempt, c.cfg.MaxRetries, err)
			continue
		}
		c.table = tbl
		commitErr = nil
		break
	}
	if commitErr != nil {
		return nil, fmt.Errorf("committing transaction after %d attempts: %w", attempt, commitErr)
	}
	c.logger.Debugf("Committed %d files", len(allFiles))
	responses := make([]struct{}, len(inputs))
	return responses, nil
}

// currentSchemaID returns the table's current schema ID.
func (c *committer) currentSchemaID() int {
	return c.table.Schema().ID
}

// Close shuts down the committer and waits for pending commits.
func (c *committer) Close() {
	c.batcher.Close()
}
