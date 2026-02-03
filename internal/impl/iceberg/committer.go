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
	"fmt"

	"github.com/apache/iceberg-go/table"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/asyncroutine"
)

// dataFile represents a written parquet file ready for commit.
type dataFile struct {
	path     string // full path (e.g., s3://bucket/ns/table/data/xxx.parquet)
	rowCount int64
	fileSize int64
}

// committer batches data file commits for a single table.
// Commits are serialized - only one commit at a time per committer.
type committer struct {
	table   *table.Table
	batcher *asyncroutine.Batcher[dataFile, struct{}]
	logger  *service.Logger
}

// NewCommitter creates a new committer for a specific table.
func NewCommitter(tbl *table.Table, logger *service.Logger) (*committer, error) {
	c := &committer{
		table:  tbl,
		logger: logger,
	}

	batcher, err := asyncroutine.NewBatcher(100, c.doCommit)
	if err != nil {
		return nil, fmt.Errorf("failed to create batcher: %w", err)
	}
	c.batcher = batcher

	return c, nil
}

// Commit submits a data file for commit and waits for the result.
func (c *committer) Commit(ctx context.Context, file dataFile) error {
	_, err := c.batcher.Submit(ctx, file)
	return err
}

// doCommit processes a batch of data files for this table.
func (c *committer) doCommit(ctx context.Context, files []dataFile) ([]struct{}, error) {
	if len(files) == 0 {
		return nil, nil
	}

	// Collect file paths and stats
	paths := make([]string, len(files))
	var totalRows int64
	for i, f := range files {
		paths[i] = f.path
		totalRows += f.rowCount
	}

	// Create transaction and add files
	txn := c.table.NewTransaction()
	if err := txn.AddFiles(ctx, paths, nil, true); err != nil {
		return nil, fmt.Errorf("failed to add files: %w", err)
	}

	// Commit the transaction
	if _, err := txn.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	c.logger.Debugf("Committed %d files (%d rows)", len(paths), totalRows)

	// All succeeded - return empty responses
	responses := make([]struct{}, len(files))
	return responses, nil
}

// Close shuts down the committer and waits for pending commits.
func (c *committer) Close() {
	if c.batcher != nil {
		c.batcher.Close()
	}
}
