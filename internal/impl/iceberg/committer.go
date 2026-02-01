/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
package iceberg

import (
	"context"

	"github.com/redpanda-data/connect/v4/internal/asyncroutine"
	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/icebergx"
)

// committer is the final stage of the pipeline, which does the actual communication
// with the catalog to register a new snapshot with the given data files.
type committer struct {
	client *catalogClient // unowned

	batcher *asyncroutine.Batcher[commitRequest, commitResponse]
}

type (
	dataFile struct {
		remotePath      string
		rowCount        int
		sizeBytes       []byte
		tableSchemaID   int
		partitionSpecID int
		partitionKey    icebergx.PartitionKey
	}
	commitRequest  struct{}
	commitResponse struct{}
)

// newCommitter returns a new committer object
//
// NOTE: it is the callers responsibility to close the passed in client
func newCommitter(client *catalogClient) (*committer, error) {
	c := &committer{
		client: client,
	}
	if batcher, err := asyncroutine.NewBatcher(100, c.doCommit); err != nil {
		return nil, err
	} else {
		c.batcher = batcher
	}
	return c, nil
}

// Commit to the catalog the given data files
func (c *committer) Commit(ctx context.Context, req commitRequest) (commitResponse, error) {
	return c.batcher.Submit(ctx, req)
}

func (c *committer) doCommit(ctx context.Context, reqs []commitRequest) ([]commitResponse, error) {
	panic(nil)
}

// Close the committer and any pending commit requests
func (c *committer) Close() {
	c.batcher.Close()
}
