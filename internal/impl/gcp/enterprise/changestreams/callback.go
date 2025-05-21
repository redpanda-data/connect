// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package changestreams

import (
	"context"
)

// CallbackFunc is a function that is called for each change record.
// If error is returned the processing will be stopped. Implementations should
// update the partition watermark by calling Subscriber.UpdatePartitionWatermark
// when data is processed.
//
// When partition ends, the callback will be called with a nil DataChangeRecord.
// If batch processing is enabled, the batch shall be flushed when the last
// record is received to avoid mixing records from different partitions in
// the same batch.
type CallbackFunc func(ctx context.Context, partitionToken string, dcr *DataChangeRecord) error

// UpdatePartitionWatermark updates the watermark for a partition. It's intended
// for use by Callback function to update progress.
func (s *Subscriber) UpdatePartitionWatermark(ctx context.Context, partitionToken string, dcr *DataChangeRecord) error {
	return s.store.UpdateWatermark(ctx, partitionToken, dcr.CommitTimestamp)
}
