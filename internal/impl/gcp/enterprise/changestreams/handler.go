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
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/grpc/codes"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/gcp/enterprise/changestreams/metadata"
)

type handler struct {
	pm      metadata.PartitionMetadata
	tr      timeRange
	cb      CallbackFunc
	store   *metadata.Store
	log     *service.Logger
	metrics *Metrics
}

func (s *Subscriber) partitionMetadataHandler(pm metadata.PartitionMetadata) *handler {
	return &handler{
		pm: pm,
		cb: s.cb,
		tr: timeRange{
			cur: pm.StartTimestamp,
			end: pm.EndTimestamp,
		},
		store:   s.store,
		log:     s.log,
		metrics: s.metrics,
	}
}

func (h *handler) handleChangeRecord(ctx context.Context, cr ChangeRecord) error {
	if err := h.handleDataChangeRecords(ctx, cr); err != nil {
		return err
	}
	for _, hr := range cr.HeartbeatRecords {
		h.metrics.IncHeartbeatRecordCount()
		h.tr.tryClaim(hr.Timestamp)
	}
	if err := h.handleChildPartitionsRecords(ctx, cr); err != nil {
		return err
	}

	return nil
}

func (h *handler) handleDataChangeRecords(ctx context.Context, cr ChangeRecord) error {
	for _, dcr := range cr.DataChangeRecords {
		h.metrics.IncDataChangeRecordCount()
		if !h.tr.tryClaim(dcr.CommitTimestamp) {
			h.log.Errorf("%s: failed to claim data change record timestamp: %v, current: %v",
				h.pm.PartitionToken, dcr.CommitTimestamp, h.tr.now())
			continue
		}

		h.log.Tracef("%s: data change record: table: %s, modification type: %s, commit timestamp: %v",
			h.pm.PartitionToken, dcr.TableName, dcr.ModType, dcr.CommitTimestamp)

		if err := h.cb(ctx, h.pm.PartitionToken, dcr); err != nil {
			return fmt.Errorf("data change record handler failed: %w", err)
		}
		h.metrics.UpdateDataChangeRecordCommittedToEmitted(time.Since(dcr.CommitTimestamp))

		// Updating watermark is delegated to Callback.
	}
	return nil
}

func (h *handler) handleChildPartitionsRecords(ctx context.Context, cr ChangeRecord) error {
	for _, cpr := range cr.ChildPartitionsRecords {
		if !h.tr.tryClaim(cpr.StartTimestamp) {
			h.log.Errorf("%s: failed to claim child partition record timestamp: %v, current: %v",
				h.pm.PartitionToken, cpr.StartTimestamp, h.tr.now())
			continue
		}

		var childPartitions []metadata.PartitionMetadata
		for _, cp := range cpr.ChildPartitions {
			h.log.Debugf("%s: child partition: token: %s, parent partition tokens: %+v",
				h.pm.PartitionToken, cp.Token, cp.ParentPartitionTokens)
			childPartitions = append(childPartitions,
				cp.toPartitionMetadata(cpr.StartTimestamp, h.pm.EndTimestamp, h.pm.HeartbeatMillis))
		}

		if err := h.store.Create(ctx, childPartitions); err != nil {
			if spanner.ErrCode(err) != codes.AlreadyExists {
				return fmt.Errorf("create partitions: %w", err)
			}
		}
		h.metrics.IncPartitionRecordCreatedCount(len(childPartitions))

		for _, cp := range cpr.ChildPartitions {
			if cp.isSplit() {
				h.metrics.IncPartitionRecordSplitCount()
			} else {
				h.metrics.IncPartitionRecordMergeCount()
			}
		}
	}
	return nil
}

func (h *handler) watermark() time.Time {
	return h.tr.now()
}
