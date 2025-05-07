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

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/gcp/enterprise/changestreams/metadata"
)

type handler struct {
	pm    metadata.PartitionMetadata
	cb    func(context.Context, *DataChangeRecord) error
	store *metadata.Store
	log   *service.Logger
}

func (h *handler) handleChangeRecord(ctx context.Context, cr ChangeRecord) error {
	tr := timeRange{
		cur: h.pm.StartTimestamp,
		end: h.pm.EndTimestamp,
	}

	if err := h.handleDataChangeRecords(ctx, cr, &tr); err != nil {
		return err
	}
	lastUpdatedWatermark := tr.now()

	for _, hr := range cr.HeartbeatRecords {
		tr.tryClaim(hr.Timestamp)
	}
	if err := h.handleChildPartitionsRecords(ctx, cr, &tr); err != nil {
		return err
	}

	if tr.now().Compare(lastUpdatedWatermark) != 0 {
		if err := h.store.UpdateWatermark(ctx, h.pm.PartitionToken, tr.now()); err != nil {
			return fmt.Errorf("update watermark: %w", err)
		}
	}

	return nil
}

func (h *handler) handleDataChangeRecords(
	ctx context.Context,
	cr ChangeRecord,
	tr *timeRange,
) error {
	for _, dcr := range cr.DataChangeRecords {
		if !tr.tryClaim(dcr.CommitTimestamp) {
			h.log.Errorf("%s: failed to claim data change record timestamp: %v, current: %v",
				h.pm.PartitionToken, dcr.CommitTimestamp, tr.now())
			continue
		}

		h.log.Debugf("%s: data change record: table: %s, modification type: %s, commit timestamp: %v",
			h.pm.PartitionToken, dcr.TableName, dcr.ModType, dcr.CommitTimestamp)
		if err := h.cb(ctx, dcr); err != nil {
			return fmt.Errorf("data change record handler failed: %w", err)
		}
		if err := h.store.UpdateWatermark(ctx, h.pm.PartitionToken, tr.now()); err != nil {
			return fmt.Errorf("update watermark: %w", err)
		}
	}
	return nil
}

func (h *handler) handleChildPartitionsRecords(
	ctx context.Context,
	cr ChangeRecord,
	tr *timeRange,
) error {
	for _, cpr := range cr.ChildPartitionsRecords {
		if !tr.tryClaim(cpr.StartTimestamp) {
			h.log.Errorf("%s: failed to claim child partition record timestamp: %v, current: %v",
				h.pm.PartitionToken, cpr.StartTimestamp, tr.now())
			continue
		}

		var childPartitions []metadata.PartitionMetadata
		for _, cp := range cpr.ChildPartitions {
			if cp.isSplit() {
				h.log.Infof("Detected partition split for partition %s", cp.ParentPartitionTokens[0])
			}
			h.log.Debugf("%s: child partition: token: %s, parent partition tokens: %+v",
				h.pm.PartitionToken, cp.Token, cp.ParentPartitionTokens)
			childPartitions = append(childPartitions,
				cp.toPartitionMetadata(cpr.StartTimestamp, h.pm.EndTimestamp, h.pm.HeartbeatMillis))
		}
		if err := h.store.Create(ctx, childPartitions); err != nil {
			return fmt.Errorf("create partitions: %w", err)
		}
	}
	return nil
}
