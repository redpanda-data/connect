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
	"slices"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/gcp/enterprise/changestreams/metadata"
)

type timeRange struct {
	cur time.Time
	end *time.Time
}

func (r *timeRange) tryClaim(t time.Time) bool {
	if t.Before(r.cur) {
		return false
	}
	if r.end != nil && r.end.Compare(t) <= 0 {
		return false
	}

	r.cur = t
	return true
}

func (r *timeRange) now() time.Time {
	return r.cur
}

type Subscriber struct {
	clock timeRange
	store *metadata.Store
	log   *service.Logger
}

func NewSubscriber(ctx context.Context, conf Config, log *service.Logger) (*Subscriber, error) {
	dbName := fmt.Sprintf("projects/%s/instances/%s/databases/%s", conf.ProjectID, conf.InstanceID, conf.DatabaseID)
	client, err := spanner.NewClientWithConfig(ctx, dbName, conf.SpannerClientConfig, conf.SpannerClientOptions...)
	if err != nil {
		return nil, err
	}

	dialect, err := detectDialect(ctx, client)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to detect dialect: %w", err)
	}

	var tableNames metadata.TableNames
	if conf.MetadataTable != "" {
		tableNames = metadata.TableNamesFromExistingTable(conf.DatabaseID, conf.MetadataTable)
	} else {
		tableNames = metadata.RandomTableNames(conf.DatabaseID)
	}

	sConf := metadata.StoreConfig{
		ProjectID:  conf.ProjectID,
		InstanceID: conf.InstanceID,
		DatabaseID: conf.DatabaseID,
		Dialect:    dialect,
		TableNames: tableNames,
	}

	store := metadata.NewStore(sConf, client)

	clock := timeRange{
		cur: conf.StartTimestamp,
		end: nil,
	}
	if !conf.EndTimestamp.IsZero() {
		end := conf.EndTimestamp
		clock.end = &end
	}

	return &Subscriber{
		clock: clock,
		store: store,
		log:   log,
	}, nil
}

func (s *Subscriber) DetectNewPartitions(ctx context.Context) error {
	minWatermark, err := s.store.GetUnfinishedMinWatermark(ctx)
	if err != nil {
		return fmt.Errorf("get unfinished min watermark: %w", err)
	}
	if s.clock.tryClaim(minWatermark) {
		return nil
	}

	pms, err := s.store.GetAllPartitionsCreatedAfter(ctx, s.clock.now())
	if err != nil {
		return err
	}
	if len(pms) == 0 {
		return nil
	}

	for _, g := range groupPartitionsByCreatedAt(pms) {
		s.log.Infof("group: %v", g)
	}
	return nil
}

// groupPartitionsByCreatedAt works on partitions sorted by creation time.
func groupPartitionsByCreatedAt(partitions []metadata.PartitionMetadata) [][]metadata.PartitionMetadata {
	groups := [][]metadata.PartitionMetadata{{partitions[0]}}

	cmp := func(g []metadata.PartitionMetadata, t time.Time) int {
		return g[0].CreatedAt.Compare(t)
	}

	for _, p := range partitions[1:] {
		idx, ok := slices.BinarySearchFunc(groups, p.CreatedAt, cmp)
		if ok {
			groups[idx] = append(groups[idx], p)
		} else {
			groups = append(groups, []metadata.PartitionMetadata{p})
		}
	}

	return groups
}

func tokensOf(partitions []metadata.PartitionMetadata) []string {
	s := make([]string, len(partitions))
	for i, p := range partitions {
		s[i] = p.PartitionToken
	}
	return s
}
