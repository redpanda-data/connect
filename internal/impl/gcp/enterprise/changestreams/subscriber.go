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
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/gcp/enterprise/changestreams/metadata"
)

// Config is the configuration for a Subscriber.
type Config struct {
	ProjectID            string
	InstanceID           string
	DatabaseID           string
	StreamID             string
	StartTimestamp       time.Time
	EndTimestamp         time.Time
	HeartbeatInterval    time.Duration
	MetadataTable        string
	MinWatermarkCacheTTL time.Duration
	AllowedModTypes      []string

	SpannerClientConfig       spanner.ClientConfig
	SpannerClientOptions      []option.ClientOption
	ChangeStreamQueryPriority spannerpb.RequestOptions_Priority
}

// Subscriber is a partition aware Spanner change stream consumer. It reads
// change records from the stream and passes them to the provided callback.
// It persists the state of the stream partitions to the metadata table in
// order to resume from the last record processed.
//
// The watermark is updated after each record callback. Callbacks for single
// partitions are executed sequentially. Callbacks for multiple partitions are
// executed in parallel.
//
// Subscriber supports both PostgreSQL and GoogleSQL dialects. It automatically
// detects the Spanner dialect and uses the appropriate dialect in the queries.
// It creates the metadata table if it does not exist. If MetadataTable is
// not set, it uses a random table name, this should be used in tests only.
type Subscriber struct {
	conf         Config
	client       *spanner.Client
	store        *metadata.Store
	minWatermark timeCache
	querier      querier
	resumed      map[string]struct{}
	eg           *errgroup.Group
	cb           CallbackFunc
	log          *service.Logger

	testingAdminClient  *adminapi.DatabaseAdminClient
	testingPostFinished func(partitionToken string, err error)
}

// NewSubscriber creates Spanner client and initializes the Subscriber.
func NewSubscriber(
	ctx context.Context,
	conf Config,
	cb CallbackFunc,
	log *service.Logger,
) (*Subscriber, error) {
	if cb == nil {
		return nil, errors.New("no callback provided")
	}

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
		log.Infof("Using random table names for metadata table, this should only be used for testing")
		tableNames = metadata.RandomTableNames(conf.DatabaseID)
	}

	sConf := metadata.StoreConfig{
		ProjectID:  conf.ProjectID,
		InstanceID: conf.InstanceID,
		DatabaseID: conf.DatabaseID,
		Dialect:    dialect,
		TableNames: tableNames,
	}

	if len(conf.AllowedModTypes) != 0 {
		cb = filteredCallback(cb, modTypeFilter(conf.AllowedModTypes))
	}

	store, err := metadata.NewStore(sConf, client)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("create metadata store: %w", err)
	}

	return &Subscriber{
		conf:   conf,
		client: client,
		store:  store,
		minWatermark: timeCache{
			d:   conf.MinWatermarkCacheTTL,
			now: now,
		},
		querier: clientQuerier{
			client:     client,
			dialect:    dialect,
			streamName: conf.StreamID,
			priority:   conf.ChangeStreamQueryPriority,
			log:        log,
		},
		cb:  cb,
		log: log,
	}, nil
}

// Setup creates the metadata table and detects the root partitions.
func (s *Subscriber) Setup(ctx context.Context) error {
	if err := s.createPartitionMetadataTableIfNotExist(ctx); err != nil {
		return fmt.Errorf("create partition metadata table: %w", err)
	}

	if err := s.detectRootPartitions(ctx); err != nil {
		return fmt.Errorf("detect root partitions: %w", err)
	}

	return nil
}

func (s *Subscriber) createPartitionMetadataTableIfNotExist(ctx context.Context) error {
	s.log.Debugf("Creating partition metadata table %s if not exist", s.store.Config().TableName)

	var adm *adminapi.DatabaseAdminClient
	if s.testingAdminClient != nil {
		adm = s.testingAdminClient
	} else {
		var err error
		adm, err = adminapi.NewDatabaseAdminClient(ctx, s.conf.SpannerClientOptions...)
		if err != nil {
			return err
		}
		defer func() {
			if err := adm.Close(); err != nil {
				s.log.Warnf("Failed to close database admin client: %v", err)
			}
		}()
	}
	return metadata.CreatePartitionMetadataTableWithDatabaseAdminClient(ctx, s.store.Config(), adm)
}

func (s *Subscriber) detectRootPartitions(ctx context.Context) error {
	pm := metadata.PartitionMetadata{
		PartitionToken:  "", // Empty token to query all partitions
		StartTimestamp:  s.conf.StartTimestamp,
		EndTimestamp:    s.conf.EndTimestamp,
		HeartbeatMillis: s.conf.HeartbeatInterval.Milliseconds(),
		Watermark:       s.conf.StartTimestamp,
	}

	if err := s.querier.query(ctx, pm, s.handleRootPartitions); err != nil {
		return fmt.Errorf("query for root partitions: %w", err)
	}

	return nil
}

func (s *Subscriber) handleRootPartitions(ctx context.Context, cr ChangeRecord) error {
	for _, cpr := range cr.ChildPartitionsRecords {
		for _, cp := range cpr.ChildPartitions {
			if len(cp.ParentPartitionTokens) != 0 {
				s.log.Debugf("Ignoring child partition with parent partition tokens: %+v", cp.ParentPartitionTokens)
				continue
			}

			rpm := cp.toPartitionMetadata(
				cpr.StartTimestamp,
				s.conf.EndTimestamp,
				s.conf.HeartbeatInterval.Milliseconds(),
			)
			if err := s.store.Create(ctx, []metadata.PartitionMetadata{rpm}); err != nil {
				if spanner.ErrCode(err) != codes.AlreadyExists {
					return fmt.Errorf("create root partition metadata: %w", err)
				}
			} else {
				s.log.Infof("Detected root partition %s", rpm.PartitionToken)
			}
		}
	}

	return nil
}

// Run starts reading the change stream and processing partitions. It can be
// stopped by canceling the context. If EndTimestamp is set, the subscriber will
// stop when it reaches the end timestamp. Setup can resume the subscriber
// from the last record processed.
//
// Error can be returned only if rescheduling interrupted partitions fails or
// if the context is canceled.
//
// Setup must be called before Run.
func (s *Subscriber) Run(ctx context.Context) error {
	s.log.Infof("Starting subscriber stream_id=%s start_timestamp=%v end_timestamp=%v",
		s.conf.StreamID,
		s.conf.StartTimestamp,
		s.conf.EndTimestamp)
	defer func() {
		s.log.Info("Subscriber stopped")
	}()

	s.eg, ctx = errgroup.WithContext(ctx)

	if pms, err := s.store.GetInterruptedPartitions(ctx); err != nil {
		return fmt.Errorf("get interrupted partitions: %w", err)
	} else if len(pms) > 0 {
		s.resumed = make(map[string]struct{}, len(pms))
		for _, pm := range pms {
			s.resumed[pm.PartitionToken] = struct{}{}
		}

		s.log.Debugf("Detected %d interrupted partitions", len(pms))
		if err := s.schedule(ctx, pms); err != nil {
			return fmt.Errorf("schedule interrupted partitions: %w", err)
		}
	}

	s.eg.Go(func() error {
		defer func() {
			s.log.Info("Waiting for all partitions to finish")
		}()
		return s.detectNewPartitionsLoop(ctx)
	})

	return s.eg.Wait()
}

func (s *Subscriber) detectNewPartitionsLoop(ctx context.Context) error {
	const resumeDuration = 100 * time.Millisecond
	t := time.NewTimer(0)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := s.detectNewPartitions(ctx); err != nil {
				if isCancelled(err) {
					return ctx.Err()
				}
				if errors.Is(err, errEndOfStream) {
					s.log.Infof("No new partitions detected, exiting")
					return nil
				}
				return fmt.Errorf("detect new partitions: %w", err)
			}
			t.Reset(resumeDuration)
		}
	}
}

var errEndOfStream = errors.New("no new partitions")

func (s *Subscriber) detectNewPartitions(ctx context.Context) error {
	minWatermark := s.minWatermark.get()
	if minWatermark.IsZero() {
		var err error
		minWatermark, err = s.store.GetUnfinishedMinWatermark(ctx)
		if err != nil {
			return fmt.Errorf("get unfinished min watermark: %w", err)
		}
		s.log.Debugf("Detected unfinished min watermark: %v", minWatermark)
	}
	if minWatermark.IsZero() {
		return nil
	}
	s.minWatermark.set(minWatermark)

	if !s.conf.EndTimestamp.IsZero() && minWatermark.After(s.conf.EndTimestamp) {
		s.log.Debugf("Min watermark is after end timestamp: %v", s.conf.EndTimestamp)
		return errEndOfStream
	}

	pms, err := s.store.GetPartitionsCreatedAfter(ctx, minWatermark)
	if err != nil {
		return err
	}
	if len(pms) == 0 {
		return nil
	}
	s.log.Debugf("Detected %d new partitions", len(pms))

	if err := s.schedule(ctx, pms); err != nil {
		return fmt.Errorf("schedule partitions: %w", err)
	}

	return nil
}

func (s *Subscriber) schedule(ctx context.Context, pms []metadata.PartitionMetadata) error {
	for _, g := range groupPartitionsByCreatedAt(pms) {
		if _, err := s.store.UpdateToScheduled(ctx, tokensOf(g)); err != nil {
			return fmt.Errorf("update partitions to scheduled: %w", err)
		}

		for _, pm := range g {
			s.eg.Go(func() error {
				s.waitForParentPartitionsToFinish(ctx, pm)

				err := s.queryChangeStream(ctx, pm.PartitionToken)
				if s.testingPostFinished != nil {
					s.testingPostFinished(pm.PartitionToken, err)
				}
				if err != nil {
					if isCancelled(err) {
						return ctx.Err()
					}
					return fmt.Errorf("%s: query change stream: %w", pm.PartitionToken, err)
				}

				return nil
			})
		}
	}

	return nil
}

func tokensOf(partitions []metadata.PartitionMetadata) []string {
	s := make([]string, len(partitions))
	for i, p := range partitions {
		s[i] = p.PartitionToken
	}
	return s
}

// groupPartitionsByCreatedAt groups partitions by their creation time.
// Partitions with different CreatedAt times will be placed in separate groups.
// It works only on partitions already sorted by CreatedAt in ascending order.
func groupPartitionsByCreatedAt(partitions []metadata.PartitionMetadata) [][]metadata.PartitionMetadata {
	if len(partitions) == 0 {
		return nil
	}

	groups := [][]metadata.PartitionMetadata{{partitions[0]}}
	cur := partitions[0].CreatedAt

	for _, p := range partitions[1:] {
		if !p.CreatedAt.Equal(cur) {
			groups = append(groups, []metadata.PartitionMetadata{p})
			cur = p.CreatedAt
		} else {
			lastIdx := len(groups) - 1
			groups[lastIdx] = append(groups[lastIdx], p)
		}
	}

	return groups
}

// waitForParentPartitionsToFinish ensures that all parent partitions have
// finished processing before processing a child partition.
//
// Due to the parent-child partition lineage, in order to process changes for a
// particular key in commit timestamp order, records returned from child
// partitions should be processed only after records from all parent partitions
// have been processed.
func (s *Subscriber) waitForParentPartitionsToFinish(ctx context.Context, pm metadata.PartitionMetadata) {
	for {
		ok, err := s.store.CheckPartitionsFinished(ctx, pm.ParentTokens)
		if err != nil {
			s.log.Errorf("%s: error while checking parent partitions: %v",
				pm.PartitionToken, err)
		}
		if ok {
			return
		}

		s.log.Debugf("%s: waiting for parent partitions to finish, next check in %s",
			pm.PartitionToken, s.conf.HeartbeatInterval)
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.conf.HeartbeatInterval):
		}
	}
}

func (s *Subscriber) queryChangeStream(ctx context.Context, partitionToken string) error {
	s.log.Debugf("%s: updating partition to running", partitionToken)
	ts, err := s.store.UpdateToRunning(ctx, partitionToken)
	if err != nil {
		return fmt.Errorf("update partition to running: %w", err)
	}

	pm, err := s.store.GetPartition(ctx, partitionToken)
	if err != nil {
		return err
	}
	if pm.State != metadata.StateRunning {
		return fmt.Errorf("partition is not running: %s", pm.State)
	}
	if _, resumed := s.resumed[partitionToken]; !resumed {
		if pm.RunningAt == nil || !ts.Equal(*pm.RunningAt) {
			return fmt.Errorf("partition is already running: %s", pm.RunningAt)
		}
	}

	h := s.partitionMetadataHandler(pm)

	s.log.Debugf("%s: querying partition change stream", partitionToken)
	if err := s.querier.query(ctx, pm, h.handleChangeRecord); err != nil {
		return fmt.Errorf("process partition change stream: %w", err)
	}
	if err := s.cb(ctx, partitionToken, nil); err != nil {
		return fmt.Errorf("end of partition: %w", err)
	}
	s.log.Debugf("%s: done querying partition change stream", partitionToken)

	s.log.Debugf("%s: updating partition to finished", partitionToken)
	if err := s.store.UpdateWatermark(ctx, partitionToken, h.watermark()); err != nil {
		return fmt.Errorf("update watermark: %w", err)
	}
	if _, err := s.store.UpdateToFinished(ctx, partitionToken); err != nil {
		return fmt.Errorf("update partition to finished: %w", err)
	}

	return nil
}

func (s *Subscriber) Close() {
	s.client.Close()
}

func isCancelled(err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		spanner.ErrCode(err) == codes.Canceled
}
