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
	"log/slog"
	"os"
	"testing"
	"time"

	"google.golang.org/api/option"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/impl/gcp/enterprise/changestreams/changestreamstest"
	"github.com/redpanda-data/connect/v4/internal/impl/gcp/enterprise/changestreams/metadata"
)

var (
	testStartTimestamp    = time.Now().UTC().Truncate(time.Microsecond)
	rootPartitionMetadata = metadata.PartitionMetadata{
		PartitionToken:  "", // Empty token to query all partitions
		StartTimestamp:  testStartTimestamp,
		EndTimestamp:    time.Time{},
		HeartbeatMillis: 1000,
		Watermark:       testStartTimestamp,
	}
)

func testSubscriber(
	t *testing.T,
	e changestreamstest.EmulatorHelper,
	cb CallbackFunc,
) (*Subscriber, *metadata.Store, *mockQuerier) {
	t.Helper()

	const databaseID = "test"
	e.CreateTestDatabase(databaseID)

	conf := Config{
		ProjectID:         changestreamstest.EmulatorProjectID,
		InstanceID:        changestreamstest.EmulatorInstanceID,
		DatabaseID:        databaseID,
		StreamID:          "test-stream",
		StartTimestamp:    testStartTimestamp,
		EndTimestamp:      time.Time{}, // No end timestamp
		HeartbeatInterval: time.Second,

		SpannerClientOptions: []option.ClientOption{
			option.WithGRPCConn(e.Conn()),
		},
	}

	if cb == nil {
		cb = func(_ context.Context, _ string, _ *DataChangeRecord) error { return nil }
	}

	log := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})))

	s, err := NewSubscriber(t.Context(), conf, cb, log)
	if err != nil {
		t.Fatal(err)
	}
	mq := new(mockQuerier)
	s.querier = mq
	s.testingAdminClient = e.DatabaseAdminClient

	return s, s.store, mq
}

func TestIntegrationSubscriberSetup(t *testing.T) {
	integration.CheckSkip(t)

	e := changestreamstest.MakeEmulatorHelper(t)
	defer e.Close()

	s, ms, mq := testSubscriber(t, e, nil)
	defer s.Close()

	const childPartitionToken = "child-partition-token"
	mq.ExpectQueryWithRecords(rootPartitionMetadata.PartitionToken, ChangeRecord{
		ChildPartitionsRecords: []*ChildPartitionsRecord{
			{
				StartTimestamp: testStartTimestamp,
				RecordSequence: "1",
				ChildPartitions: []*ChildPartition{
					{
						Token:                 childPartitionToken,
						ParentPartitionTokens: []string{}, // Empty for root partition
					},
				},
			},
		},
	}).Twice()
	defer mq.AssertExpectations(t)

	// When Setup is called
	if err := s.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Then the root partition is created
	cpm0, err := s.store.GetPartition(t.Context(), childPartitionToken)
	if err != nil {
		t.Fatal(err)
	}
	if cpm0.State != metadata.StateCreated {
		t.Fatalf("child partition is not running: %s", cpm0.State)
	}

	// Given the root partition is scheduled
	if ms.UpdateToScheduled(t.Context(), []string{childPartitionToken}); err != nil { //nolint:errcheck
		t.Fatal(err)
	}
	// When Setup is called again
	if err := s.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Then the root partition is not changed
	cpm1, err := s.store.GetPartition(t.Context(), childPartitionToken)
	if err != nil {
		t.Fatal(err)
	}
	if cpm1.State != metadata.StateScheduled {
		t.Fatalf("child partition is not scheduled: %s", cpm1.State)
	}
}

func TestIntegrationSubscriberResume(t *testing.T) {
	integration.CheckSkip(t)

	e := changestreamstest.MakeEmulatorHelper(t)
	defer e.Close()

	dch := make(chan *DataChangeRecord)
	s, ms, mq := testSubscriber(t, e, func(ctx context.Context, partitionToken string, dcr *DataChangeRecord) error {
		if dcr != nil {
			dch <- dcr
		}
		return nil
	})
	defer s.Close()

	fch := make(chan string)
	s.testingPostFinished = func(partitionToken string, err error) {
		if err == nil {
			fch <- partitionToken
		}
	}

	// Call setup to create the metadata table
	mq.ExpectQueryWithRecords(rootPartitionMetadata.PartitionToken, ChangeRecord{})
	if err := s.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	mq.AssertExpectations(t)

	pm := func(token string) metadata.PartitionMetadata {
		return metadata.PartitionMetadata{
			PartitionToken: token,
			ParentTokens:   []string{},
			StartTimestamp: testStartTimestamp,
			Watermark:      testStartTimestamp,
		}
	}

	// Create partition in SCHEDULED state
	if err := ms.Create(t.Context(), []metadata.PartitionMetadata{pm("scheduled")}); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.UpdateToScheduled(t.Context(), []string{"scheduled"}); err != nil {
		t.Fatal(err)
	}

	// Create partition in RUNNING state
	if err := ms.Create(t.Context(), []metadata.PartitionMetadata{pm("running")}); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.UpdateToScheduled(t.Context(), []string{"running"}); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.UpdateToRunning(t.Context(), "running"); err != nil {
		t.Fatal(err)
	}

	mq.ExpectQueryWithRecords("scheduled", ChangeRecord{
		DataChangeRecords: []*DataChangeRecord{
			{
				RecordSequence:  "1",
				CommitTimestamp: testStartTimestamp,
			},
		},
	})
	mq.ExpectQueryWithRecords("running", ChangeRecord{
		DataChangeRecords: []*DataChangeRecord{
			{
				RecordSequence:  "2",
				CommitTimestamp: testStartTimestamp,
			},
		},
	})

	// When Start is called
	go func() {
		if err := s.Start(t.Context()); err != nil {
			t.Log(err)
		}
	}()

	// Then partitions in SCHEDULED and RUNNING states are queried
	collectN(t, 2, dch)
	mq.AssertExpectations(t)

	// And partitions are moved to FINISHED state
	collectN(t, 2, fch)
	if pm, err := ms.GetPartition(t.Context(), "scheduled"); err != nil {
		t.Fatal(err)
	} else if pm.State != metadata.StateFinished {
		t.Fatalf("partition is not finished: %s", pm.State)
	}
	if pm, err := ms.GetPartition(t.Context(), "running"); err != nil {
		t.Fatal(err)
	} else if pm.State != metadata.StateFinished {
		t.Fatalf("partition is not finished: %s", pm.State)
	}
}

func TestIntegrationSubscriberCallbackUpdatePartitionWatermark(t *testing.T) {
	integration.CheckSkip(t)

	e := changestreamstest.MakeEmulatorHelper(t)
	defer e.Close()

	partitionToken := "test-partition"
	cnt := 0
	var s *Subscriber
	s, ms, mq := testSubscriber(t, e, func(ctx context.Context, partitionToken string, dcr *DataChangeRecord) error {
		cnt += 1
		switch cnt {
		case 1:
			// When message is added to batch
		case 2:
			// Then watermark is not updated
			pm, err := s.store.GetPartition(t.Context(), partitionToken)
			if err != nil {
				t.Fatal(err)
			}
			if pm.State != metadata.StateRunning {
				t.Errorf("partition is not running: %s", pm.State)
			}
			if pm.Watermark != testStartTimestamp {
				t.Errorf("watermark updated to %v after ErrAddedToBatch returned", pm.Watermark)
			}

			// When UpdatePartitionWatermark is called
			if err := s.UpdatePartitionWatermark(ctx, partitionToken, dcr); err != nil {
				t.Fatal(err)
			}
		case 3:
			if dcr != nil {
				t.Fatal("expected nil dcr")
			}

			// Then watermark is updated
			pm, err := s.store.GetPartition(t.Context(), partitionToken)
			if err != nil {
				t.Fatal(err)
			}
			if pm.State != metadata.StateRunning {
				t.Errorf("partition is not running: %s", pm.State)
			}
			if pm.Watermark != testStartTimestamp.Add(2*time.Second) {
				t.Errorf("watermark updated to %v after UpdatePartitionWatermark returned", pm.Watermark)
			}
		default:
			t.Fatal("unexpected call")
		}

		return nil
	})
	defer s.Close()

	fch := make(chan string)
	s.testingPostFinished = func(partitionToken string, err error) {
		if err == nil {
			fch <- partitionToken
		}
	}

	// Call setup to create the metadata table
	mq.ExpectQueryWithRecords(rootPartitionMetadata.PartitionToken, ChangeRecord{})
	if err := s.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	mq.AssertExpectations(t)

	// Given partition with data change records
	pm := metadata.PartitionMetadata{
		PartitionToken: partitionToken,
		ParentTokens:   []string{},
		StartTimestamp: testStartTimestamp,
		Watermark:      testStartTimestamp,
	}
	if err := ms.Create(t.Context(), []metadata.PartitionMetadata{pm}); err != nil {
		t.Fatal(err)
	}
	mq.ExpectQueryWithRecords(partitionToken, ChangeRecord{
		DataChangeRecords: []*DataChangeRecord{
			{
				RecordSequence:  "1",
				CommitTimestamp: testStartTimestamp.Add(time.Second),
				TableName:       "test-table",
				ModType:         "INSERT",
			},
			{
				RecordSequence:  "2",
				CommitTimestamp: testStartTimestamp.Add(2 * time.Second),
				TableName:       "test-table",
				ModType:         "UPDATE",
			},
		},
	})

	// When Start is called
	go func() {
		if err := s.Start(t.Context()); err != nil {
			t.Log(err)
		}
	}()

	// And partition is processed
	<-fch

	mq.AssertExpectations(t)
}

func collectN[T any](t *testing.T, n int, ch <-chan T) []T {
	t.Helper()

	var items []T
	for i := 0; i < n; i++ {
		select {
		case item := <-ch:
			items = append(items, item)
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for channel item")
		}
	}
	return items
}
