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
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"google.golang.org/api/option"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	testPartitionToken = "partition0"
)

func testPartitionMetadata(token string) metadata.PartitionMetadata {
	return metadata.PartitionMetadata{
		PartitionToken: token,
		ParentTokens:   []string{},
		StartTimestamp: testStartTimestamp,
		Watermark:      testStartTimestamp,
	}
}

func testSubscriber(
	t *testing.T,
	e changestreamstest.EmulatorHelper,
	cb CallbackFunc,
	opts ...func(*Config),
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
	for _, o := range opts {
		o(&conf)
	}

	if cb == nil {
		cb = func(_ context.Context, _ string, _ *DataChangeRecord) error { return nil }
	}

	log := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})))

	s, err := NewSubscriber(t.Context(), conf, cb, log, NewMetrics(nil, conf.StreamID))
	require.NoError(t, err)

	mq := new(mockQuerier)
	s.querier = mq
	s.testingAdminClient = e.DatabaseAdminClient

	return s, s.store, mq
}

func testSubscriberSetup(
	t *testing.T,
	e changestreamstest.EmulatorHelper,
	cb CallbackFunc,
	opts ...func(*Config),
) (*Subscriber, *metadata.Store, *mockQuerier, chan string) {
	s, ms, mq := testSubscriber(t, e, cb, opts...)

	done := make(chan string)
	s.testingPostFinished = func(partitionToken string, err error) {
		if err == nil {
			done <- partitionToken
		}
	}

	// Call setup to create the metadata table
	mq.ExpectQueryWithRecords(rootPartitionMetadata.PartitionToken, ChangeRecord{})
	require.NoError(t, s.Setup(t.Context()))
	mq.AssertExpectations(t)

	return s, ms, mq, done
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
	require.NoError(t, s.Setup(t.Context()))

	// Then the root partition is created
	cpm0, err := s.store.GetPartition(t.Context(), childPartitionToken)
	require.NoError(t, err)
	assert.Equal(t, metadata.StateCreated, cpm0.State)

	// Given the root partition is scheduled
	_, err = ms.UpdateToScheduled(t.Context(), []string{childPartitionToken})
	require.NoError(t, err)

	// When Setup is called again
	require.NoError(t, s.Setup(t.Context()))

	// Then the root partition is not changed
	cpm1, err := s.store.GetPartition(t.Context(), childPartitionToken)
	require.NoError(t, err)
	assert.Equal(t, metadata.StateScheduled, cpm1.State)
}

func TestIntegrationSubscriberStartContextCanceled(t *testing.T) {
	integration.CheckSkip(t)

	e := changestreamstest.MakeEmulatorHelper(t)
	defer e.Close()

	s, ms, mq, _ := testSubscriberSetup(t, e, nil)
	defer s.Close()

	// Given a single partition
	require.NoError(t, ms.Create(t.Context(), []metadata.PartitionMetadata{
		testPartitionMetadata(testPartitionToken),
	}))

	// When the partition waits for context cancellation
	mq.ExpectQuery(testPartitionToken).Run(func(args mock.Arguments) {
		ctx := args.Get(0).(context.Context)
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for partition1 to be aborted")
		}
	}).Return(context.Canceled)

	// And context is cancelled
	ctx, cancel := context.WithCancel(t.Context())
	time.AfterFunc(100*time.Millisecond, cancel)

	// Then Run returns context.Canceled
	require.ErrorIs(t, s.Run(ctx), context.Canceled)

	mq.AssertExpectations(t)
}

func TestIntegrationSubscriberStartReturnsErrorOnPartitionError(t *testing.T) {
	integration.CheckSkip(t)

	e := changestreamstest.MakeEmulatorHelper(t)
	defer e.Close()

	s, ms, mq, _ := testSubscriberSetup(t, e, nil)
	defer s.Close()

	// Given two sibling partitions
	require.NoError(t, ms.Create(t.Context(), []metadata.PartitionMetadata{
		testPartitionMetadata("partition1"),
		testPartitionMetadata("partition2"),
	}))

	// When partition2 returns an error
	testErr := errors.New("test error from partition2")
	mq.ExpectQuery("partition2").Return(testErr)

	// Then partition1 is aborted
	mq.ExpectQuery("partition1").Run(func(args mock.Arguments) {
		ctx := args.Get(0).(context.Context)
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for partition1 to be aborted")
		}
	}).Return(context.Canceled)

	require.ErrorIs(t, s.Run(t.Context()), testErr)
	mq.AssertExpectations(t)
}

func TestIntegrationSubscriberStartReturnsErrorOnCallbackError(t *testing.T) {
	integration.CheckSkip(t)

	e := changestreamstest.MakeEmulatorHelper(t)
	defer e.Close()

	// When callback returns an error
	testErr := errors.New("test error from callback")
	s, ms, mq, _ := testSubscriberSetup(t, e, func(_ context.Context, _ string, _ *DataChangeRecord) error {
		return testErr
	})
	defer s.Close()

	// Given partition with data
	require.NoError(t, ms.Create(t.Context(), []metadata.PartitionMetadata{
		testPartitionMetadata(testPartitionToken),
	}))
	mq.ExpectQueryWithRecords(testPartitionToken, ChangeRecord{
		DataChangeRecords: []*DataChangeRecord{
			{
				RecordSequence:  "1",
				CommitTimestamp: testStartTimestamp,
				TableName:       "test-table",
				ModType:         "INSERT",
			},
		},
	})
	mq.expectCallbackError = true

	// Then Run returns the error
	require.ErrorIs(t, s.Run(t.Context()), testErr)
	mq.AssertExpectations(t)
}

func TestIntegrationSubscriberResume(t *testing.T) {
	integration.CheckSkip(t)

	e := changestreamstest.MakeEmulatorHelper(t)
	defer e.Close()

	dch := make(chan *DataChangeRecord)
	s, ms, mq, done := testSubscriberSetup(t, e, func(_ context.Context, _ string, dcr *DataChangeRecord) error {
		if dcr != nil {
			dch <- dcr
		}
		return nil
	})
	defer s.Close()

	// Create partition in SCHEDULED state
	err := ms.Create(t.Context(), []metadata.PartitionMetadata{testPartitionMetadata("scheduled")})
	require.NoError(t, err)
	_, err = ms.UpdateToScheduled(t.Context(), []string{"scheduled"})
	require.NoError(t, err)

	// Create partition in RUNNING state
	err = ms.Create(t.Context(), []metadata.PartitionMetadata{testPartitionMetadata("running")})
	require.NoError(t, err)
	_, err = ms.UpdateToScheduled(t.Context(), []string{"running"})
	require.NoError(t, err)
	_, err = ms.UpdateToRunning(t.Context(), "running")
	require.NoError(t, err)

	mq.ExpectQueryWithRecords("scheduled", ChangeRecord{
		DataChangeRecords: []*DataChangeRecord{
			{
				RecordSequence:  "1",
				CommitTimestamp: testStartTimestamp,
				TableName:       "test-table",
				ModType:         "INSERT",
			},
		},
	})
	mq.ExpectQueryWithRecords("running", ChangeRecord{
		DataChangeRecords: []*DataChangeRecord{
			{
				RecordSequence:  "2",
				CommitTimestamp: testStartTimestamp,
				TableName:       "test-table",
				ModType:         "UPDATE",
			},
		},
	})

	// When Run is called
	go func() {
		if err := s.Run(t.Context()); err != nil {
			t.Log(err)
		}
	}()

	// Then partitions in SCHEDULED and RUNNING states are queried
	collectN(t, 2, dch)
	mq.AssertExpectations(t)

	// When partitions are finished
	collectN(t, 2, done)

	// Then partitions are moved to FINISHED state
	pm, err := ms.GetPartition(t.Context(), "scheduled")
	require.NoError(t, err)
	assert.Equal(t, metadata.StateFinished, pm.State)

	pm, err = ms.GetPartition(t.Context(), "running")
	require.NoError(t, err)
	assert.Equal(t, metadata.StateFinished, pm.State)
}

func TestIntegrationSubscriberCallbackUpdatePartitionWatermark(t *testing.T) {
	integration.CheckSkip(t)

	e := changestreamstest.MakeEmulatorHelper(t)
	defer e.Close()

	var (
		cnt = 0
		s   *Subscriber
	)
	s, ms, mq, done := testSubscriberSetup(t, e, func(_ context.Context, partitionToken string, dcr *DataChangeRecord) error {
		cnt += 1
		switch cnt {
		case 1:
			// When message is added to batch
		case 2:
			// Then watermark is not updated
			pm, err := s.store.GetPartition(t.Context(), partitionToken)
			require.NoError(t, err)
			assert.Equal(t, metadata.StateRunning, pm.State)
			assert.Equal(t, testStartTimestamp, pm.Watermark)

			// When UpdatePartitionWatermark is called
			require.NoError(t, s.UpdatePartitionWatermark(t.Context(), partitionToken, dcr.CommitTimestamp))
		case 3:
			assert.Nil(t, dcr)

			// Then watermark is updated
			pm, err := s.store.GetPartition(t.Context(), partitionToken)
			require.NoError(t, err)
			assert.Equal(t, metadata.StateRunning, pm.State)
			assert.Equal(t, testStartTimestamp.Add(2*time.Second), pm.Watermark)
		default:
			t.Fatal("unexpected call")
		}

		return nil
	})
	defer s.Close()

	// Given partition with data change records
	pm := metadata.PartitionMetadata{
		PartitionToken: testPartitionToken,
		ParentTokens:   []string{},
		StartTimestamp: testStartTimestamp,
		Watermark:      testStartTimestamp,
	}
	require.NoError(t, ms.Create(t.Context(), []metadata.PartitionMetadata{pm}))

	mq.ExpectQueryWithRecords(testPartitionToken, ChangeRecord{
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

	// When Run is called
	go func() {
		if err := s.Run(t.Context()); err != nil {
			t.Log(err)
		}
	}()

	// And partition is processed
	collectN(t, 1, done)

	mq.AssertExpectations(t)
}

func TestIntegrationSubscriberAllowedModTypes(t *testing.T) {
	integration.CheckSkip(t)

	e := changestreamstest.MakeEmulatorHelper(t)
	defer e.Close()

	// Given subscriber with allowed mod types
	dch := make(chan *DataChangeRecord, 10) // Make sure we don't block
	s, ms, mq, done := testSubscriberSetup(t, e, func(_ context.Context, _ string, dcr *DataChangeRecord) error {
		if dcr != nil {
			dch <- dcr
		}
		return nil
	}, func(conf *Config) {
		conf.AllowedModTypes = []string{"INSERT"} // Only allow INSERT operations
	})
	defer s.Close()

	// Call setup to create the metadata table
	mq.ExpectQueryWithRecords(rootPartitionMetadata.PartitionToken, ChangeRecord{})
	require.NoError(t, s.Setup(t.Context()))
	mq.AssertExpectations(t)

	// Given partition with INSERT and UPDATE data change records
	pm := metadata.PartitionMetadata{
		PartitionToken: testPartitionToken,
		ParentTokens:   []string{},
		StartTimestamp: testStartTimestamp,
		Watermark:      testStartTimestamp,
	}
	require.NoError(t, ms.Create(t.Context(), []metadata.PartitionMetadata{pm}))

	mq.ExpectQueryWithRecords(testPartitionToken, ChangeRecord{
		DataChangeRecords: []*DataChangeRecord{
			{
				RecordSequence:  "1",
				CommitTimestamp: testStartTimestamp.Add(time.Second),
				TableName:       "test-table",
				ModType:         "INSERT", // This should be processed
			},
			{
				RecordSequence:  "2",
				CommitTimestamp: testStartTimestamp.Add(2 * time.Second),
				TableName:       "test-table",
				ModType:         "UPDATE", // This should be filtered out
			},
		},
	})

	// When Run is called
	go func() {
		if err := s.Run(t.Context()); err != nil {
			t.Log(err)
		}
	}()

	// And partition is processed
	collectN(t, 1, done)

	// Then only INSERT data change record is processed
	assert.Len(t, dch, 1)
	dcrs := collectN(t, 1, dch)
	assert.Equal(t, "INSERT", dcrs[0].ModType)

	mq.AssertExpectations(t)
}

func TestIntegrationSubscriberChildTokenProcessingOrder(t *testing.T) {
	integration.CheckSkip(t)

	e := changestreamstest.MakeEmulatorHelper(t)
	defer e.Close()

	// Given child partition tokens where 0->1,2,3 and 2,3->4
	const (
		childToken1 = "child_token_1"
		childToken2 = "child_token_2"
		childToken3 = "child_token_3"
		childToken4 = "child_token_4"
	)

	// And child token 3 blocks
	childToken3Done := make(chan struct{})
	s, ms, mq, done := testSubscriberSetup(t, e, func(_ context.Context, partitionToken string, _ *DataChangeRecord) error {
		if partitionToken == childToken3 {
			select {
			case <-childToken3Done:
			case <-time.After(time.Second):
				t.Errorf("timeout waiting for child token 3 to be processed")
			}
		}
		return nil
	})
	defer s.Close()

	ts := time.Date(2022, 5, 1, 9, 0, 0, 0, time.UTC)
	heartbeatMillis := int64(10000)

	require.NoError(t, ms.Create(t.Context(), []metadata.PartitionMetadata{{
		PartitionToken:  testPartitionToken,
		ParentTokens:    []string{},
		StartTimestamp:  ts,
		EndTimestamp:    time.Time{}, // No end timestamp
		HeartbeatMillis: heartbeatMillis,
		State:           metadata.StateCreated,
		Watermark:       ts,
	}}))
	mq.ExpectQueryWithRecords(testPartitionToken, ChangeRecord{
		ChildPartitionsRecords: []*ChildPartitionsRecord{
			{
				StartTimestamp: ts,
				RecordSequence: "1000012389",
				ChildPartitions: []*ChildPartition{
					{
						Token:                 childToken1,
						ParentPartitionTokens: []string{},
					},
					{
						Token:                 childToken2,
						ParentPartitionTokens: []string{},
					},
				},
			},
			{
				StartTimestamp: ts,
				RecordSequence: "1000012390",
				ChildPartitions: []*ChildPartition{
					{
						Token:                 childToken3,
						ParentPartitionTokens: []string{},
					},
				},
			},
		},
	})

	ts4 := time.Date(2022, 5, 1, 9, 30, 15, 0, time.UTC)
	mq.ExpectQueryWithRecords(childToken1, ChangeRecord{}).Run(func(args mock.Arguments) {
		// Verify query parameters
		pm := args.Get(1).(metadata.PartitionMetadata)
		assert.Equal(t, ts, pm.StartTimestamp)
		assert.True(t, pm.EndTimestamp.IsZero())
		assert.Equal(t, heartbeatMillis, pm.HeartbeatMillis)
	})
	mq.ExpectQueryWithRecords(childToken2, ChangeRecord{
		ChildPartitionsRecords: []*ChildPartitionsRecord{
			{
				StartTimestamp: ts4,
				RecordSequence: "1000012389",
				ChildPartitions: []*ChildPartition{
					{
						Token:                 childToken4,
						ParentPartitionTokens: []string{childToken2, childToken3},
					},
				},
			},
		},
	})
	mq.ExpectQueryWithRecords(childToken3, ChangeRecord{
		ChildPartitionsRecords: []*ChildPartitionsRecord{
			{
				StartTimestamp: ts4,
				RecordSequence: "1000012389",
				ChildPartitions: []*ChildPartition{
					{
						Token:                 childToken4,
						ParentPartitionTokens: []string{childToken2, childToken3},
					},
				},
			},
		},
	})

	// When Run is called
	go func() {
		if err := s.Run(t.Context()); err != nil {
			t.Log(err)
		}
	}()

	// Then child partitions are processed
	collectN(t, 3, done) // 0, 1, 2

	// When detect new partitions runs
	time.Sleep(500 * time.Millisecond)

	// Then child token 4 is NOT processed
	mq.AssertExpectations(t)

	// When child token 3 is finished
	mq.ExpectQueryWithRecords(childToken4, ChangeRecord{})
	close(childToken3Done)

	// Then child token 4 is processed
	collectN(t, 2, done)
	mq.AssertExpectations(t)
}

func collectN[T any](t *testing.T, n int, ch <-chan T) []T {
	t.Helper()

	var items []T
	for range n {
		select {
		case item := <-ch:
			items = append(items, item)
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for channel item")
		}
	}
	return items
}
