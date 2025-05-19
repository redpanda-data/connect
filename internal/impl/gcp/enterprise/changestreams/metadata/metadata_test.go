// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package metadata

import (
	"context"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"

	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/impl/gcp/enterprise/changestreams/changestreamstest"
)

func testStores(e changestreamstest.EmulatorHelper) (*Store, *Store) {
	const (
		googleSQLDatabaseName = "google_sql_db"
		postgresDatabaseName  = "postgres_db"
	)

	g := NewStore(StoreConfig{
		ProjectID:  changestreamstest.EmulatorProjectID,
		InstanceID: changestreamstest.EmulatorInstanceID,
		DatabaseID: googleSQLDatabaseName,
		TableNames: RandomTableNames(googleSQLDatabaseName),
		Dialect:    databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL,
	}, e.CreateTestDatabase(googleSQLDatabaseName))

	p := NewStore(StoreConfig{
		ProjectID:  changestreamstest.EmulatorProjectID,
		InstanceID: changestreamstest.EmulatorInstanceID,
		DatabaseID: postgresDatabaseName,
		TableNames: RandomTableNames(postgresDatabaseName),
		Dialect:    databasepb.DatabaseDialect_POSTGRESQL,
	}, e.CreateTestDatabaseWithDialect(postgresDatabaseName, databasepb.DatabaseDialect_POSTGRESQL))

	return g, p
}

func TestIntegrationStore(t *testing.T) {
	integration.CheckSkip(t)

	e := changestreamstest.MakeEmulatorHelper(t)
	g, p := testStores(e)
	tests := []struct {
		name string
		s    *Store
	}{
		{name: "GoogleSQL", s: g},
		{name: "Postgres", s: p},
	}

	t.Run("CreatePartitionMetadataTableWithDatabaseAdminClient", func(t *testing.T) {
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				if err := CreatePartitionMetadataTableWithDatabaseAdminClient(t.Context(), tc.s.conf, e.DatabaseAdminClient); err != nil {
					t.Fatal(err)
				}
			})
		}
	})

	t.Run("GetUnfinishedMinWatermarkEmpty", func(t *testing.T) {
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				if err := CreatePartitionMetadataTableWithDatabaseAdminClient(t.Context(), tc.s.conf, e.DatabaseAdminClient); err != nil {
					t.Fatal(err)
				}

				// Test with empty table
				got, err := tc.s.GetUnfinishedMinWatermark(t.Context())
				if err != nil {
					t.Fatal(err)
				}

				// Should return zero time when no data exists
				want := time.Time{}
				if !got.Equal(want) {
					t.Errorf("GetUnfinishedMinWatermark on empty data = %v, want = %v", got, want)
				}
			})
		}
	})

	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	pm := func(token string, start time.Time, state State) PartitionMetadata {
		return PartitionMetadata{
			PartitionToken: token,
			ParentTokens:   []string{},
			StartTimestamp: start,
			State:          state,
			Watermark:      start,
			CreatedAt:      start,
		}
	}

	t.Run("InsertTestData", func(t *testing.T) {
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				if err := tc.s.insert(t.Context(), []PartitionMetadata{
					pm("created1", ts, StateCreated),
					pm("created2", ts.Add(-2*time.Second), StateCreated),
					pm("scheduled", ts.Add(time.Second), StateScheduled),
					pm("running", ts.Add(2*time.Second), StateRunning),
					pm("finished", ts.Add(-time.Second), StateFinished),
				}); err != nil {
					t.Fatal(err)
				}
			})
		}
	})

	t.Run("GetPartition", func(t *testing.T) {
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				got, err := tc.s.GetPartition(t.Context(), "created1")
				if err != nil {
					t.Fatal(err)
				}
				want := pm("created1", ts, StateCreated)
				if diff := cmp.Diff(got, want); diff != "" {
					t.Errorf("GetPartition() mismatch (-want +got):\n%s", diff)
				}
			})
		}
	})

	t.Run("GetUnfinishedMinWatermark", func(t *testing.T) {
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				got, err := tc.s.GetUnfinishedMinWatermark(t.Context())
				if err != nil {
					t.Error(err)
					return
				}

				want := ts.Add(-2 * time.Second)
				if got != want {
					t.Errorf("GetUnfinishedMinWatermark = %v, want = %v", got, want)
				}
			})
		}
	})

	t.Run("GetPartitionsCreatedAfter", func(t *testing.T) {
		cutoff := ts.Add(-1 * time.Second)

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				got, err := tc.s.GetPartitionsCreatedAfter(t.Context(), cutoff)
				if err != nil {
					t.Fatal(err)
				}

				want := []PartitionMetadata{
					pm("created1", ts, StateCreated),
				}

				if diff := cmp.Diff(got, want); diff != "" {
					t.Errorf("GetPartitionsCreatedAfter() mismatch (-want +got):\n%s", diff)
				}
			})
		}
	})

	t.Run("GetInterruptedPartitions", func(t *testing.T) {
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				got, err := tc.s.GetInterruptedPartitions(t.Context())
				if err != nil {
					t.Fatalf("GetInterruptedPartitions failed: %v", err)
				}

				// Should return partitions in SCHEDULED or RUNNING state
				// ordered by creation time and start timestamp ascending
				want := []PartitionMetadata{
					pm("scheduled", ts.Add(time.Second), StateScheduled),
					pm("running", ts.Add(2*time.Second), StateRunning),
				}

				if diff := cmp.Diff(got, want); diff != "" {
					t.Errorf("GetInterruptedPartitions() mismatch (-want +got):\n%s", diff)
				}
			})
		}
	})

	t.Run("Create", func(t *testing.T) {
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				if err := tc.s.Create(t.Context(), []PartitionMetadata{
					pm("created3", ts, StateCreated),
				}); err != nil {
					t.Fatal(err)
				}
				if err := tc.s.Create(t.Context(), []PartitionMetadata{
					pm("created3", ts.Add(time.Second), StateCreated),
					pm("created4", ts.Add(time.Second), StateCreated),
				}); spanner.ErrCode(err) != codes.AlreadyExists {
					t.Fatal(err)
				}
			})
		}
	})

	t.Run("UpdateToScheduled", func(t *testing.T) {
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				partitionForToken := func(token string) PartitionMetadata {
					t.Helper()
					pm, err := tc.s.GetPartition(t.Context(), token)
					if err != nil {
						t.Fatal(err)
					}
					return pm
				}

				// Before UpdateToScheduled:
				pms := partitionForToken("scheduled")
				pmr := partitionForToken("running")

				commitTs, err := tc.s.UpdateToScheduled(t.Context(), []string{"created1", "scheduled", "running"})
				if err != nil {
					t.Fatal(err)
				}
				if commitTs.IsZero() {
					t.Error("Expected non-zero commit timestamp")
				}

				// created1
				{
					pm, err := tc.s.GetPartition(t.Context(), "created1")
					if err != nil {
						t.Fatalf("Failed to get partition created1: %v", err)
					}
					if pm.State != StateScheduled {
						t.Errorf("Expected partition created1 to be in SCHEDULED state, got %s", pm.State)
					}
					if pm.ScheduledAt == nil {
						t.Errorf("Expected partition created1 to have ScheduledAt timestamp")
					}
					if !commitTs.Equal(*pm.ScheduledAt) {
						t.Errorf("Expected commitTs (%v) to match ScheduledAt (%v)", commitTs, *pm.ScheduledAt)
					}
				}

				// scheduled
				{
					pm, err := tc.s.GetPartition(t.Context(), "scheduled")
					if err != nil {
						t.Fatalf("Failed to get partition scheduled: %v", err)
					}
					if diff := cmp.Diff(pm, pms); diff != "" {
						t.Errorf("UpdateToScheduled() mismatch (-want +got):\n%s", diff)
					}
				}

				// running
				{
					pm, err := tc.s.GetPartition(t.Context(), "running")
					if err != nil {
						t.Fatalf("Failed to get partition running: %v", err)
					}
					if diff := cmp.Diff(pm, pmr); diff != "" {
						t.Errorf("UpdateToScheduled() mismatch (-want +got):\n%s", diff)
					}
				}
			})
		}
	})

	t.Run("CheckParentPartitionsFinished", func(t *testing.T) {
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				subtests := []struct {
					name          string
					partitions    []string
					expectResult  bool
					errorContains string
				}{
					{
						name:         "all finished",
						partitions:   []string{"finished"},
						expectResult: true,
					},
					{
						name:         "mixed states",
						partitions:   []string{"finished", "running"},
						expectResult: false,
					},
					{
						name:         "empty list",
						partitions:   []string{},
						expectResult: true,
					},
					{
						name:         "non-existent",
						partitions:   []string{"nonexistent"},
						expectResult: false,
					},
				}

				for _, st := range subtests {
					t.Run(st.name, func(t *testing.T) {
						result, err := tc.s.CheckParentPartitionsFinished(t.Context(), st.partitions)
						if err != nil {
							t.Fatalf("CheckParentPartitionsFinished failed: %v", err)
						}
						if result != st.expectResult {
							t.Errorf("Expected result to be %v, got %v", st.expectResult, result)
						}
					})
				}
			})
		}
	})

	t.Run("UpdateWatermark", func(t *testing.T) {
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				want := ts.Add(5 * time.Minute)
				if err := tc.s.UpdateWatermark(t.Context(), "created1", want); err != nil {
					t.Fatalf("Failed to update watermark: %v", err)
				}

				got, err := tc.s.GetPartition(t.Context(), "created1")
				if err != nil {
					t.Fatalf("Failed to get partition after update: %v", err)
				}

				if !got.Watermark.Equal(want) {
					t.Errorf("Expected watermark to be %v, got %v", want, got.Watermark)
				}
			})
		}
	})

	t.Run("DeletePartitionMetadataTableWithDatabaseAdminClient", func(t *testing.T) {
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				if err := CreatePartitionMetadataTableWithDatabaseAdminClient(t.Context(), tc.s.conf, e.DatabaseAdminClient); err != nil {
					t.Fatal(err)
				}
			})
		}
	})
}

func realTestSore(r changestreamstest.RealHelper) *Store {
	return NewStore(StoreConfig{
		ProjectID:  r.ProjectID(),
		InstanceID: r.InstanceID(),
		DatabaseID: r.DatabaseID(),
		TableNames: RandomTableNames(r.DatabaseID()),
		Dialect:    databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL,
	}, r.Client())
}

func TestIntegrationRealStore(t *testing.T) {
	integration.CheckSkip(t)

	changestreamstest.CheckSkipReal(t)

	r := changestreamstest.MakeRealHelper(t)
	defer r.Close()
	s := realTestSore(r)

	if err := CreatePartitionMetadataTableWithDatabaseAdminClient(t.Context(), s.conf, r.DatabaseAdminClient()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := DeletePartitionMetadataTableWithDatabaseAdminClient(
			context.Background(), s.conf, r.DatabaseAdminClient()); err != nil { //nolint:usetesting // use context.Background
			t.Log(err)
		}
	}()

	t.Run("UpdateToScheduledInParallel", func(t *testing.T) {
		if err := s.Create(t.Context(), []PartitionMetadata{{
			PartitionToken: "created",
			ParentTokens:   []string{},
		}}); err != nil {
			t.Fatal(err)
		}

		// Run 10 workers in parallel, all trying to update the same partition
		const numWorkers = 10
		workerCommitTs := make([]time.Time, numWorkers)

		var wg sync.WaitGroup
		wg.Add(numWorkers)
		for i := 0; i < numWorkers; i++ {
			go func(workerID int) {
				defer wg.Done()

				// Each worker tries to update the same partition
				commitTs, err := s.UpdateToScheduled(t.Context(), []string{"created"})
				if err != nil {
					t.Errorf("Worker %d: %v", workerID, err)
					return
				}
				workerCommitTs[workerID] = commitTs
			}(i)
		}
		wg.Wait()

		// Verify that the partition is now in SCHEDULED state
		pm, err := s.GetPartition(t.Context(), "created")
		if err != nil {
			t.Fatal(err)
		}
		if pm.State != StateScheduled {
			t.Errorf("Expected partition to be in SCHEDULED state, got %s", pm.State)
		}
		if pm.ScheduledAt == nil {
			t.Error("Expected ScheduledAt to be set")
		}

		// Verify only one commit timestamp was set
		var matchCount int
		for i := 0; i < numWorkers; i++ {
			if workerCommitTs[i].Equal(*pm.ScheduledAt) {
				matchCount++
			}
		}
		if matchCount != 1 {
			t.Errorf("Expected only one commit timestamp to be set, got %d", matchCount)
		}
	})
}
