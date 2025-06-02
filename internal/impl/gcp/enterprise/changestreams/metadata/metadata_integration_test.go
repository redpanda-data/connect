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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/impl/gcp/enterprise/changestreams/changestreamstest"
)

func testStores(t *testing.T, e changestreamstest.EmulatorHelper) (*Store, *Store) {
	const (
		googleSQLDatabaseName = "google_sql_db"
		postgresDatabaseName  = "postgres_db"
	)

	g, err := NewStore(StoreConfig{
		ProjectID:  changestreamstest.EmulatorProjectID,
		InstanceID: changestreamstest.EmulatorInstanceID,
		DatabaseID: googleSQLDatabaseName,
		TableNames: RandomTableNames(googleSQLDatabaseName),
		Dialect:    databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL,
	}, e.CreateTestDatabase(googleSQLDatabaseName))
	require.NoError(t, err)

	p, err := NewStore(StoreConfig{
		ProjectID:  changestreamstest.EmulatorProjectID,
		InstanceID: changestreamstest.EmulatorInstanceID,
		DatabaseID: postgresDatabaseName,
		TableNames: RandomTableNames(postgresDatabaseName),
		Dialect:    databasepb.DatabaseDialect_POSTGRESQL,
	}, e.CreateTestDatabaseWithDialect(postgresDatabaseName, databasepb.DatabaseDialect_POSTGRESQL))
	require.NoError(t, err)

	return g, p
}

func TestIntegrationStore(t *testing.T) {
	integration.CheckSkip(t)

	e := changestreamstest.MakeEmulatorHelper(t)
	g, p := testStores(t, e)
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
				require.NoError(t,
					CreatePartitionMetadataTableWithDatabaseAdminClient(t.Context(), tc.s.conf, e.DatabaseAdminClient))
			})
		}
	})

	t.Run("GetUnfinishedMinWatermarkEmpty", func(t *testing.T) {
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				require.NoError(t,
					CreatePartitionMetadataTableWithDatabaseAdminClient(t.Context(), tc.s.conf, e.DatabaseAdminClient))

				// Test with empty table
				got, err := tc.s.GetUnfinishedMinWatermark(t.Context())
				require.NoError(t, err)

				// Should return zero time when no data exists
				want := time.Time{}
				assert.Equal(t, want, got)
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
				require.NoError(t, tc.s.insert(t.Context(), []PartitionMetadata{
					pm("created1", ts, StateCreated),
					pm("created2", ts.Add(-2*time.Second), StateCreated),
					pm("scheduled", ts.Add(time.Second), StateScheduled),
					pm("running", ts.Add(2*time.Second), StateRunning),
					pm("finished", ts.Add(-time.Second), StateFinished),
				}))
			})
		}
	})

	t.Run("GetPartition", func(t *testing.T) {
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				got, err := tc.s.GetPartition(t.Context(), "created1")
				require.NoError(t, err)
				want := pm("created1", ts, StateCreated)
				assert.Equal(t, want, got)
			})
		}
	})

	t.Run("GetUnfinishedMinWatermark", func(t *testing.T) {
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				got, err := tc.s.GetUnfinishedMinWatermark(t.Context())
				require.NoError(t, err)

				want := ts.Add(-2 * time.Second)
				assert.Equal(t, want, got)
			})
		}
	})

	t.Run("GetPartitionsCreatedAfter", func(t *testing.T) {
		cutoff := ts.Add(-1 * time.Second)

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				got, err := tc.s.GetPartitionsCreatedAfter(t.Context(), cutoff)
				require.NoError(t, err)

				want := []PartitionMetadata{
					pm("created1", ts, StateCreated),
				}

				assert.Equal(t, want, got)
			})
		}
	})

	t.Run("GetInterruptedPartitions", func(t *testing.T) {
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				got, err := tc.s.GetInterruptedPartitions(t.Context())
				require.NoError(t, err)

				// Should return partitions in SCHEDULED or RUNNING state
				// ordered by creation time and start timestamp ascending
				want := []PartitionMetadata{
					pm("scheduled", ts.Add(time.Second), StateScheduled),
					pm("running", ts.Add(2*time.Second), StateRunning),
				}

				assert.Equal(t, want, got)
			})
		}
	})

	t.Run("Create", func(t *testing.T) {
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				err := tc.s.Create(t.Context(), []PartitionMetadata{
					pm("created3", ts, StateCreated),
				})
				require.NoError(t, err)

				err = tc.s.Create(t.Context(), []PartitionMetadata{
					pm("created3", ts.Add(time.Second), StateCreated),
					pm("created4", ts.Add(time.Second), StateCreated),
				})
				assert.Equal(t, codes.AlreadyExists, spanner.ErrCode(err))
			})
		}
	})

	t.Run("UpdateToScheduled", func(t *testing.T) {
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				partitionForToken := func(token string) PartitionMetadata {
					t.Helper()
					pm, err := tc.s.GetPartition(t.Context(), token)
					require.NoError(t, err)
					return pm
				}

				// Before UpdateToScheduled:
				pms := partitionForToken("scheduled")
				pmr := partitionForToken("running")

				commitTs, err := tc.s.UpdateToScheduled(t.Context(), []string{"created1", "scheduled", "running"})
				require.NoError(t, err)
				assert.False(t, commitTs.IsZero())

				// created1
				{
					pm, err := tc.s.GetPartition(t.Context(), "created1")
					require.NoError(t, err)
					assert.Equal(t, StateScheduled, pm.State)
					assert.NotNil(t, pm.ScheduledAt)
					assert.Equal(t, commitTs, *pm.ScheduledAt)
				}

				// scheduled
				{
					pm, err := tc.s.GetPartition(t.Context(), "scheduled")
					require.NoError(t, err)
					assert.Equal(t, pms, pm)
				}

				// running
				{
					pm, err := tc.s.GetPartition(t.Context(), "running")
					require.NoError(t, err)
					assert.Equal(t, pmr, pm)
				}
			})
		}
	})

	t.Run("CheckPartitionsFinished", func(t *testing.T) {
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
						result, err := tc.s.CheckPartitionsFinished(t.Context(), st.partitions)
						require.NoError(t, err)
						assert.Equal(t, st.expectResult, result)
					})
				}
			})
		}
	})

	t.Run("UpdateWatermark", func(t *testing.T) {
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				want := ts.Add(5 * time.Minute)
				err := tc.s.UpdateWatermark(t.Context(), "created1", want)
				require.NoError(t, err)

				got, err := tc.s.GetPartition(t.Context(), "created1")
				require.NoError(t, err)

				assert.Equal(t, want, got.Watermark)
			})
		}
	})

	t.Run("DeletePartitionMetadataTableWithDatabaseAdminClient", func(t *testing.T) {
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				require.NoError(t, CreatePartitionMetadataTableWithDatabaseAdminClient(t.Context(), tc.s.conf, e.DatabaseAdminClient))
			})
		}
	})
}

func realTestSore(t *testing.T, r changestreamstest.RealHelper) *Store {
	s, err := NewStore(StoreConfig{
		ProjectID:  r.ProjectID(),
		InstanceID: r.InstanceID(),
		DatabaseID: r.DatabaseID(),
		TableNames: RandomTableNames(r.DatabaseID()),
		Dialect:    databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL,
	}, r.Client())
	require.NoError(t, err)
	return s
}

func TestIntegrationRealStore(t *testing.T) {
	integration.CheckSkip(t)

	changestreamstest.CheckSkipReal(t)

	r := changestreamstest.MakeRealHelper(t)
	defer r.Close()
	s := realTestSore(t, r)

	require.NoError(t,
		CreatePartitionMetadataTableWithDatabaseAdminClient(t.Context(), s.conf, r.DatabaseAdminClient()))

	defer func() {
		if err := DeletePartitionMetadataTableWithDatabaseAdminClient(
			context.Background(), s.conf, r.DatabaseAdminClient()); err != nil { //nolint:usetesting // use context.Background
			t.Log(err)
		}
	}()

	t.Run("UpdateToScheduledInParallel", func(t *testing.T) {
		require.NoError(t, s.Create(t.Context(), []PartitionMetadata{{
			PartitionToken: "created",
			ParentTokens:   []string{},
		}}))

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
		require.NoError(t, err)
		assert.Equal(t, StateScheduled, pm.State)
		assert.NotNil(t, pm.ScheduledAt)

		// Verify only one commit timestamp was set
		var matchCount int
		for i := 0; i < numWorkers; i++ {
			if workerCommitTs[i].Equal(*pm.ScheduledAt) {
				matchCount++
			}
		}
		assert.Equal(t, 1, matchCount)
	})
}
