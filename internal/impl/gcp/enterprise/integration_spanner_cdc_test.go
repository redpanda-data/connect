// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package enterprise

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/impl/gcp/enterprise/changestreams"
	"github.com/redpanda-data/connect/v4/internal/license"
)

var (
	spannerProjectID  = flag.String("spanner.project_id", "", "GCP project ID for Spanner tests")
	spannerInstanceID = flag.String("spanner.instance_id", "", "Spanner instance ID for tests")
	spannerDatabaseID = flag.String("spanner.database_id", "", "Spanner database ID for tests")
)

func spannerDatabasePath() string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", *spannerProjectID, *spannerInstanceID, *spannerDatabaseID)
}

type spannerTestHelper struct {
	client   *spanner.Client
	tableID  string
	streamID string
}

func newSpannerTestHelper(ctx context.Context) (spannerTestHelper, error) {
	client, err := spanner.NewClient(ctx, spannerDatabasePath())
	if err != nil {
		return spannerTestHelper{}, err
	}

	ts := time.Now().UnixNano()
	return spannerTestHelper{
		client:   client,
		tableID:  fmt.Sprintf("rpcn_test_table_%d", ts),
		streamID: fmt.Sprintf("rpcn_test_stream_%d", ts),
	}, nil
}

// cleanupOrphanedTestStreams finds all change streams with the pattern
// "rpcn_test_stream_%d" and deletes both the streams and their associated tables.
func (h spannerTestHelper) cleanupOrphanedTestStreams(ctx context.Context) error {
	stmt := spanner.Statement{
		SQL: `SELECT change_stream_name FROM information_schema.change_streams WHERE change_stream_name LIKE 'rpcn_test_stream_%'`,
	}
	iter := h.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	// Collect all stream names
	streamNames := make([]string, 0)
	if err := iter.Do(func(row *spanner.Row) error {
		var sn string
		if err := row.Columns(&sn); err != nil {
			return err
		}
		streamNames = append(streamNames, sn)
		return nil
	}); err != nil {
		return err
	}

	if len(streamNames) == 0 {
		return nil
	}

	dropSQLs := make([]string, 0, len(streamNames)*2)
	for _, sn := range streamNames {
		dropSQLs = append(dropSQLs,
			fmt.Sprintf(`DROP CHANGE STREAM %s`, sn),
			fmt.Sprintf(`DROP TABLE %s`, strings.Replace(sn, "stream", "table", 1)))
	}
	adm, err := adminapi.NewDatabaseAdminClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create admin client: %w", err)
	}

	op, err := adm.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   spannerDatabasePath(),
		Statements: dropSQLs,
	})
	if err != nil {
		return fmt.Errorf("failed to execute drop statements: %w", err)
	}
	return op.Wait(ctx)
}

func (h spannerTestHelper) createTableAndStream(ctx context.Context) error {
	adm, err := adminapi.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}

	op, err := adm.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database: spannerDatabasePath(),
		Statements: []string{
			fmt.Sprintf(`CREATE TABLE %s (id INT64 NOT NULL, active BOOL NOT NULL ) PRIMARY KEY (id)`, h.tableID),
			fmt.Sprintf(`CREATE CHANGE STREAM %s FOR %s`, h.streamID, h.tableID),
		},
	})
	if err != nil {
		return err
	}
	return op.Wait(ctx)
}

func (h spannerTestHelper) dropTableAndStream(ctx context.Context) error {
	adm, err := adminapi.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}

	op, err := adm.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database: spannerDatabasePath(),
		Statements: []string{
			fmt.Sprintf(`DROP CHANGE STREAM %s`, h.streamID),
			fmt.Sprintf(`DROP TABLE %s`, h.tableID),
		},
	})
	if err != nil {
		return err
	}
	return op.Wait(ctx)
}

// Run tests with:
// go test -v -run TestIntegrationSpannerCDCInput . -spanner.project_id=sandbox-rpcn -spanner.instance_id=rpcn-tests -spanner.database_id=changestreams
func TestIntegrationSpannerCDCInput(t *testing.T) {
	integration.CheckSkip(t)

	if *spannerProjectID == "" || *spannerInstanceID == "" || *spannerDatabaseID == "" {
		t.Skip("Skipping Spanner integration test as required flags are not set")
	}

	h, err := newSpannerTestHelper(t.Context())
	if err != nil {
		t.Fatalf("failed to create test helper: %v", err)
	}
	if err := h.cleanupOrphanedTestStreams(t.Context()); err != nil {
		t.Fatalf("failed to cleanup orphaned test streams: %v", err)
	}
	if err := h.createTableAndStream(t.Context()); err != nil {
		t.Fatalf("failed to create table and stream: %v", err)
	}
	t.Logf("Created table %q and stream %q", h.tableID, h.streamID)

	t.Cleanup(func() {
		if err := h.dropTableAndStream(context.Background()); err != nil { //nolint:usetesting // the cleanup needs to run with fresh context
			t.Error(err)
		}
		t.Logf("Dropped table %q and stream %q", h.tableID, h.streamID)
	})

	// Configure the Spanner input using StreamBuilder
	inputConf := fmt.Sprintf(`
gcp_spanner_cdc:
  project_id: %s
  instance_id: %s
  database_id: %s
  stream_id: %s
  start_timestamp: %s
  heartbeat_interval: "5s"
`, *spannerProjectID, *spannerInstanceID, *spannerDatabaseID, h.streamID, time.Now().Format(time.RFC3339))

	// Create the stream builder and add the input
	sb := service.NewStreamBuilder()
	require.NoError(t, sb.AddInputYAML(inputConf))
	require.NoError(t, sb.SetLoggerYAML(`level: DEBUG`))

	const maxTestTime = 2 * time.Minute
	ctx, cancel := context.WithTimeout(t.Context(), maxTestTime)
	defer cancel()

	messages := make(chan *service.Message)

	// Add a consumer function to collect messages
	err := sb.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
		messages <- msg
		return nil
	})
	require.NoError(t, err)

	// Build the stream
	stream, err := sb.Build()
	require.NoError(t, err, "failed to build stream")
	license.InjectTestService(stream.Resources())

	t.Cleanup(func() {
		if err := stream.StopWithin(time.Second); err != nil {
			t.Log(err)
		}
	})

	go func() {
		if err := stream.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("stream error: %v", err)
		}
	}()

	_, err = h.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		if _, err := txn.Update(ctx, spanner.NewStatement(fmt.Sprintf("INSERT INTO %s (id, active) VALUES (1, true)", h.Table()))); err != nil {
			return err
		}
		if _, err := txn.Update(ctx, spanner.NewStatement(fmt.Sprintf("DELETE FROM %s WHERE id = 1", h.tableID))); err != nil {
			return err
		}
		return nil
	})
	require.NoError(t, err)

	expected := []spannerMod{
		{
			TableName: h.tableID,
			ColumnTypes: []*changestreams.ColumnType{
				{
					Name: "id",
					Type: spanner.NullJSON{
						Value: map[string]interface{}{"code": "INT64"},
						Valid: true,
					},
					IsPrimaryKey:    true,
					OrdinalPosition: 1,
				},
				{
					Name: "active",
					Type: spanner.NullJSON{
						Value: map[string]interface{}{"code": "BOOL"},
						Valid: true,
					},
					IsPrimaryKey:    false,
					OrdinalPosition: 2,
				},
			},
			Mod: &changestreams.Mod{
				Keys: spanner.NullJSON{
					Value: map[string]interface{}{"id": "1"},
					Valid: true,
				},
				NewValues: spanner.NullJSON{
					Value: map[string]interface{}{"active": true},
					Valid: true,
				},
				OldValues: spanner.NullJSON{
					Value: map[string]interface{}{},
					Valid: true,
				},
			},
			ModType: "INSERT",
		},
		{
			TableName: h.tableID,
			ColumnTypes: []*changestreams.ColumnType{
				{
					Name: "id",
					Type: spanner.NullJSON{
						Value: map[string]interface{}{"code": "INT64"},
						Valid: true,
					},
					IsPrimaryKey:    true,
					OrdinalPosition: 1,
				},
				{
					Name: "active",
					Type: spanner.NullJSON{
						Value: map[string]interface{}{"code": "BOOL"},
						Valid: true,
					},
					IsPrimaryKey:    false,
					OrdinalPosition: 2,
				},
			},
			Mod: &changestreams.Mod{
				Keys: spanner.NullJSON{
					Value: map[string]interface{}{"id": "1"},
					Valid: true,
				},
				NewValues: spanner.NullJSON{
					Value: map[string]interface{}{},
					Valid: true,
				},
				OldValues: spanner.NullJSON{
					Value: map[string]interface{}{"active": true},
					Valid: true,
				},
			},
			ModType: "DELETE",
		},
	}

	var got []spannerMod
	for msg := range messages {
		b, err := msg.AsBytes()
		require.NoError(t, err)

		var mod spannerMod
		require.NoError(t, json.Unmarshal(b, &mod))
		got = append(got, mod)
		if len(got) == len(expected) {
			break
		}
	}

	opt := cmpopts.IgnoreFields(changestreams.DataChangeRecord{}, "CommitTimestamp", "ServerTransactionID")
	if diff := cmp.Diff(got, expected, opt); diff != "" {
		t.Errorf("diff = %v", diff)
	}
}
