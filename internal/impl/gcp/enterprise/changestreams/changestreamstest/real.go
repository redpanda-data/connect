// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package changestreamstest

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
)

var (
	realSpannerProjectID  = flag.String("spanner.project_id", "", "GCP project ID for Spanner tests")
	realSpannerInstanceID = flag.String("spanner.instance_id", "", "Spanner instance ID for tests")
	realSpannerDatabaseID = flag.String("spanner.database_id", "", "Spanner database ID for tests")
)

// CheckSkipReal skips the test if the real Spanner environment is not configured.
// It checks if the required environment variables for real Spanner tests are set.
func CheckSkipReal(t *testing.T) {
	if *realSpannerProjectID == "" || *realSpannerInstanceID == "" || *realSpannerDatabaseID == "" {
		t.Skip("skipping real tests")
	}
}

func realSpannerFullDatabaseName() string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", *realSpannerProjectID, *realSpannerInstanceID, *realSpannerDatabaseID)
}

// MaybeDropOrphanedStreams finds all change streams with the pattern
// "rpcn_test_stream_%d" and deletes both the streams and their associated
// tables.
//
// Spanner has a limit of 10 streams per database. In some cases when tests fail
// the database may be left in a bad state. This function is used to clean up
// those bad states 10% of the time.
func MaybeDropOrphanedStreams(ctx context.Context) error {
	if rand.Intn(100) > 10 {
		return nil
	}
	return dropOrphanedStreams(ctx)
}

func dropOrphanedStreams(ctx context.Context) error {
	client, err := spanner.NewClient(ctx, realSpannerFullDatabaseName())
	if err != nil {
		return err
	}

	stmt := spanner.Statement{
		SQL: `SELECT change_stream_name FROM information_schema.change_streams WHERE change_stream_name LIKE 'rpcn_test_stream_%'`,
	}
	iter := client.Single().Query(ctx, stmt)
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
		Database:   realSpannerFullDatabaseName(),
		Statements: dropSQLs,
	})
	if err != nil {
		return fmt.Errorf("failed to execute drop statements: %w", err)
	}
	return op.Wait(ctx)
}

// RealHelper provides utilities for testing with a real Spanner instance.
// It manages the lifecycle of Spanner client and admin connections.
type RealHelper struct {
	t      *testing.T
	admin  *adminapi.DatabaseAdminClient
	client *spanner.Client
	table  string
	stream string
}

// MakeRealHelper creates a RealHelper for the real spanner test environment.
func MakeRealHelper(t *testing.T) RealHelper {
	client, err := spanner.NewClient(t.Context(), realSpannerFullDatabaseName())
	if err != nil {
		t.Fatal(err)
	}

	admin, err := adminapi.NewDatabaseAdminClient(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	ts := time.Now().UnixNano()
	return RealHelper{
		t:      t,
		admin:  admin,
		client: client,
		table:  fmt.Sprintf("rpcn_test_table_%d", ts),
		stream: fmt.Sprintf("rpcn_test_stream_%d", ts),
	}
}

// MakeRealHelperWithTableName creates a RealHelper with custom table and stream
// names for the real spanner test environment.
func MakeRealHelperWithTableName(t *testing.T, tableName, streamName string) RealHelper {
	h := MakeRealHelper(t)
	h.table = tableName
	h.stream = streamName
	return h
}

// ProjectID returns the project ID for the real Spanner instance.
func (RealHelper) ProjectID() string {
	return *realSpannerProjectID
}

// InstanceID returns the instance ID for the real Spanner instance.
func (RealHelper) InstanceID() string {
	return *realSpannerInstanceID
}

// DatabaseID returns the database ID for the real Spanner instance.
func (RealHelper) DatabaseID() string {
	return *realSpannerDatabaseID
}

// Table returns the table name generated for the test.
func (h RealHelper) Table() string {
	return h.table
}

// Stream returns the stream name generated for the test.
func (h RealHelper) Stream() string {
	return h.stream
}

// DatabaseAdminClient returns the database admin client.
func (h RealHelper) DatabaseAdminClient() *adminapi.DatabaseAdminClient {
	return h.admin
}

// Client returns the Spanner client.
func (h RealHelper) Client() *spanner.Client {
	return h.client
}

// CreateTableAndStream creates a table and a change stream for the current
// test. The table name and stream name are pre-generated and are available
// via Table() and Stream().
func (h RealHelper) CreateTableAndStream(sql string) {
	b := time.Now()
	h.t.Logf("Creating table %q and stream %q", h.table, h.stream)
	if err := h.createTableAndStream(sql); err != nil {
		h.t.Fatal(err)
	}
	h.t.Logf("Table %q and stream %q created in %s", h.table, h.stream, time.Since(b))

	h.t.Cleanup(func() {
		if err := h.dropTableAndStream(); err != nil {
			h.t.Logf("drop failed: %v", err)
		}
	})
}

func (h RealHelper) createTableAndStream(sql string) error {
	ctx := h.t.Context()

	op, err := h.admin.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database: realSpannerFullDatabaseName(),
		Statements: []string{
			fmt.Sprintf(sql, h.table),
			fmt.Sprintf(`CREATE CHANGE STREAM %s FOR %s`, h.stream, h.table),
		},
	})
	if err != nil {
		return fmt.Errorf("creating singers table: %w", err)
	}
	return op.Wait(ctx)
}

func (h RealHelper) dropTableAndStream() error {
	ctx := context.Background()
	op, err := h.admin.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database: realSpannerFullDatabaseName(),
		Statements: []string{
			fmt.Sprintf(`DROP CHANGE STREAM %s`, h.stream),
			fmt.Sprintf(`DROP TABLE %s`, h.table),
		},
	})
	if err != nil {
		return err
	}
	return op.Wait(ctx)
}

func (h RealHelper) Close() error {
	if err := h.admin.Close(); err != nil {
		return err
	}

	h.client.Close()

	return nil
}
