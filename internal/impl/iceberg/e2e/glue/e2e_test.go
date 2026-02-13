// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package glue

import (
	"context"
	"flag"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	icebergimpl "github.com/redpanda-data/connect/v4/internal/impl/iceberg"
	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/catalogx"
)

var (
	glueRegion          = flag.String("glue.region", "", "AWS region")
	glueBucket          = flag.String("glue.bucket", "", "S3 warehouse bucket")
	glueDatabase        = flag.String("glue.database", "", "Glue database name")
	glueWarehouse       = flag.String("glue.warehouse", "", "Glue catalog warehouse (AWS account ID)")
	athenaWorkgroup     = flag.String("glue.athena-workgroup", "", "Athena workgroup")
	athenaResultsBucket = flag.String("glue.athena-results-bucket", "", "Athena results bucket")
)

func skipIfNotConfigured(t *testing.T) {
	t.Helper()
	if *glueRegion == "" || *glueBucket == "" || *glueDatabase == "" || *glueWarehouse == "" {
		t.Skip("set -glue.region, -glue.bucket, -glue.database, -glue.warehouse flags to run Glue e2e tests")
	}
	if *athenaWorkgroup == "" || *athenaResultsBucket == "" {
		t.Skip("set -glue.athena-workgroup and -glue.athena-results-bucket flags for Athena verification")
	}
}

func catalogConfig() catalogx.Config {
	return catalogx.Config{
		URL:          fmt.Sprintf("https://glue.%s.amazonaws.com/iceberg", *glueRegion),
		Warehouse:    *glueWarehouse,
		AuthType:     "sigv4",
		SigV4Region:  *glueRegion,
		SigV4Service: "glue",
	}
}

func newRouter(t *testing.T, namespace, table string, schemaEvo bool) *icebergimpl.Router {
	t.Helper()
	namespaceStr, err := service.NewInterpolatedString(namespace)
	require.NoError(t, err)
	tableStr, err := service.NewInterpolatedString(table)
	require.NoError(t, err)

	logger := service.MockResources().Logger()
	commitCfg := icebergimpl.CommitConfig{
		ManifestMergeEnabled: true,
		MaxSnapshotAge:       24 * time.Hour,
		MaxRetries:           3,
	}
	schemaEvoCfg := icebergimpl.SchemaEvolutionConfig{
		Enabled:       schemaEvo,
		TableLocation: fmt.Sprintf("s3://%s/", *glueBucket),
	}
	router := icebergimpl.NewRouter(catalogConfig(), namespaceStr, tableStr, schemaEvoCfg, commitCfg, logger)
	t.Cleanup(func() { router.Close() })
	return router
}

func produce(t *testing.T, ctx context.Context, router *icebergimpl.Router, jsonMsgs ...string) {
	t.Helper()
	batch := make(service.MessageBatch, len(jsonMsgs))
	for i, j := range jsonMsgs {
		batch[i] = service.NewMessage([]byte(j))
	}
	require.NoError(t, router.Route(ctx, batch))
	time.Sleep(2 * time.Second)
}

func athenaQuery(t *testing.T, ctx context.Context, sql string) []map[string]string {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(*glueRegion))
	require.NoError(t, err)

	client := athena.NewFromConfig(cfg)

	startResult, err := client.StartQueryExecution(ctx, &athena.StartQueryExecutionInput{
		QueryString: aws.String(sql),
		WorkGroup:   aws.String(*athenaWorkgroup),
		QueryExecutionContext: &athenatypes.QueryExecutionContext{
			Database: aws.String(*glueDatabase),
		},
		ResultConfiguration: &athenatypes.ResultConfiguration{
			OutputLocation: aws.String(fmt.Sprintf("s3://%s/results/", *athenaResultsBucket)),
		},
	})
	require.NoError(t, err)

	queryID := startResult.QueryExecutionId

	for {
		status, err := client.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{
			QueryExecutionId: queryID,
		})
		require.NoError(t, err)

		state := status.QueryExecution.Status.State
		switch state {
		case athenatypes.QueryExecutionStateSucceeded:
		case athenatypes.QueryExecutionStateFailed, athenatypes.QueryExecutionStateCancelled:
			reason := ""
			if status.QueryExecution.Status.StateChangeReason != nil {
				reason = *status.QueryExecution.Status.StateChangeReason
			}
			t.Fatalf("Athena query %s: %s", state, reason)
		default:
			time.Sleep(time.Second)
			continue
		}
		break
	}

	results, err := client.GetQueryResults(ctx, &athena.GetQueryResultsInput{
		QueryExecutionId: queryID,
	})
	require.NoError(t, err)

	if results.ResultSet == nil || len(results.ResultSet.Rows) < 2 {
		return nil
	}

	headers := make([]string, len(results.ResultSet.Rows[0].Data))
	for i, d := range results.ResultSet.Rows[0].Data {
		if d.VarCharValue != nil {
			headers[i] = *d.VarCharValue
		}
	}

	var rows []map[string]string
	for _, row := range results.ResultSet.Rows[1:] {
		m := make(map[string]string, len(headers))
		for i, d := range row.Data {
			if i < len(headers) && d.VarCharValue != nil {
				m[headers[i]] = *d.VarCharValue
			}
		}
		rows = append(rows, m)
	}
	return rows
}

func glueCleanup(t *testing.T, tableName string) {
	t.Helper()
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(*glueRegion))
	require.NoError(t, err)

	glueClient := glue.NewFromConfig(cfg)
	_, err = glueClient.DeleteTable(ctx, &glue.DeleteTableInput{
		DatabaseName: aws.String(*glueDatabase),
		Name:         aws.String(tableName),
	})
	if err != nil {
		t.Logf("warning: failed to delete Glue table %s: %v", tableName, err)
	}

	s3Client := s3.NewFromConfig(cfg)
	prefix := *glueDatabase + "/" + tableName + "/"

	paginator := s3.NewListObjectsV2Paginator(s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(*glueBucket),
		Prefix: aws.String(prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			t.Logf("warning: failed to list S3 objects: %v", err)
			return
		}
		if len(page.Contents) == 0 {
			continue
		}
		objects := make([]s3types.ObjectIdentifier, len(page.Contents))
		for i, obj := range page.Contents {
			objects[i] = s3types.ObjectIdentifier{Key: obj.Key}
		}
		_, err = s3Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(*glueBucket),
			Delete: &s3types.Delete{Objects: objects, Quiet: aws.Bool(true)},
		})
		if err != nil {
			t.Logf("warning: failed to delete S3 objects: %v", err)
		}
	}
}

func TestGlueE2E_BasicWrite(t *testing.T) {
	skipIfNotConfigured(t)

	ctx := context.Background()
	tableName := fmt.Sprintf("e2e_basic_%d", time.Now().UnixNano())
	t.Cleanup(func() { glueCleanup(t, tableName) })

	router := newRouter(t, *glueDatabase, tableName, true)
	produce(t, ctx, router,
		`{"id": 1, "name": "alice", "event_type": "click", "value": 10}`,
		`{"id": 2, "name": "bob", "event_type": "view", "value": 20}`,
		`{"id": 3, "name": "charlie", "event_type": "purchase", "value": 30}`,
		`{"id": 4, "name": "alice", "event_type": "view", "value": 40}`,
		`{"id": 5, "name": "bob", "event_type": "click", "value": 50}`,
		`{"id": 6, "name": "charlie", "event_type": "purchase", "value": 60}`,
		`{"id": 7, "name": "alice", "event_type": "purchase", "value": 70}`,
		`{"id": 8, "name": "bob", "event_type": "view", "value": 80}`,
		`{"id": 9, "name": "charlie", "event_type": "click", "value": 90}`,
		`{"id": 10, "name": "alice", "event_type": "view", "value": 100}`,
	)

	rows := athenaQuery(t, ctx, fmt.Sprintf(`SELECT COUNT(*) AS cnt FROM "%s"`, tableName))
	require.Len(t, rows, 1)
	assert.Equal(t, "10", rows[0]["cnt"])

	// Use information_schema to verify columns (DESCRIBE not supported for Iceberg tables)
	desc := athenaQuery(t, ctx, fmt.Sprintf(
		`SELECT column_name FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'`,
		*glueDatabase, tableName))
	colNames := make([]string, len(desc))
	for i, row := range desc {
		colNames[i] = row["column_name"]
	}
	assert.Contains(t, colNames, "id")
	assert.Contains(t, colNames, "name")
	assert.Contains(t, colNames, "event_type")
	assert.Contains(t, colNames, "value")
}

func TestGlueE2E_SchemaEvolution(t *testing.T) {
	skipIfNotConfigured(t)

	ctx := context.Background()
	tableName := fmt.Sprintf("e2e_schema_evo_%d", time.Now().UnixNano())
	t.Cleanup(func() { glueCleanup(t, tableName) })

	router := newRouter(t, *glueDatabase, tableName, true)

	produce(t, ctx, router,
		`{"id": 1, "name": "alice"}`,
		`{"id": 2, "name": "bob"}`,
		`{"id": 3, "name": "charlie"}`,
		`{"id": 4, "name": "dave"}`,
		`{"id": 5, "name": "eve"}`,
	)

	produce(t, ctx, router,
		`{"id": 6, "name": "frank", "email": "frank@example.com"}`,
		`{"id": 7, "name": "grace", "email": "grace@example.com"}`,
		`{"id": 8, "name": "henry", "email": "henry@example.com"}`,
		`{"id": 9, "name": "iris", "email": "iris@example.com"}`,
		`{"id": 10, "name": "jack", "email": "jack@example.com"}`,
	)

	rows := athenaQuery(t, ctx, fmt.Sprintf(`SELECT COUNT(*) AS cnt FROM "%s"`, tableName))
	require.Len(t, rows, 1)
	assert.Equal(t, "10", rows[0]["cnt"])

	// Use information_schema to verify columns (DESCRIBE not supported for Iceberg tables)
	desc := athenaQuery(t, ctx, fmt.Sprintf(
		`SELECT column_name FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'`,
		*glueDatabase, tableName))
	colNames := make([]string, len(desc))
	for i, row := range desc {
		colNames[i] = row["column_name"]
	}
	assert.Contains(t, colNames, "email")

	nullRows := athenaQuery(t, ctx, fmt.Sprintf(`SELECT CAST(id AS INTEGER) AS id FROM "%s" WHERE email IS NULL ORDER BY id`, tableName))
	require.Len(t, nullRows, 5)
	assert.Equal(t, "1", nullRows[0]["id"])
	assert.Equal(t, "5", nullRows[4]["id"])
}
