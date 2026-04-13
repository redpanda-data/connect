// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package bigquery

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/license"
)

func TestIntegrationBigQueryWriteAPI(t *testing.T) {
	integration.CheckSkip(t)

	const (
		projectID = "test-project"
		datasetID = "test_dataset"
		tableID   = "test_table"
	)

	// Given a BigQuery emulator with an empty table.
	t.Log("Given a BigQuery emulator running with HTTP and gRPC ports")
	ctr, err := testcontainers.Run(t.Context(),
		"ghcr.io/goccy/bigquery-emulator:latest",
		testcontainers.WithExposedPorts("9050/tcp", "9060/tcp"),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Cmd: []string{
					"--project=" + projectID,
					"--dataset=" + datasetID,
					"--log-level=debug",
				},
			},
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("9050/tcp").WithStartupTimeout(60*time.Second),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	httpPort, err := ctr.MappedPort(t.Context(), "9050/tcp")
	require.NoError(t, err)
	grpcPort, err := ctr.MappedPort(t.Context(), "9060/tcp")
	require.NoError(t, err)

	httpEndpoint := fmt.Sprintf("http://localhost:%s", httpPort.Port())
	grpcEndpoint := fmt.Sprintf("localhost:%s", grpcPort.Port())

	t.Log("Given a table with name and age columns")
	bqClient, err := bigquery.NewClient(t.Context(), projectID,
		option.WithoutAuthentication(),
		option.WithEndpoint(httpEndpoint),
	)
	require.NoError(t, err)
	defer bqClient.Close()

	schema := bigquery.Schema{
		{Name: "name", Type: bigquery.StringFieldType, Required: true},
		{Name: "age", Type: bigquery.IntegerFieldType, Required: true},
	}
	err = bqClient.Dataset(datasetID).Table(tableID).Create(t.Context(), &bigquery.TableMetadata{
		Schema: schema,
	})
	require.NoError(t, err)

	// When we build a stream with the BigQuery Write API output and send messages.
	t.Log("When we send 3 JSON messages through the BigQuery Write API output")
	sb := service.NewStreamBuilder()
	require.NoError(t, sb.SetLoggerYAML(`level: DEBUG`))

	sendFn, err := sb.AddProducerFunc()
	require.NoError(t, err)

	require.NoError(t, sb.AddOutputYAML(fmt.Sprintf(`
gcp_bigquery_write_api:
  project: %s
  dataset: %s
  table: %s
  endpoint:
    http: %s
    grpc: %s
`, projectID, datasetID, tableID, httpEndpoint, grpcEndpoint)))

	stream, err := sb.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())

	go func() {
		if err := stream.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("stream error: %v", err)
		}
	}()

	t.Cleanup(func() {
		if err := stream.StopWithin(10 * time.Second); err != nil {
			t.Log(err)
		}
	})

	for i, msg := range []string{
		`{"name":"alice","age":"30"}`,
		`{"name":"bob","age":"25"}`,
		`{"name":"charlie","age":"40"}`,
	} {
		require.NoError(t, sendFn(t.Context(), service.NewMessage([]byte(msg))), "message %d", i)
	}

	// Then all 3 rows land in the BigQuery table.
	t.Log("Then all 3 rows are present in the BigQuery table")
	assert.Eventually(t, func() bool {
		it := bqClient.Dataset(datasetID).Table(tableID).Read(t.Context())
		var count int
		for {
			var row map[string]bigquery.Value
			err := it.Next(&row)
			if errors.Is(err, iterator.Done) {
				break
			}
			if err != nil {
				return false
			}
			count++
		}
		return count >= 3
	}, 30*time.Second, 500*time.Millisecond)
}
