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
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/reflect/protoreflect"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/license"
)

// bqEmulator holds connection details for a running BigQuery emulator container.
type bqEmulator struct {
	httpEndpoint string
	grpcEndpoint string
	bqClient     *bigquery.Client
}

// startEmulator launches a BigQuery emulator container and returns connection details.
func startEmulator(t *testing.T, projectID, datasetID string) *bqEmulator {
	t.Helper()

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
			wait.ForAll(
				wait.ForListeningPort("9050/tcp"),
				wait.ForListeningPort("9060/tcp"),
			).WithDeadline(60*time.Second),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	host, err := ctr.Host(t.Context())
	require.NoError(t, err)
	httpPort, err := ctr.MappedPort(t.Context(), "9050/tcp")
	require.NoError(t, err)
	grpcPort, err := ctr.MappedPort(t.Context(), "9060/tcp")
	require.NoError(t, err)

	httpEndpoint := fmt.Sprintf("http://%s:%s", host, httpPort.Port())
	grpcEndpoint := fmt.Sprintf("%s:%s", host, grpcPort.Port())

	bqClient, err := bigquery.NewClient(t.Context(), projectID,
		option.WithoutAuthentication(),
		option.WithEndpoint(httpEndpoint),
	)
	require.NoError(t, err)
	t.Cleanup(func() { bqClient.Close() })

	return &bqEmulator{
		httpEndpoint: httpEndpoint,
		grpcEndpoint: grpcEndpoint,
		bqClient:     bqClient,
	}
}

// bqSchemaToMessageDescriptor converts a BigQuery schema to a protoreflect.MessageDescriptor
// via the adapt pipeline, for use in tests that need proto descriptors.
func bqSchemaToMessageDescriptor(t *testing.T, schema bigquery.Schema) protoreflect.MessageDescriptor {
	t.Helper()
	tableSchema, err := adapt.BQSchemaToStorageTableSchema(schema)
	require.NoError(t, err)
	descriptor, err := adapt.StorageSchemaToProto2Descriptor(tableSchema, "root")
	require.NoError(t, err)
	md, ok := descriptor.(protoreflect.MessageDescriptor)
	require.True(t, ok)
	return md
}

func TestIntegrationBigQueryWriteAPI(t *testing.T) {
	integration.CheckSkip(t)

	const (
		projectID = "test-project"
		datasetID = "test_dataset"
		tableID   = "test_table"
	)

	emu := startEmulator(t, projectID, datasetID)

	t.Log("Given a table with name and age columns")
	schema := bigquery.Schema{
		{Name: "name", Type: bigquery.StringFieldType, Required: true},
		{Name: "age", Type: bigquery.IntegerFieldType, Required: true},
	}
	err := emu.bqClient.Dataset(datasetID).Table(tableID).Create(t.Context(), &bigquery.TableMetadata{
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
`, projectID, datasetID, tableID, emu.httpEndpoint, emu.grpcEndpoint)))

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
		it := emu.bqClient.Dataset(datasetID).Table(tableID).Read(t.Context())
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

func TestIntegrationSchemaEvolution(t *testing.T) {
	integration.CheckSkip(t)

	const (
		projectID = "test-project"
		datasetID = "test_dataset"
		tableID   = "test_evolution"
	)

	emu := startEmulator(t, projectID, datasetID)

	t.Log("Given a table with name and age columns")
	err := emu.bqClient.Dataset(datasetID).Table(tableID).Create(t.Context(), &bigquery.TableMetadata{
		Schema: bigquery.Schema{
			{Name: "name", Type: bigquery.StringFieldType},
			{Name: "age", Type: bigquery.IntegerFieldType},
		},
	})
	require.NoError(t, err)

	// Build a proto descriptor with an extra "email" field.
	expandedSchema := bigquery.Schema{
		{Name: "name", Type: bigquery.StringFieldType},
		{Name: "age", Type: bigquery.IntegerFieldType},
		{Name: "email", Type: bigquery.StringFieldType},
	}
	md := bqSchemaToMessageDescriptor(t, expandedSchema)

	t.Log("When we call Evolve with a descriptor that has an extra email column")
	evolver := &schemaEvolver{
		bqClient:  emu.bqClient,
		datasetID: datasetID,
		log:       service.MockResources().Logger(),
	}
	evolved, err := evolver.Evolve(t.Context(), tableID, md)
	require.NoError(t, err)
	assert.True(t, evolved, "expected columns to be added")

	t.Log("Then the table schema includes the email column")
	meta, err := emu.bqClient.Dataset(datasetID).Table(tableID).Metadata(t.Context())
	require.NoError(t, err)

	var colNames []string
	for _, f := range meta.Schema {
		colNames = append(colNames, f.Name)
	}
	assert.Contains(t, colNames, "email")
	assert.Contains(t, colNames, "name")
	assert.Contains(t, colNames, "age")
}

func TestIntegrationTableNameSanitization(t *testing.T) {
	integration.CheckSkip(t)

	const (
		projectID = "test-project"
		datasetID = "test_dataset"
	)

	emu := startEmulator(t, projectID, datasetID)

	// Create a table with the sanitized name. The output will receive
	// "events.user.created" but sanitize it to "events_user_created".
	sanitizedTableID := "events_user_created"
	err := emu.bqClient.Dataset(datasetID).Table(sanitizedTableID).Create(t.Context(), &bigquery.TableMetadata{
		Schema: bigquery.Schema{
			{Name: "name", Type: bigquery.StringFieldType},
		},
	})
	require.NoError(t, err)

	t.Log("When we send a message with a dot-separated table name")
	sb := service.NewStreamBuilder()
	require.NoError(t, sb.SetLoggerYAML(`level: DEBUG`))

	sendFn, err := sb.AddProducerFunc()
	require.NoError(t, err)

	// Use a static table name with dots — sanitization should convert to underscores.
	require.NoError(t, sb.AddOutputYAML(fmt.Sprintf(`
gcp_bigquery_write_api:
  project: %s
  dataset: %s
  table: events.user.created
  endpoint:
    http: %s
    grpc: %s
`, projectID, datasetID, emu.httpEndpoint, emu.grpcEndpoint)))

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

	require.NoError(t, sendFn(t.Context(), service.NewMessage([]byte(`{"name":"alice"}`))))

	t.Log("Then the row lands in the sanitized table name")
	assert.Eventually(t, func() bool {
		it := emu.bqClient.Dataset(datasetID).Table(sanitizedTableID).Read(t.Context())
		var count int
		for {
			var row map[string]bigquery.Value
			if err := it.Next(&row); errors.Is(err, iterator.Done) {
				break
			} else if err != nil {
				return false
			}
			count++
		}
		return count >= 1
	}, 30*time.Second, 500*time.Millisecond)
}
