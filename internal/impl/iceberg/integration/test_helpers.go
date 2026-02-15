// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/io"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/redpanda-data/benthos/v4/public/service"

	icebergimpl "github.com/redpanda-data/connect/v4/internal/impl/iceberg"
	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/catalogx"
)

// testInfrastructure holds containers and connection info for integration tests.
type testInfrastructure struct {
	network         *testcontainers.DockerNetwork
	minioContainer  testcontainers.Container
	restContainer   testcontainers.Container
	duckdbContainer testcontainers.Container

	MinioEndpoint    string // Endpoint for host/test code to reach MinIO
	MinioInternalURL string // Endpoint for containers to reach MinIO (via Docker network)
	RestURL          string // Endpoint for host/test code to reach REST catalog
	RestInternalURL  string // Endpoint for containers to reach REST catalog (via Docker network)
}

// setupTestInfra starts all containers, creates the warehouse bucket, and
// registers cleanup. This is the single entry point for all integration tests.
func setupTestInfra(t *testing.T, ctx context.Context) *testInfrastructure {
	t.Helper()
	infra := startTestInfrastructure(t, ctx)
	t.Cleanup(func() { require.NoError(t, infra.Terminate(context.Background())) })
	infra.CreateBucket(t, "warehouse")
	return infra
}

// CatalogConfig returns a catalogx.Config pre-populated with MinIO/REST
// credentials suitable for integration tests.
func (infra *testInfrastructure) CatalogConfig() catalogx.Config {
	return catalogx.Config{
		URL:      infra.RestURL,
		AuthType: "none",
		AdditionalProps: iceberg.Properties{
			io.S3AccessKeyID:            "admin",
			io.S3SecretAccessKey:        "password",
			io.S3EndpointURL:            infra.MinioEndpoint,
			io.S3ForceVirtualAddressing: "false",
			io.S3Region:                 "us-east-1",
		},
	}
}

// NewCatalogClient creates a catalogx.Client for the given namespace,
// using the standard test credentials. It registers t.Cleanup to close the client.
func (infra *testInfrastructure) NewCatalogClient(t *testing.T, namespace string) *catalogx.Client {
	t.Helper()
	client, err := catalogx.NewCatalogClient(infra.CatalogConfig(), []string{namespace})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// RouterOption configures a test router.
type RouterOption func(*routerOpts)

type routerOpts struct {
	schemaEvoCfg icebergimpl.SchemaEvolutionConfig
}

// WithSchemaEvolution enables schema evolution on the test router.
func WithSchemaEvolution(cfg icebergimpl.SchemaEvolutionConfig) RouterOption {
	return func(o *routerOpts) {
		o.schemaEvoCfg = cfg
	}
}

// NewRouter creates a Router for the given namespace and table expressions,
// using the standard test credentials. It registers t.Cleanup to close the router.
// The namespace and table strings can be static or Bloblang interpolation expressions.
func (infra *testInfrastructure) NewRouter(
	t *testing.T,
	namespace, table string,
	opts ...RouterOption,
) *icebergimpl.Router {
	t.Helper()

	o := routerOpts{
		schemaEvoCfg: icebergimpl.SchemaEvolutionConfig{Enabled: false},
	}
	for _, opt := range opts {
		opt(&o)
	}

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
	router := icebergimpl.NewRouter(infra.CatalogConfig(), namespaceStr, tableStr, o.schemaEvoCfg, commitCfg, logger)
	t.Cleanup(func() { router.Close() })
	return router
}

// produce routes JSON messages through a router and waits for the commit to complete.
func produce(t *testing.T, ctx context.Context, router *icebergimpl.Router, jsonMsgs ...string) {
	t.Helper()
	batch := make(service.MessageBatch, len(jsonMsgs))
	for i, j := range jsonMsgs {
		batch[i] = service.NewMessage([]byte(j))
	}
	require.NoError(t, router.Route(ctx, batch))
	time.Sleep(500 * time.Millisecond)
}

// produceMessages routes a pre-built MessageBatch through a router and waits
// for the commit to complete. Use this when messages need metadata or typed
// structured data that produce() cannot express.
func produceMessages(t *testing.T, ctx context.Context, router *icebergimpl.Router, batch service.MessageBatch) {
	t.Helper()
	require.NoError(t, router.Route(ctx, batch))
	time.Sleep(500 * time.Millisecond)
}

// querySQL executes a SQL query against DuckDB through the Iceberg REST catalog
// and parses the results into a slice of T. The DuckDB setup (iceberg extension,
// S3 credentials, catalog attach) is prepended automatically. Tables are
// accessible as iceberg_cat."namespace"."table".
func querySQL[T any](t *testing.T, ctx context.Context, infra *testInfrastructure, sql string) []T {
	t.Helper()
	fullSQL := infra.duckDBSetupSQL("rest") + sql
	output, err := infra.ExecSQL(ctx, fullSQL)
	require.NoError(t, err)
	results, err := parseJSONArray[T](output)
	require.NoError(t, err)
	return results
}

// countResult is used with querySQL to parse COUNT(*) results from DuckDB.
type countResult struct {
	Count int `json:"count"`
}

// ColumnInfo represents a column's schema information from DuckDB DESCRIBE.
type ColumnInfo struct {
	ColumnName string `json:"column_name"`
	ColumnType string `json:"column_type"`
	Null       string `json:"null"`
}

// createMessageWithMeta creates a message with structured data and metadata.
func createMessageWithMeta(t *testing.T, data map[string]any, metaKey, metaValue string) *service.Message {
	t.Helper()
	msg := service.NewMessage(nil)
	msg.SetStructured(data)
	msg.MetaSetMut(metaKey, metaValue)
	return msg
}

// ---------------------------------------------------------------------------
// Infrastructure setup (internal)
// ---------------------------------------------------------------------------

// startTestInfrastructure starts MinIO, iceberg-rest-fixture, and DuckDB containers.
func startTestInfrastructure(t *testing.T, ctx context.Context) *testInfrastructure {
	t.Helper()

	infra := &testInfrastructure{}

	net, err := network.New(ctx)
	require.NoError(t, err)
	infra.network = net
	networkName := net.Name

	const minioInternalPort = "19123"
	const restInternalPort = "18181"

	// Start MinIO
	minioContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "minio/minio:latest",
			ExposedPorts: []string{minioInternalPort + "/tcp"},
			Env: map[string]string{
				"MINIO_ROOT_USER":     "admin",
				"MINIO_ROOT_PASSWORD": "password",
				"MINIO_REGION":        "us-east-1",
			},
			Cmd:      []string{"server", "/data", "--address", ":" + minioInternalPort},
			Networks: []string{networkName},
			NetworkAliases: map[string][]string{
				networkName: {"minio"},
			},
			WaitingFor: wait.ForHTTP("/minio/health/live").
				WithPort(minioInternalPort + "/tcp").
				WithStartupTimeout(time.Minute),
		},
		Started: true,
	})
	require.NoError(t, err)
	infra.minioContainer = minioContainer

	minioHost, err := minioContainer.Host(ctx)
	require.NoError(t, err)
	minioMappedPort, err := minioContainer.MappedPort(ctx, minioInternalPort)
	require.NoError(t, err)

	if minioHost == "localhost" {
		minioHost = "127.0.0.1"
	}
	infra.MinioEndpoint = fmt.Sprintf("http://%s:%s", minioHost, minioMappedPort.Port())
	infra.MinioInternalURL = "http://minio:" + minioInternalPort

	t.Logf("MinIO started at: %s (internal: %s)", infra.MinioEndpoint, infra.MinioInternalURL)

	// Start iceberg-rest-fixture
	restContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "apache/iceberg-rest-fixture",
			ExposedPorts: []string{restInternalPort + "/tcp"},
			Env: map[string]string{
				"CATALOG_REST_PORT":              restInternalPort,
				"CATALOG_WAREHOUSE":              "s3://warehouse/",
				"CATALOG_IO__IMPL":               "org.apache.iceberg.aws.s3.S3FileIO",
				"CATALOG_S3_ENDPOINT":            infra.MinioInternalURL,
				"CATALOG_S3_PATH__STYLE__ACCESS": "true",
				"CATALOG_S3_ACCESS__KEY__ID":     "admin",
				"CATALOG_S3_SECRET__ACCESS__KEY": "password",
				"AWS_REGION":                     "us-east-1",
			},
			Networks: []string{networkName},
			NetworkAliases: map[string][]string{
				networkName: {"rest"},
			},
			WaitingFor: wait.ForHTTP("/v1/config").
				WithPort(restInternalPort + "/tcp").
				WithStartupTimeout(time.Minute),
		},
		Started: true,
	})
	require.NoError(t, err)
	infra.restContainer = restContainer

	restHost, err := restContainer.Host(ctx)
	require.NoError(t, err)
	restMappedPort, err := restContainer.MappedPort(ctx, restInternalPort)
	require.NoError(t, err)

	if restHost == "localhost" {
		restHost = "127.0.0.1"
	}
	infra.RestURL = fmt.Sprintf("http://%s:%s", restHost, restMappedPort.Port())
	infra.RestInternalURL = "http://rest:" + restInternalPort

	t.Logf("Iceberg REST catalog started at: %s (internal: %s)", infra.RestURL, infra.RestInternalURL)

	// Start DuckDB
	duckdbContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:      "datacatering/duckdb:v1.4.4",
			Entrypoint: []string{"sleep"},
			Cmd:        []string{"infinity"},
			Networks:   []string{networkName},
		},
		Started: true,
	})
	require.NoError(t, err)
	infra.duckdbContainer = duckdbContainer

	return infra
}

// Terminate cleans up all containers and network.
func (infra *testInfrastructure) Terminate(ctx context.Context) error {
	var errs []error

	if infra.duckdbContainer != nil {
		if err := infra.duckdbContainer.Terminate(ctx); err != nil {
			errs = append(errs, fmt.Errorf("terminate duckdb: %w", err))
		}
	}
	if infra.restContainer != nil {
		if err := infra.restContainer.Terminate(ctx); err != nil {
			errs = append(errs, fmt.Errorf("terminate rest: %w", err))
		}
	}
	if infra.minioContainer != nil {
		if err := infra.minioContainer.Terminate(ctx); err != nil {
			errs = append(errs, fmt.Errorf("terminate minio: %w", err))
		}
	}
	if infra.network != nil {
		if err := infra.network.Remove(ctx); err != nil {
			errs = append(errs, fmt.Errorf("remove network: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("cleanup errors: %v", errs)
	}
	return nil
}

// CreateBucket creates a bucket in MinIO.
func (infra *testInfrastructure) CreateBucket(t *testing.T, bucket string) {
	t.Helper()

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("admin", "password", ""),
		),
	)
	require.NoError(t, err)

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(infra.MinioEndpoint)
		o.UsePathStyle = true
	})

	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)
}

// CreateNamespace creates a namespace in the Iceberg REST catalog.
func (infra *testInfrastructure) CreateNamespace(t *testing.T, namespace string) {
	t.Helper()

	body := `{"namespace": ["` + namespace + `"]}`
	resp, err := http.Post(infra.RestURL+"/v1/namespaces", "application/json", strings.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()

	require.True(t, resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated,
		"create namespace failed: %d", resp.StatusCode)
}

// ExecSQL executes SQL in the DuckDB container and returns the output.
func (infra *testInfrastructure) ExecSQL(ctx context.Context, sql string) (string, error) {
	if infra.duckdbContainer == nil {
		return "", errors.New("duckdb container not started")
	}

	exitCode, reader, err := infra.duckdbContainer.Exec(ctx, []string{"/duckdb", "-json", "-c", sql})
	if err != nil {
		return "", fmt.Errorf("executing duckdb: %w", err)
	}

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(reader)
	if err != nil {
		return "", fmt.Errorf("reading output: %w", err)
	}

	output := buf.String()
	if exitCode != 0 {
		return "", fmt.Errorf("duckdb command failed with exit code %d: %s", exitCode, output)
	}

	return output, nil
}

// duckDBSetupSQL returns SQL to configure DuckDB with Iceberg REST catalog and S3/MinIO access.
func (infra *testInfrastructure) duckDBSetupSQL(catalog string) string {
	minioHostPort := strings.TrimPrefix(infra.MinioInternalURL, "http://")
	minioHostPort = strings.TrimPrefix(minioHostPort, "https://")

	replacer := strings.NewReplacer(
		"{{MINIO_HOSTPORT}}", minioHostPort,
		"{{REST_URL}}", infra.RestInternalURL,
		"{{CATALOG}}", catalog,
	)

	return replacer.Replace(`
		INSTALL iceberg;
		LOAD iceberg;
		INSTALL httpfs;
		LOAD httpfs;

		SET s3_region='us-east-1';
		SET s3_access_key_id='admin';
		SET s3_secret_access_key='password';
		SET s3_endpoint='{{MINIO_HOSTPORT}}';
		SET s3_url_style='path';
		SET s3_use_ssl=false;

		ATTACH IF NOT EXISTS '{{CATALOG}}' AS iceberg_cat (
			TYPE iceberg,
			ENDPOINT '{{REST_URL}}',
			AUTHORIZATION_TYPE 'none'
		);
	`)
}

// parseJSONArray parses JSON array output from DuckDB, handling Docker stream multiplexing prefixes.
func parseJSONArray[T any](output string) ([]T, error) {
	startIdx := strings.Index(output, "[")
	if startIdx < 0 {
		return nil, nil
	}

	decoder := json.NewDecoder(strings.NewReader(output[startIdx:]))
	var results []T
	if err := decoder.Decode(&results); err != nil {
		return nil, fmt.Errorf("decoding JSON array: %w", err)
	}

	return results, nil
}
