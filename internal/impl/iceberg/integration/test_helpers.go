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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
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

// startTestInfrastructure starts MinIO and iceberg-rest-fixture containers.
// Uses a Docker network for container-to-container communication with port mapping for host access.
func startTestInfrastructure(t *testing.T, ctx context.Context) *testInfrastructure {
	t.Helper()

	infra := &testInfrastructure{}

	// Create a Docker network for container-to-container communication
	net, err := network.New(ctx)
	require.NoError(t, err)
	infra.network = net
	networkName := net.Name

	// Internal ports (containers use these to communicate)
	// Use non-standard ports to avoid conflicts with local services
	const minioInternalPort = "19123"
	const restInternalPort = "18181"

	// Start MinIO container on the shared network
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

	// Use 127.0.0.1 instead of localhost to avoid S3 virtual-hosted style URL issues
	if minioHost == "localhost" {
		minioHost = "127.0.0.1"
	}
	infra.MinioEndpoint = fmt.Sprintf("http://%s:%s", minioHost, minioMappedPort.Port())
	// Containers reach MinIO via Docker network alias
	infra.MinioInternalURL = fmt.Sprintf("http://minio:%s", minioInternalPort)

	t.Logf("MinIO started at: %s (internal: %s)", infra.MinioEndpoint, infra.MinioInternalURL)

	// Start iceberg-rest-fixture container on the shared network
	// This is the Tabular reference implementation used by iceberg-go for testing
	restContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "apache/iceberg-rest-fixture",
			ExposedPorts: []string{restInternalPort + "/tcp"},
			Env: map[string]string{
				// REST catalog port configuration (env var pattern: CATALOG_X_Y → x.y, CATALOG_X__Y → x-y)
				"CATALOG_REST_PORT": restInternalPort,
				// Catalog configuration - use internal MinIO URL
				"CATALOG_WAREHOUSE":              "s3://warehouse/",
				"CATALOG_IO__IMPL":               "org.apache.iceberg.aws.s3.S3FileIO",
				"CATALOG_S3_ENDPOINT":            infra.MinioInternalURL,
				"CATALOG_S3_PATH__STYLE__ACCESS": "true",     // → s3.path-style-access
				"CATALOG_S3_ACCESS__KEY__ID":     "admin",    // → s3.access-key-id
				"CATALOG_S3_SECRET__ACCESS__KEY": "password", // → s3.secret-access-key
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
	// Containers reach REST catalog via Docker network alias
	infra.RestInternalURL = fmt.Sprintf("http://rest:%s", restInternalPort)

	t.Logf("Iceberg REST catalog started at: %s (internal: %s)", infra.RestURL, infra.RestInternalURL)

	// Start DuckDB container on the shared network
	duckdbContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			// We can't use the official duckdb container because it is distroless and there is no sleep command
			Image:      "datacatering/duckdb:v1.4.4",
			Entrypoint: []string{"sleep"},
			Cmd:        []string{"infinity"},
			Networks:   []string{networkName},
		},
		Started: true,
	})
	require.NoError(t, err)
	infra.duckdbContainer = duckdbContainer

	t.Logf("DuckDB container started: %s", duckdbContainer.GetContainerID())

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

	// Configure AWS SDK to use MinIO
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("admin", "password", ""),
		),
	)
	require.NoError(t, err)

	// Create S3 client with MinIO endpoint
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(infra.MinioEndpoint)
		o.UsePathStyle = true
	})

	// Create bucket with explicit us-east-1 location constraint
	// Note: For us-east-1, we don't specify CreateBucketConfiguration (it's implicit)
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
		// us-east-1 doesn't use CreateBucketConfiguration - it's the default region
	})
	require.NoError(t, err)

	// Verify bucket location
	locationResp, err := client.GetBucketLocation(ctx, &s3.GetBucketLocationInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Logf("Warning: Could not get bucket location: %v", err)
	} else {
		region := "us-east-1" // Default when LocationConstraint is empty
		if locationResp.LocationConstraint != "" {
			region = string(locationResp.LocationConstraint)
		}
		t.Logf("Created MinIO bucket: %s (reported region: %s)", bucket, region)
	}
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
	t.Logf("Created namespace: %s", namespace)
}

// ExecSQL executes SQL in the DuckDB container and returns the output.
func (infra *testInfrastructure) ExecSQL(ctx context.Context, sql string) (string, error) {
	if infra.duckdbContainer == nil {
		return "", errors.New("duckdb container not started")
	}

	exitCode, reader, err := infra.duckdbContainer.Exec(ctx, []string{"/duckdb", "-json", "-c", sql})
	if err != nil {
		return "", fmt.Errorf("failed to exec duckdb: %w", err)
	}

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(reader)
	if err != nil {
		return "", fmt.Errorf("failed to read output: %w", err)
	}

	output := buf.String()

	if exitCode != 0 {
		return "", fmt.Errorf("duckdb command failed with exit code %d: %s", exitCode, output)
	}

	return output, nil
}

// duckDBSetupSQL returns SQL to configure DuckDB with Iceberg REST catalog and S3/MinIO access.
func (infra *testInfrastructure) duckDBSetupSQL(catalog string) string {
	// Extract host:port from MinIO endpoint for s3_endpoint (remove http:// prefix)
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

		-- Configure S3/MinIO credentials for DuckDB
		SET s3_region='us-east-1';
		SET s3_access_key_id='admin';
		SET s3_secret_access_key='password';
		SET s3_endpoint='{{MINIO_HOSTPORT}}';
		SET s3_url_style='path';
		SET s3_use_ssl=false;

		-- Attach the Iceberg REST catalog (no auth needed for iceberg-rest-fixture)
		ATTACH IF NOT EXISTS '{{CATALOG}}' AS iceberg_cat (
			TYPE iceberg,
			ENDPOINT '{{REST_URL}}',
			AUTHORIZATION_TYPE 'none'
		);
	`)
}

// parseJSONArray parses JSON array output from DuckDB, handling Docker stream multiplexing prefixes.
// It uses json.NewDecoder to skip over any binary prefix and find valid JSON.
func parseJSONArray[T any](output string) ([]T, error) {
	// Find the start of a JSON array - skip any Docker multiplexing prefix
	startIdx := strings.Index(output, "[")
	if startIdx < 0 {
		return nil, nil // No JSON array found
	}

	// Use json.NewDecoder to parse the JSON array
	decoder := json.NewDecoder(strings.NewReader(output[startIdx:]))

	var results []T
	if err := decoder.Decode(&results); err != nil {
		return nil, fmt.Errorf("failed to decode JSON array: %w", err)
	}

	return results, nil
}

// QueryIcebergTable queries an Iceberg table via the Polaris catalog using DuckDB.
func (infra *testInfrastructure) QueryIcebergTable(ctx context.Context, catalog, namespace, table, whereClause string) ([]map[string]any, error) {
	// Build SQL with setup and query
	sql := infra.duckDBSetupSQL(catalog) + fmt.Sprintf(`
		-- Query the table
		SELECT * FROM iceberg_cat."%s"."%s"
		%s;
	`, namespace, table, whereClause)

	output, err := infra.ExecSQL(ctx, sql)
	if err != nil {
		return nil, err
	}

	return parseJSONArray[map[string]any](output)
}

// IcebergMetadata returns metadata for an Iceberg table using DuckDB's iceberg_metadata function.
func (infra *testInfrastructure) IcebergMetadata(ctx context.Context, catalog, namespace, table string) ([]map[string]any, error) {
	sql := infra.duckDBSetupSQL(catalog) + fmt.Sprintf(`
		SELECT * FROM iceberg_metadata('iceberg_cat."%s"."%s"');
	`, namespace, table)

	output, err := infra.ExecSQL(ctx, sql)
	if err != nil {
		return nil, err
	}

	return parseJSONArray[map[string]any](output)
}

// IcebergSnapshots returns snapshots for an Iceberg table using DuckDB's iceberg_snapshots function.
func (infra *testInfrastructure) IcebergSnapshots(ctx context.Context, catalog, namespace, table string) ([]map[string]any, error) {
	sql := infra.duckDBSetupSQL(catalog) + fmt.Sprintf(`
		SELECT * FROM iceberg_snapshots('iceberg_cat."%s"."%s"');
	`, namespace, table)

	output, err := infra.ExecSQL(ctx, sql)
	if err != nil {
		return nil, err
	}

	return parseJSONArray[map[string]any](output)
}

// CountIcebergRows counts rows in an Iceberg table via the Polaris catalog.
func (infra *testInfrastructure) CountIcebergRows(ctx context.Context, catalog, namespace, table string) (int, error) {
	sql := infra.duckDBSetupSQL(catalog) + fmt.Sprintf(`
		SELECT COUNT(*) as count FROM iceberg_cat."%s"."%s";
	`, namespace, table)

	output, err := infra.ExecSQL(ctx, sql)
	if err != nil {
		return 0, err
	}

	type countResult struct {
		Count int `json:"count"`
	}

	results, err := parseJSONArray[countResult](output)
	if err != nil {
		return 0, fmt.Errorf("failed to parse count: %w\nOutput: %s", err, output)
	}
	if len(results) == 0 {
		return 0, errors.New("no count result returned")
	}

	return results[0].Count, nil
}

// VerifyIcebergData verifies data was written correctly using DuckDB.
func (infra *testInfrastructure) VerifyIcebergData(t *testing.T, ctx context.Context, catalog, namespace, table string, expectedCount int) {
	t.Helper()

	count, err := infra.CountIcebergRows(ctx, catalog, namespace, table)
	require.NoError(t, err, "Failed to count rows with DuckDB")
	require.Equal(t, expectedCount, count, "Row count mismatch")

	t.Logf("Verified %d rows in %s.%s.%s via DuckDB", count, catalog, namespace, table)
}

// IcebergTableExists checks if a table exists in the catalog using DuckDB.
func (infra *testInfrastructure) IcebergTableExists(ctx context.Context, catalog, namespace, table string) bool {
	sql := infra.duckDBSetupSQL(catalog) + fmt.Sprintf(`
		SELECT COUNT(*) as count FROM iceberg_cat."%s"."%s" LIMIT 0;
	`, namespace, table)

	_, err := infra.ExecSQL(ctx, sql)
	return err == nil
}

// ListIcebergTables lists tables in a namespace using the Polaris catalog via DuckDB.
func (infra *testInfrastructure) ListIcebergTables(ctx context.Context, catalog, namespace string) ([]string, error) {
	// Run everything in one batch since secrets are session-scoped
	sql := infra.duckDBSetupSQL(catalog) + fmt.Sprintf(`
		SELECT table_name FROM information_schema.tables WHERE table_schema = '%s' AND table_catalog = 'iceberg_cat';
	`, namespace)

	output, err := infra.ExecSQL(ctx, sql)
	if err != nil {
		return nil, err
	}

	// DuckDB outputs multiple results, find the SELECT result (last array)
	lastBracket := strings.LastIndex(output, "[")
	if lastBracket < 0 {
		return nil, nil
	}

	// Use json.NewDecoder to parse the last JSON array
	decoder := json.NewDecoder(strings.NewReader(output[lastBracket:]))

	var results []map[string]any
	if err := decoder.Decode(&results); err != nil {
		return nil, fmt.Errorf("failed to parse tables: %w\nOutput: %s", err, output)
	}

	var tables []string
	for _, row := range results {
		if name, ok := row["table_name"].(string); ok {
			tables = append(tables, name)
		}
	}

	return tables, nil
}
