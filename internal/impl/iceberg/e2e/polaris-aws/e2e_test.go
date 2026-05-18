// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package polarisaws

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/redpanda-data/benthos/v4/public/service"

	icebergimpl "github.com/redpanda-data/connect/v4/internal/impl/iceberg"
	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/catalogx"
)

var (
	awsRegion     = flag.String("aws.region", "us-east-1", "AWS region")
	awsBucket     = flag.String("aws.bucket", "", "S3 warehouse bucket")
	awsRoleArn    = flag.String("aws.role-arn", "", "IAM role ARN for Polaris credential vendoring")
	soakDuration  = flag.Duration("test.soak-duration", 2*time.Hour, "Duration to run the soak test")
	batchInterval = flag.Duration("test.batch-interval", 5*time.Minute, "Interval between batches")
)

func skipIfNotConfigured(t *testing.T) {
	t.Helper()
	if *awsBucket == "" || *awsRoleArn == "" {
		t.Skip("set -aws.bucket, -aws.role-arn flags to run Polaris AWS e2e tests")
	}
}

func startPolaris(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	// Load current AWS credentials to pass into the Polaris container
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(*awsRegion))
	require.NoError(t, err)
	creds, err := cfg.Credentials.Retrieve(ctx)
	require.NoError(t, err)

	env := map[string]string{
		"POLARIS_BOOTSTRAP_CREDENTIALS": "POLARIS,root,secret",
		"AWS_ACCESS_KEY_ID":             creds.AccessKeyID,
		"AWS_SECRET_ACCESS_KEY":         creds.SecretAccessKey,
		"AWS_REGION":                    *awsRegion,
	}
	if creds.SessionToken != "" {
		env["AWS_SESSION_TOKEN"] = creds.SessionToken
	}

	ctr, err := testcontainers.Run(ctx, "apache/polaris:latest",
		testcontainers.WithExposedPorts("8181/tcp", "8182/tcp"),
		testcontainers.WithEnv(env),
		testcontainers.WithWaitStrategy(
			wait.ForHTTP("/q/health/ready").WithPort("8182/tcp"),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := ctr.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	})

	host, err := ctr.Host(ctx)
	require.NoError(t, err)
	port, err := ctr.MappedPort(ctx, "8181/tcp")
	require.NoError(t, err)

	return fmt.Sprintf("http://%s:%s", host, port.Port())
}

func getOAuth2Token(t *testing.T, polarisURL string) string {
	t.Helper()
	data := "grant_type=client_credentials&client_id=root&client_secret=secret&scope=PRINCIPAL_ROLE:ALL"
	resp, err := http.Post(
		polarisURL+"/api/catalog/v1/oauth/tokens",
		"application/x-www-form-urlencoded",
		bytes.NewBufferString(data),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Less(t, resp.StatusCode, 300, "OAuth2 token request failed: %s", string(body))

	var result struct {
		AccessToken string `json:"access_token"`
	}
	require.NoError(t, json.Unmarshal(body, &result))
	require.NotEmpty(t, result.AccessToken, "OAuth2 token is empty")
	return result.AccessToken
}

func polarisHTTP(t *testing.T, method, url, token string, payload any) {
	t.Helper()
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	req, err := http.NewRequest(method, url, bytes.NewBuffer(body))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	require.Less(t, resp.StatusCode, 300, "%s %s failed (%d): %s", method, url, resp.StatusCode, string(respBody))
}

func createPolarisCatalog(t *testing.T, polarisURL, token, catalogName, warehouseLocation, roleArn string) {
	t.Helper()
	polarisHTTP(t, "POST", polarisURL+"/api/management/v1/catalogs", token, map[string]any{
		"catalog": map[string]any{
			"name": catalogName,
			"type": "INTERNAL",
			"properties": map[string]string{
				"default-base-location": warehouseLocation,
			},
			"storageConfigInfo": map[string]any{
				"storageType":      "S3",
				"allowedLocations": []string{warehouseLocation},
				"roleArn":          roleArn,
			},
		},
	})
}

func grantCatalogAccess(t *testing.T, polarisURL, token, catalogName string) {
	t.Helper()

	// Create catalog role
	polarisHTTP(t, "POST",
		polarisURL+"/api/management/v1/catalogs/"+catalogName+"/catalog-roles",
		token,
		map[string]any{"catalogRole": map[string]string{"name": "admin"}},
	)

	// Grant CATALOG_MANAGE_CONTENT privilege
	polarisHTTP(t, "PUT",
		polarisURL+"/api/management/v1/catalogs/"+catalogName+"/catalog-roles/admin/grants",
		token,
		map[string]any{"grant": map[string]string{"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}},
	)

	// Assign catalog role to service_admin principal role
	polarisHTTP(t, "PUT",
		polarisURL+"/api/management/v1/principal-roles/service_admin/catalog-roles/"+catalogName,
		token,
		map[string]any{"catalogRole": map[string]string{"name": "admin"}},
	)
}

func buildCatalogConfig(polarisURL, catalogName string) catalogx.Config {
	return catalogx.Config{
		URL:                polarisURL + "/api/catalog",
		Prefix:             catalogName,
		Warehouse:          catalogName,
		AuthType:           "oauth2",
		OAuth2ClientID:     "root",
		OAuth2ClientSecret: "secret",
		OAuth2Scope:        "PRINCIPAL_ROLE:ALL",
		// No AdditionalProps — Polaris vends S3 credentials via STS AssumeRole
	}
}

func newRouter(t *testing.T, catalogCfg catalogx.Config, namespace, tableName string, schemaEvo bool) *icebergimpl.Router {
	t.Helper()
	namespaceStr, err := service.NewInterpolatedString(namespace)
	require.NoError(t, err)
	tableStr, err := service.NewInterpolatedString(tableName)
	require.NoError(t, err)

	logger := service.MockResources().Logger()
	commitCfg := icebergimpl.CommitConfig{
		ManifestMergeEnabled: true,
		MaxSnapshotAge:       24 * time.Hour,
		MaxRetries:           3,
	}
	schemaEvoCfg := icebergimpl.SchemaEvolutionConfig{
		Enabled: schemaEvo,
	}
	router := icebergimpl.NewRouter(catalogCfg, namespaceStr, tableStr, true, schemaEvoCfg, commitCfg, nil, logger)
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

func s3Cleanup(t *testing.T, bucket, region, prefix string) {
	t.Helper()
	ctx := context.Background()

	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		t.Logf("warning: failed to load AWS config for cleanup: %v", err)
		return
	}

	client := s3.NewFromConfig(cfg)

	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: &bucket,
		Prefix: &prefix,
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			t.Logf("warning: failed to list S3 objects: %v", err)
			return
		}
		for _, obj := range page.Contents {
			if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: &bucket,
				Key:    obj.Key,
			}); err != nil {
				t.Logf("warning: failed to delete S3 object %s: %v", *obj.Key, err)
			}
		}
	}
}

func TestPolarisAWSE2E_BasicWrite(t *testing.T) {
	skipIfNotConfigured(t)

	ctx := t.Context()
	polarisURL := startPolaris(t)
	token := getOAuth2Token(t, polarisURL)

	catalogName := fmt.Sprintf("catalog_%d", time.Now().UnixNano())
	warehouseLocation := fmt.Sprintf("s3://%s/", *awsBucket)
	createPolarisCatalog(t, polarisURL, token, catalogName, warehouseLocation, *awsRoleArn)
	grantCatalogAccess(t, polarisURL, token, catalogName)

	catalogCfg := buildCatalogConfig(polarisURL, catalogName)
	namespace := "e2e_ns"

	// Create namespace
	client, err := catalogx.NewCatalogClient(ctx, catalogCfg, []string{namespace})
	require.NoError(t, err)
	defer client.Close()
	require.NoError(t, client.CreateNamespace(ctx, nil))

	tableName := fmt.Sprintf("e2e_basic_%d", time.Now().UnixNano())
	t.Cleanup(func() { s3Cleanup(t, *awsBucket, *awsRegion, namespace+"/"+tableName) })

	router := newRouter(t, catalogCfg, namespace, tableName, true)
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

	// Verify via catalog client
	tbl, err := client.LoadTable(ctx, tableName)
	require.NoError(t, err)

	fields := tbl.Schema().Fields()
	colNames := make([]string, len(fields))
	for i, f := range fields {
		colNames[i] = f.Name
	}
	assert.Contains(t, colNames, "id")
	assert.Contains(t, colNames, "name")
	assert.Contains(t, colNames, "event_type")
	assert.Contains(t, colNames, "value")

	snapshot := tbl.CurrentSnapshot()
	require.NotNil(t, snapshot)
	assert.Equal(t, "10", snapshot.Summary.Properties["total-records"])
}

func TestPolarisAWSE2E_SchemaEvolution(t *testing.T) {
	skipIfNotConfigured(t)

	ctx := t.Context()
	polarisURL := startPolaris(t)
	token := getOAuth2Token(t, polarisURL)

	catalogName := fmt.Sprintf("catalog_%d", time.Now().UnixNano())
	warehouseLocation := fmt.Sprintf("s3://%s/", *awsBucket)
	createPolarisCatalog(t, polarisURL, token, catalogName, warehouseLocation, *awsRoleArn)
	grantCatalogAccess(t, polarisURL, token, catalogName)

	catalogCfg := buildCatalogConfig(polarisURL, catalogName)
	namespace := "e2e_ns"

	// Create namespace
	client, err := catalogx.NewCatalogClient(ctx, catalogCfg, []string{namespace})
	require.NoError(t, err)
	defer client.Close()
	require.NoError(t, client.CreateNamespace(ctx, nil))

	tableName := fmt.Sprintf("e2e_schema_evo_%d", time.Now().UnixNano())
	t.Cleanup(func() { s3Cleanup(t, *awsBucket, *awsRegion, namespace+"/"+tableName) })

	router := newRouter(t, catalogCfg, namespace, tableName, true)

	// Batch 1: id, name
	produce(t, ctx, router,
		`{"id": 1, "name": "alice"}`,
		`{"id": 2, "name": "bob"}`,
		`{"id": 3, "name": "charlie"}`,
		`{"id": 4, "name": "dave"}`,
		`{"id": 5, "name": "eve"}`,
	)

	// Batch 2: id, name, email (triggers schema evolution)
	produce(t, ctx, router,
		`{"id": 6, "name": "frank", "email": "frank@example.com"}`,
		`{"id": 7, "name": "grace", "email": "grace@example.com"}`,
		`{"id": 8, "name": "henry", "email": "henry@example.com"}`,
		`{"id": 9, "name": "iris", "email": "iris@example.com"}`,
		`{"id": 10, "name": "jack", "email": "jack@example.com"}`,
	)

	// Verify via catalog client
	tbl, err := client.LoadTable(ctx, tableName)
	require.NoError(t, err)

	fields := tbl.Schema().Fields()
	colNames := make([]string, len(fields))
	for i, f := range fields {
		colNames[i] = f.Name
	}
	assert.Contains(t, colNames, "email", "email column should exist after schema evolution")

	snapshot := tbl.CurrentSnapshot()
	require.NotNil(t, snapshot)
	assert.Equal(t, "10", snapshot.Summary.Properties["total-records"])
}

func TestPolarisAWSE2E_CredentialRefreshSoak(t *testing.T) {
	skipIfNotConfigured(t)

	ctx := t.Context()
	polarisURL := startPolaris(t)
	token := getOAuth2Token(t, polarisURL)

	catalogName := fmt.Sprintf("catalog_%d", time.Now().UnixNano())
	warehouseLocation := fmt.Sprintf("s3://%s/", *awsBucket)
	createPolarisCatalog(t, polarisURL, token, catalogName, warehouseLocation, *awsRoleArn)
	grantCatalogAccess(t, polarisURL, token, catalogName)

	catalogCfg := buildCatalogConfig(polarisURL, catalogName)
	namespace := "soak_ns"

	// Create namespace
	client, err := catalogx.NewCatalogClient(ctx, catalogCfg, []string{namespace})
	require.NoError(t, err)
	defer client.Close()
	require.NoError(t, client.CreateNamespace(ctx, nil))

	tableName := fmt.Sprintf("soak_%d", time.Now().UnixNano())
	t.Cleanup(func() { s3Cleanup(t, *awsBucket, *awsRegion, namespace+"/"+tableName) })

	router := newRouter(t, catalogCfg, namespace, tableName, true)

	startTime := time.Now()
	deadline := startTime.Add(*soakDuration)
	batchNum := 0
	totalRecords := 0

	t.Logf("Starting soak test: duration=%v, interval=%v", *soakDuration, *batchInterval)

	// Write first batch immediately
	batchNum++
	writeBatch(t, ctx, router, batchNum, startTime, &totalRecords)

	ticker := time.NewTicker(*batchInterval)
	defer ticker.Stop()

	for range ticker.C {
		if time.Now().After(deadline) {
			goto verify
		}
		batchNum++
		writeBatch(t, ctx, router, batchNum, startTime, &totalRecords)
	}

verify:
	// Verify final state
	t.Logf("Soak test complete: %d batches, %d total records, elapsed %v", batchNum, totalRecords, time.Since(startTime))

	tbl, err := client.LoadTable(ctx, tableName)
	require.NoError(t, err)

	snapshot := tbl.CurrentSnapshot()
	require.NotNil(t, snapshot)
	t.Logf("Final snapshot: %s total records", snapshot.Summary.Properties["total-records"])
	assert.Equal(t, fmt.Sprintf("%d", totalRecords), snapshot.Summary.Properties["total-records"])
}

func writeBatch(t *testing.T, ctx context.Context, router *icebergimpl.Router, batchNum int, startTime time.Time, totalRecords *int) {
	t.Helper()
	batchStart := time.Now()

	records := make([]string, 10)
	for i := range records {
		id := (batchNum-1)*10 + i + 1
		records[i] = fmt.Sprintf(`{"id": %d, "name": "user_%d", "batch": %d, "ts": "%s"}`,
			id, id, batchNum, time.Now().Format(time.RFC3339))
	}

	produce(t, ctx, router, records...)
	*totalRecords += 10

	t.Logf("Batch %d: wrote 10 records (total: %d) in %v, elapsed: %v",
		batchNum, *totalRecords, time.Since(batchStart), time.Since(startTime))
}
