// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package polaris

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/redpanda-data/benthos/v4/public/service"

	icebergimpl "github.com/redpanda-data/connect/v4/internal/impl/iceberg"
	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/catalogx"
)

var (
	storageAccount = flag.String("polaris.storage-account", "", "Azure storage account name")
	accessKey      = flag.String("polaris.access-key", "", "Azure storage account access key")
	container      = flag.String("polaris.container", "", "Azure storage container name")
	tenantID       = flag.String("polaris.tenant-id", "", "Azure tenant ID")
	spClientID     = flag.String("polaris.sp-client-id", "", "Service principal client ID for Polaris")
	spClientSecret = flag.String("polaris.sp-client-secret", "", "Service principal client secret for Polaris")
)

func skipIfNotConfigured(t *testing.T) {
	t.Helper()
	if *storageAccount == "" || *accessKey == "" || *container == "" || *tenantID == "" || *spClientID == "" || *spClientSecret == "" {
		t.Skip("set -polaris.storage-account, -polaris.access-key, -polaris.container, -polaris.tenant-id, -polaris.sp-client-id, -polaris.sp-client-secret flags to run Polaris e2e tests")
	}
}

func startPolaris(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	ctr, err := testcontainers.Run(ctx, "apache/polaris:latest",
		testcontainers.WithExposedPorts("8181/tcp", "8182/tcp"),
		testcontainers.WithEnv(map[string]string{
			"POLARIS_BOOTSTRAP_CREDENTIALS": "POLARIS,root,secret",
			"AZURE_TENANT_ID":               *tenantID,
			"AZURE_CLIENT_ID":               *spClientID,
			"AZURE_CLIENT_SECRET":           *spClientSecret,
		}),
		testcontainers.WithWaitStrategy(
			wait.ForHTTP("/q/health/ready").WithPort("8182/tcp"),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ctr.Terminate(ctx)) })

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

func createPolarisCatalog(t *testing.T, polarisURL, token, catalogName, warehouseLocation, tenantID string) {
	t.Helper()
	polarisHTTP(t, "POST", polarisURL+"/api/management/v1/catalogs", token, map[string]any{
		"catalog": map[string]any{
			"name": catalogName,
			"type": "INTERNAL",
			"properties": map[string]string{
				"default-base-location": warehouseLocation,
			},
			"storageConfigInfo": map[string]any{
				"storageType":      "AZURE",
				"allowedLocations": []string{warehouseLocation},
				"tenantId":         tenantID,
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
		AdditionalProps: iceberg.Properties{
			iceio.AdlsSharedKeyAccountName: *storageAccount,
			iceio.AdlsSharedKeyAccountKey:  *accessKey,
		},
	}
}

func newRouter(t *testing.T, catalogCfg catalogx.Config, namespace, table string, schemaEvo bool) *icebergimpl.Router {
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
		Enabled: schemaEvo,
	}
	router := icebergimpl.NewRouter(catalogCfg, namespaceStr, tableStr, schemaEvoCfg, commitCfg, logger)
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

func adlsCleanup(t *testing.T, storageAcct, key, ctr, prefix string) {
	t.Helper()
	cred, err := azblob.NewSharedKeyCredential(storageAcct, key)
	if err != nil {
		t.Logf("warning: failed to create ADLS credential: %v", err)
		return
	}

	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net", storageAcct)
	client, err := azblob.NewClientWithSharedKeyCredential(serviceURL, cred, nil)
	if err != nil {
		t.Logf("warning: failed to create ADLS client: %v", err)
		return
	}

	ctx := context.Background()
	// Collect all blob paths, then delete deepest-first (required for HNS/ADLS Gen2)
	var paths []string
	pager := client.NewListBlobsFlatPager(ctr, &azblob.ListBlobsFlatOptions{
		Prefix: &prefix,
	})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			t.Logf("warning: failed to list blobs: %v", err)
			return
		}
		for _, blob := range page.Segment.BlobItems {
			paths = append(paths, *blob.Name)
		}
	}
	// Sort by length descending so leaf files are deleted before parent directories
	sort.Slice(paths, func(i, j int) bool { return len(paths[i]) > len(paths[j]) })
	for _, p := range paths {
		if _, err := client.DeleteBlob(ctx, ctr, p, nil); err != nil {
			t.Logf("warning: failed to delete blob %s: %v", p, err)
		}
	}
}

func TestPolarisE2E_BasicWrite(t *testing.T) {
	skipIfNotConfigured(t)

	ctx := context.Background()
	polarisURL := startPolaris(t)
	token := getOAuth2Token(t, polarisURL)

	catalogName := fmt.Sprintf("catalog_%d", time.Now().UnixNano())
	warehouseLocation := fmt.Sprintf("abfss://%s@%s.dfs.core.windows.net/", *container, *storageAccount)
	createPolarisCatalog(t, polarisURL, token, catalogName, warehouseLocation, *tenantID)
	grantCatalogAccess(t, polarisURL, token, catalogName)

	catalogCfg := buildCatalogConfig(polarisURL, catalogName)
	namespace := "e2e_ns"

	// Create namespace
	client, err := catalogx.NewCatalogClient(catalogCfg, []string{namespace})
	require.NoError(t, err)
	defer client.Close()
	require.NoError(t, client.CreateNamespace(ctx, nil))

	tableName := fmt.Sprintf("e2e_basic_%d", time.Now().UnixNano())
	t.Cleanup(func() { adlsCleanup(t, *storageAccount, *accessKey, *container, namespace+"/"+tableName) })

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

func TestPolarisE2E_SchemaEvolution(t *testing.T) {
	skipIfNotConfigured(t)

	ctx := context.Background()
	polarisURL := startPolaris(t)
	token := getOAuth2Token(t, polarisURL)

	catalogName := fmt.Sprintf("catalog_%d", time.Now().UnixNano())
	warehouseLocation := fmt.Sprintf("abfss://%s@%s.dfs.core.windows.net/", *container, *storageAccount)
	createPolarisCatalog(t, polarisURL, token, catalogName, warehouseLocation, *tenantID)
	grantCatalogAccess(t, polarisURL, token, catalogName)

	catalogCfg := buildCatalogConfig(polarisURL, catalogName)
	namespace := "e2e_ns"

	// Create namespace
	client, err := catalogx.NewCatalogClient(catalogCfg, []string{namespace})
	require.NoError(t, err)
	defer client.Close()
	require.NoError(t, client.CreateNamespace(ctx, nil))

	tableName := fmt.Sprintf("e2e_schema_evo_%d", time.Now().UnixNano())
	t.Cleanup(func() { adlsCleanup(t, *storageAccount, *accessKey, *container, namespace+"/"+tableName) })

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
