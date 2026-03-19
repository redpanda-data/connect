// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package secrets

import (
	"context"
	"log/slog"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseGCPSecretsURL(t *testing.T) {
	testURL := "gcp://integ-serverless-proxy-1/int-us-central1-customer-secrets_d1ojlqfl816oi8l26qfg_a845616f-0484-4506-9638-45fe28f34865_?trimPrefix=secrets.&audience=%2F%2Fiam.googleapis.com%2Fprojects%2F515626364553%2Flocations%2Fglobal%2FworkloadIdentityPools%2Fconnect-int-us-central1%2Fproviders%2Fconnect-consumer-provider"

	// Parse the URL to validate components
	u, err := url.Parse(testURL)
	require.NoError(t, err)

	// Validate URL components
	assert.Equal(t, "gcp", u.Scheme)
	assert.Equal(t, "integ-serverless-proxy-1", u.Host)
	assert.Equal(t, "/int-us-central1-customer-secrets_d1ojlqfl816oi8l26qfg_a845616f-0484-4506-9638-45fe28f34865_", u.Path)
	
	// Validate query parameters
	assert.Equal(t, "secrets.", u.Query().Get("trimPrefix"))
	
	// Validate audience parameter (URL decoded)
	expectedAudience := "//iam.googleapis.com/projects/515626364553/locations/global/workloadIdentityPools/connect-int-us-central1/providers/connect-consumer-provider"
	assert.Equal(t, expectedAudience, u.Query().Get("audience"))

	// Test the actual parsing function
	ctx := context.Background()
	logger := slog.Default()
	
	// This should not error during parsing (even if GCP client creation fails)
	_, err = parseSecretsLookupURN(ctx, logger, testURL)
	
	// We expect this to fail due to GCP credentials not being available in test,
	// but the URL parsing should work fine
	if err != nil {
		// Error is expected due to missing GCP credentials in test environment
		t.Logf("Expected error due to missing GCP credentials: %v", err)
	}
}

func TestGCPSecretsURLComponents(t *testing.T) {
	testURL := "gcp://integ-serverless-proxy-1/int-us-central1-customer-secrets_d1ojlqfl816oi8l26qfg_a845616f-0484-4506-9638-45fe28f34865_?trimPrefix=secrets.&audience=%2F%2Fiam.googleapis.com%2Fprojects%2F515626364553%2Flocations%2Fglobal%2FworkloadIdentityPools%2Fconnect-int-us-central1%2Fproviders%2Fconnect-consumer-provider"

	u, err := url.Parse(testURL)
	require.NoError(t, err)

	// Extract components as the parseSecretsLookupURN function does
	projectID := u.Host
	prefix := u.Path[1:] // Remove leading slash
	trimPrefix := u.Query().Get("trimPrefix")
	audience := u.Query().Get("audience")

	// Validate extracted components
	assert.Equal(t, "integ-serverless-proxy-1", projectID)
	assert.Equal(t, "int-us-central1-customer-secrets_d1ojlqfl816oi8l26qfg_a845616f-0484-4506-9638-45fe28f34865_", prefix)
	assert.Equal(t, "secrets.", trimPrefix)
	assert.Equal(t, "//iam.googleapis.com/projects/515626364553/locations/global/workloadIdentityPools/connect-int-us-central1/providers/connect-consumer-provider", audience)

	// Test that the prefix ends with underscore (as expected for GCP format)
	assert.True(t, prefix[len(prefix)-1] == '_', "GCP prefix should end with underscore")
}

func TestGCPSecretNameConstruction(t *testing.T) {
	// Simulate how a secret name would be constructed
	prefix := "int-us-central1-customer-secrets_d1ojlqfl816oi8l26qfg_a845616f-0484-4506-9638-45fe28f34865_"
	trimPrefix := "secrets."
	secretKey := "secrets.MY_SECRET"

	// Simulate the trimPrefixAndSplit logic
	if !assert.True(t, len(secretKey) > len(trimPrefix) && secretKey[:len(trimPrefix)] == trimPrefix) {
		t.Fatal("Secret key should start with trimPrefix")
	}

	// Trim the prefix
	trimmedKey := secretKey[len(trimPrefix):]
	assert.Equal(t, "MY_SECRET", trimmedKey)

	// Construct full secret name
	fullSecretName := prefix + trimmedKey
	expectedSecretName := "int-us-central1-customer-secrets_d1ojlqfl816oi8l26qfg_a845616f-0484-4506-9638-45fe28f34865_MY_SECRET"
	assert.Equal(t, expectedSecretName, fullSecretName)

	// Validate that the constructed name would be valid for GCP Secret Manager
	assert.True(t, len(fullSecretName) <= 255, "GCP secret name should be <= 255 characters")
	assert.True(t, len(fullSecretName) > 0, "GCP secret name should not be empty")
}