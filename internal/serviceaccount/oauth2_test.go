// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package serviceaccount

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetTokenSourceBeforeInit(t *testing.T) {
	// Reset global state
	globalConfigMu.Lock()
	globalConfig = nil
	globalConfigMu.Unlock()

	_, err := GetTokenSource()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "service account authentication has not been set up")
}

func TestGetHTTPClientBeforeInit(t *testing.T) {
	// Reset global state
	globalConfigMu.Lock()
	globalConfig = nil
	globalConfigMu.Unlock()

	_, err := GetHTTPClient()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "service account authentication has not been set up")
}

func TestInitGlobalWithMissingCredentials(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name         string
		tokenURL     string
		clientID     string
		clientSecret string
	}{
		{"missing tokenURL", "", "client", "secret"},
		{"missing clientID", "http://token", "", "secret"},
		{"missing clientSecret", "http://token", "client", ""},
		{"all missing", "", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := InitGlobal(ctx, tt.tokenURL, tt.clientID, tt.clientSecret, "")
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "tokenURL, clientID, and clientSecret are required")
		})
	}
}

func TestInitGlobalAndRetrieve(t *testing.T) {
	// Create a mock OAuth2 server
	tokenCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			tokenCount++
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"access_token":"test-token","token_type":"Bearer","expires_in":3600}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	ctx := context.Background()

	// Initialize global config
	err := InitGlobal(ctx, server.URL+"/token", "test-client", "test-secret", "test-audience")
	require.NoError(t, err)
	assert.Greater(t, tokenCount, 0, "should have called token endpoint during init")

	// Test GetTokenSource
	tokenSource, err := GetTokenSource()
	require.NoError(t, err)
	assert.NotNil(t, tokenSource)

	// Test token retrieval
	token, err := tokenSource.Token()
	require.NoError(t, err)
	assert.Equal(t, "test-token", token.AccessToken)

	// Test GetHTTPClient
	httpClient, err := GetHTTPClient()
	require.NoError(t, err)
	assert.NotNil(t, httpClient)
}

func TestInitGlobalWithInvalidTokenURL(t *testing.T) {
	ctx := context.Background()

	err := InitGlobal(ctx, "http://invalid-host-that-does-not-exist.local/token", "client", "secret", "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to acquire OAuth2 token")
}
