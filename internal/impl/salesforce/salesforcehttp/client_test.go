// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package salesforcehttp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateAndSetBearerToken_RealClient(t *testing.T) {
	t.Parallel()

	// Fake Salesforce OAuth server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/services/oauth2/token", r.URL.Path)

		resp := map[string]string{"access_token": "abc123"}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer ts.Close()

	client, err := NewClient(
		ts.URL,
		"id",
		"secret",
		"v65.0",
		1,
		2000,
		ts.Client(),
		nil,
		nil,
	)
	require.NoError(t, err)

	err = client.updateAndSetBearerToken(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "abc123", client.getBearerToken())
}

func TestCallSalesforceApi_RefreshOn401_RealClient(t *testing.T) {
	t.Parallel()

	callCount := 0
	tokenIssued := false

	// Fake Salesforce server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {

		// Token refresh endpoint
		case "/services/oauth2/token":
			tokenIssued = true
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"access_token":"new-token"}`))
			return

		// Data endpoint
		case "/services/data/v65.0":
			callCount++

			// First call → 401 Unauthorized
			if callCount == 1 {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}

			// Second call → success
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))
			return

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	client, err := NewClient(
		ts.URL,
		"id",
		"secret",
		"v65.0",
		1,
		2000,
		ts.Client(),
		nil,
		nil,
	)
	require.NoError(t, err)

	body, err := client.callSalesforceAPI(context.Background(), mustParseURL(ts.URL+"/services/data/v65.0"))
	require.NoError(t, err)

	assert.Equal(t, `{"ok":true}`, string(body))
	assert.Equal(t, 2, callCount)
	assert.True(t, tokenIssued, "token refresh should have been called")
}

func mustParseURL(s string) *url.URL {
	u, _ := url.Parse(s)
	return u
}
