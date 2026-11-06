// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package jirahttp

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newRolesTestServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	return httptest.NewServer(handler)
}

func newRolesTestJiraHttp(server *httptest.Server) *Client {
	return &Client{
		baseURL:    server.URL,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

func TestSearchRoles_Success(t *testing.T) {
	srv := newRolesTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/role") {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[
			{"id": 1, "name": "Developers"},
			{"id": 2, "name": "Administrators"}
		]`))
	})
	defer srv.Close()

	j := newRolesTestJiraHttp(srv)

	got, err := j.searchRoles(t.Context())
	if err != nil {
		t.Fatalf("searchRoles returned error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 roles, got %d", len(got))
	}
}

func TestSearchRoles_InvalidJSON(t *testing.T) {
	srv := newRolesTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/role") {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{ this is not valid json ]`))
	})
	defer srv.Close()

	j := newRolesTestJiraHttp(srv)

	_, err := j.searchRoles(t.Context())
	if err == nil {
		t.Fatalf("expected error on invalid JSON, got nil")
	}
}

func TestSearchRolesResource_NoRoles(t *testing.T) {
	// Return an empty array to test the early-exit branch in searchRolesResource.
	srv := newRolesTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/role") {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	})
	defer srv.Close()

	j := newRolesTestJiraHttp(srv)

	// Minimal input query: no fields trigger a basic path without filtering.
	q := &JsonInputQuery{
		Fields: nil,
	}

	batch, err := j.SearchRolesResource(t.Context(), q, map[string]string{})
	if err != nil {
		t.Fatalf("searchRolesResource returned error: %v", err)
	}
	if len(batch) != 0 {
		t.Fatalf("expected empty batch when no roles returned, got %d", len(batch))
	}
}
