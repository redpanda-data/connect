// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSubmitConnector_PUT(t *testing.T) {
	var gotMethod string
	var gotURL string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotURL = r.URL.Path
		json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"name":"bench_x","config":{}}`))
	}))
	defer srv.Close()

	cfg := map[string]any{"connector.class": "x"}
	if err := SubmitConnector(context.Background(), srv.URL, "bench_x", cfg); err != nil {
		t.Fatalf("SubmitConnector: %v", err)
	}
	if gotMethod != "PUT" {
		t.Errorf("method = %s, want PUT", gotMethod)
	}
	if gotURL != "/connectors/bench_x/config" {
		t.Errorf("URL = %s, want /connectors/bench_x/config", gotURL)
	}
	if gotBody["connector.class"] != "x" {
		t.Errorf("body class = %v", gotBody["connector.class"])
	}
}

func TestWaitConnectorRunning_HappyPath(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			// First call: not yet
			w.Write([]byte(`{"connector":{"state":"UNASSIGNED"},"tasks":[]}`))
			return
		}
		// Subsequent calls: ready
		w.Write([]byte(`{"connector":{"state":"RUNNING"},"tasks":[{"id":0,"state":"RUNNING"}]}`))
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := WaitConnectorRunning(ctx, srv.URL, "bench_x", 50*time.Millisecond); err != nil {
		t.Fatalf("WaitConnectorRunning: %v", err)
	}
	if atomic.LoadInt32(&calls) < 2 {
		t.Errorf("expected at least 2 status polls; got %d", calls)
	}
}

func TestWaitConnectorRunning_TaskFailed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"connector":{"state":"RUNNING"},"tasks":[{"id":0,"state":"FAILED","trace":"boom"}]}`))
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err := WaitConnectorRunning(ctx, srv.URL, "bench_x", 50*time.Millisecond)
	if err == nil {
		t.Fatal("expected error when a task is FAILED")
	}
	if !strings.Contains(err.Error(), "FAILED") {
		t.Errorf("error should mention FAILED state; got %v", err)
	}
}

func TestDeleteConnector_404IsOK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	// DELETE on a non-existent connector should be a no-op (idempotent reset).
	if err := DeleteConnector(context.Background(), srv.URL, "missing"); err != nil {
		t.Errorf("DeleteConnector on 404 should succeed; got %v", err)
	}
}

func TestDeleteConnector_204IsOK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	if err := DeleteConnector(context.Background(), srv.URL, "bench_x"); err != nil {
		t.Errorf("DeleteConnector on 204 should succeed; got %v", err)
	}
}
