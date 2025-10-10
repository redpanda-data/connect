// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jirahttp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/redpanda-data/connect/v4/internal/impl/jira/helpers/jira_helper"
)

func TestSearchAllProjects_PaginatesViaStartAt(t *testing.T) {
	// First page returns IsLast:false and a NextPage URL that includes startAt=2
	call := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call++
		if r.URL.Path != "/rest/api/3/project/search" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}

		switch call {
		case 1:
			// startAt omitted or 0 on first call
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(ProjectSearchResponse{
				Projects: []any{
					map[string]any{"id": "P1", "key": "PRJ-1"},
					map[string]any{"id": "P2", "key": "PRJ-2"},
				},
				IsLast:   false,
				NextPage: "https://" + r.Host + "/rest/api/3/project/search?startAt=2",
			})
		case 2:
			// Verify the client passes startAt=2
			if r.URL.Query().Get("startAt") != "2" {
				t.Fatalf("expected startAt=2, got %q", r.URL.Query().Get("startAt"))
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(ProjectSearchResponse{
				Projects: []any{
					map[string]any{"id": "P3", "key": "PRJ-3"},
				},
				IsLast: true,
			})
		default:
			t.Fatalf("unexpected extra call %d", call)
		}
	}))
	defer srv.Close()

	j := &Client{
		baseURL:    srv.URL,
		username:   "u",
		apiToken:   "t",
		maxResults: 2,
		httpClient: srv.Client(),
		retryOpts:  jira_helper.RetryOptions{MaxRetries: 0},
	}

	ctx := t.Context()
	params := map[string]string{"fields": "key,name"}
	projects, err := j.searchAllProjects(ctx, params)
	if err != nil {
		t.Fatalf("searchAllProjects error: %v", err)
	}
	if len(projects) != 3 {
		t.Fatalf("expected 3 projects, got %d", len(projects))
	}
}

func TestSearchProjectsPage_SendsParamsAndMaxResults(t *testing.T) {
	var got url.Values

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rest/api/3/project/search" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		got = r.URL.Query()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(ProjectSearchResponse{IsLast: true})
	}))
	defer srv.Close()

	j := &Client{
		baseURL:    srv.URL,
		username:   "u",
		apiToken:   "t",
		maxResults: 50,
		httpClient: srv.Client(),
		retryOpts:  jira_helper.RetryOptions{MaxRetries: 0},
	}

	ctx := t.Context()
	params := map[string]string{
		"fields": "id,key,name",
	}
	if _, err := j.searchProjectsPage(ctx, params, 10); err != nil {
		t.Fatalf("searchProjectsPage error: %v", err)
	}

	if got.Get("fields") != "id,key,name" {
		t.Fatalf("expected fields to propagate, got %q", got.Get("fields"))
	}
	if got.Get("startAt") != "10" {
		t.Fatalf("expected startAt=10, got %q", got.Get("startAt"))
	}
	if got.Get("maxResults") != "50" {
		t.Fatalf("expected maxResults=50, got %q", got.Get("maxResults"))
	}
	if _, err := strconv.Atoi(got.Get("maxResults")); err != nil {
		t.Fatalf("expected numeric maxResults, got %q", got.Get("maxResults"))
	}
}
