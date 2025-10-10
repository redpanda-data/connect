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

func TestSearchAllIssues_PaginatesAndAggregates(t *testing.T) {
	// Arrange a fake Jira API with two pages using nextPageToken.
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++

		if r.URL.Path != "/rest/api/3/search/jql" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}

		// Ensure maxResults is set by the client
		if r.URL.Query().Get("maxResults") == "" {
			t.Fatalf("missing maxResults query param")
		}

		switch callCount {
		case 1:
			// First page, no nextPageToken -> respond with IsLast:false and NextPageToken
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(SearchJQLResponse{
				Issues: []Issue{
					{ID: "1", Key: "DEMO-1"},
					{ID: "2", Key: "DEMO-2"},
				},
				IsLast:        false,
				NextPageToken: "token-2",
			})
		case 2:
			// Second page must include nextPageToken
			if r.URL.Query().Get("nextPageToken") != "token-2" {
				t.Fatalf("expected nextPageToken=token-2, got %q", r.URL.Query().Get("nextPageToken"))
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(SearchJQLResponse{
				Issues: []Issue{
					{ID: "3", Key: "DEMO-3"},
				},
				IsLast: true,
			})
		default:
			t.Fatalf("unexpected extra call #%d", callCount)
		}
	}))
	defer srv.Close()

	// Build a minimal jiraProc with our test server and short timeouts.
	j := &Client{
		baseURL:    srv.URL,
		username:   "u",
		apiToken:   "t",
		maxResults: 2,
		httpClient: srv.Client(),
		retryOpts:  jira_helper.RetryOptions{MaxRetries: 0},
	}

	// Act
	ctx := t.Context()
	params := map[string]string{
		"jql":    "project = DEMO",
		"fields": "summary,status",
	}
	all, err := j.searchAllIssues(ctx, params)
	if err != nil {
		t.Fatalf("searchAllIssues error: %v", err)
	}

	// Assert
	if len(all) != 3 {
		t.Fatalf("expected 3 issues, got %d", len(all))
	}
	if all[0].Key != "DEMO-1" || all[2].Key != "DEMO-3" {
		t.Fatalf("unexpected issue keys: %+v", all)
	}
}

func TestSearchIssuesPage_SendsExpectedQueryParams(t *testing.T) {
	seen := struct {
		maxResults    string
		nextPageToken string
	}{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rest/api/3/search/jql" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		q := r.URL.Query()
		seen.maxResults = q.Get("maxResults")
		seen.nextPageToken = q.Get("nextPageToken")

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(SearchJQLResponse{IsLast: true})
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
	params := map[string]string{"jql": "project = DEMO"}
	_, err := j.searchIssuesPage(ctx, params, "nxt-123")
	if err != nil {
		t.Fatalf("searchIssuesPage error: %v", err)
	}

	if seen.maxResults != "50" {
		t.Fatalf("expected maxResults=50, got %q", seen.maxResults)
	}
	if seen.nextPageToken != "nxt-123" {
		t.Fatalf("expected nextPageToken=nxt-123, got %q", seen.nextPageToken)
	}
}

func TestSearchIssuesPage_PropagatesParams(t *testing.T) {
	var got url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.URL.Query()
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(SearchJQLResponse{IsLast: true})
	}))
	defer srv.Close()

	j := &Client{
		baseURL:    srv.URL,
		username:   "u",
		apiToken:   "t",
		maxResults: 10,
		httpClient: srv.Client(),
		retryOpts:  jira_helper.RetryOptions{MaxRetries: 0},
	}

	ctx := t.Context()
	params := map[string]string{
		"jql":    "project = DEMO and updated > -1d",
		"fields": "summary,status",
		"expand": "changelog",
	}
	if _, err := j.searchIssuesPage(ctx, params, ""); err != nil {
		t.Fatalf("searchIssuesPage error: %v", err)
	}

	if got.Get("jql") == "" || got.Get("fields") == "" || got.Get("expand") == "" {
		t.Fatalf("expected jql/fields/expand to propagate, got: %v", got)
	}
	if _, err := strconv.Atoi(got.Get("maxResults")); err != nil {
		t.Fatalf("expected numeric maxResults, got %q", got.Get("maxResults"))
	}
}
