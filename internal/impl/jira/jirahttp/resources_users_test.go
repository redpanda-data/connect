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
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// newTestServer wraps httptest.NewServer for convenience.
func newUsersTestServer(t *testing.T, h http.HandlerFunc) *httptest.Server {
	t.Helper()
	return httptest.NewServer(h)
}

// newTestJiraProc creates a minimal JiraProc configured to use the provided server.
func newUsersJiraProc(srv *httptest.Server, maxResults int) *JiraProc {
	return &JiraProc{
		BaseURL:    srv.URL,
		HttpClient: &http.Client{Timeout: 10 * time.Second},
		MaxResults: maxResults,
		// other fields of jiraProc are not required for these tests
	}
}

func TestSearchUsersPage_SendsParamsAndParses(t *testing.T) {
	srv := newUsersTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		// Endpoint shape tolerance: look for /users/search
		if !strings.Contains(r.URL.Path, "/users/search") {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}

		// Validate maxResults reflect j.maxResults
		if got := r.URL.Query().Get("maxResults"); got != "5" {
			t.Fatalf("expected maxResults=5, got %q", got)
		}
		// Respond with a small array payload
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"accountId":"u1"},{"accountId":"u2"}]`))
	})
	defer srv.Close()

	j := newUsersJiraProc(srv, 5)

	ctx := t.Context()
	qp := map[string]string{"query": "alice"}
	users, err := j.searchUsersPage(ctx, qp, 0)
	if err != nil {
		t.Fatalf("searchUsersPage error: %v", err)
	}
	if len(users) != 2 {
		t.Fatalf("expected 2 users, got %d", len(users))
	}
}

func TestSearchUsersPage_WithStartAt(t *testing.T) {
	srv := newUsersTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/users/search") {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		// Ensure startAt is propagated when non-zero
		if got := r.URL.Query().Get("startAt"); got != "3" {
			t.Fatalf("expected startAt=3, got %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"accountId":"u4"},{"accountId":"u5"}]`))
	})
	defer srv.Close()

	j := newUsersJiraProc(srv, 2)

	ctx := t.Context()
	users, err := j.searchUsersPage(ctx, map[string]string{}, 3)
	if err != nil {
		t.Fatalf("searchUsersPage error: %v", err)
	}
	if len(users) != 2 {
		t.Fatalf("expected 2 users, got %d", len(users))
	}
}

func TestSearchAllUsers_PaginatesUntilEmpty(t *testing.T) {
	// Emulate pagination: when startAt is absent or 0 -> 2 users, startAt=2 -> 1 user, startAt=3 -> empty
	srv := newUsersTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/users/search") {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		startAt := r.URL.Query().Get("startAt")
		w.Header().Set("Content-Type", "application/json")
		switch startAt {
		case "":
			_, _ = w.Write([]byte(`[{"accountId":"u1"},{"accountId":"u2"}]`))
		case "2":
			_, _ = w.Write([]byte(`[{"accountId":"u3"}]`))
		case "3":
			_, _ = w.Write([]byte(`[]`))
		default:
			t.Fatalf("unexpected startAt: %s", startAt)
		}
	})
	defer srv.Close()

	j := newUsersJiraProc(srv, 2)

	ctx := t.Context()
	got, err := j.searchAllUsers(ctx, map[string]string{"query": "any"})
	if err != nil {
		t.Fatalf("searchAllUsers error: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 aggregated users, got %d", len(got))
	}
}

func TestSearchUsersResource_EmptyBatchWhenNoUsers(t *testing.T) {
	srv := newUsersTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/users/search") {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		// Return empty page immediately
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	})
	defer srv.Close()

	j := newUsersJiraProc(srv, 50)

	q := &jsonInputQuery{
		Fields: []string{},
	}
	batch, err := j.searchUsersResource(t.Context(), q, map[string]string{}, map[string]string{})
	if err != nil {
		t.Fatalf("searchUsersResource error: %v", err)
	}
	if len(batch) != 0 {
		t.Fatalf("expected empty batch, got %d", len(batch))
	}
}

func TestSearchUsersPage_PropagatesQueryParams(t *testing.T) {
	// Ensure arbitrary query params are forwarded
	srv := newUsersTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/users/search") {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if q := r.URL.Query().Get("query"); q != "alice" {
			t.Fatalf("expected query=alice, got %q", q)
		}
		if l := r.URL.Query().Get("limit"); l != "10" {
			t.Fatalf("expected limit=10, got %q", l)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"accountId":"u1"}]`))
	})
	defer srv.Close()

	j := newUsersJiraProc(srv, 1)
	ctx := t.Context()
	users, err := j.searchUsersPage(ctx, map[string]string{
		"query": "alice",
		"limit": "10",
	}, 0)
	if err != nil {
		t.Fatalf("searchUsersPage error: %v", err)
	}
	if len(users) != 1 {
		t.Fatalf("expected 1 user, got %d", len(users))
	}
}

// Optional: sanity-check that startAt increments by page length in searchAllUsers.
func TestSearchAllUsers_StartAtIncrementsByPageSize(t *testing.T) {
	var calls []int
	srv := newUsersTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/users/search") {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		startAtStr := r.URL.Query().Get("startAt")
		if startAtStr == "" {
			calls = append(calls, 0)
			_, _ = w.Write([]byte(`[{"accountId":"u1"},{"accountId":"u2"}]`))
			return
		}
		startAt, _ := strconv.Atoi(startAtStr)
		calls = append(calls, startAt)
		switch startAt {
		case 2:
			_, _ = w.Write([]byte(`[{"accountId":"u3"},{"accountId":"u4"}]`))
		case 4:
			_, _ = w.Write([]byte(`[]`))
		default:
			t.Fatalf("unexpected startAt: %d", startAt)
		}
	})
	defer srv.Close()

	j := newUsersJiraProc(srv, 2)

	_, err := j.searchAllUsers(t.Context(), nil)
	if err != nil {
		t.Fatalf("searchAllUsers error: %v", err)
	}
	// Expected call sequence: first (no startAt), then 2, then 4
	want := []int{0, 2, 4}
	if len(calls) != len(want) {
		t.Fatalf("unexpected number of calls, got %v", calls)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("call %d: expected startAt=%d, got %d", i, want[i], calls[i])
		}
	}
}
