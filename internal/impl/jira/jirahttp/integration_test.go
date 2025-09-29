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
	"github.com/redpanda-data/connect/v4/internal/impl/jira/helpers/http_helper"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestProcessor_EndToEnd_Issues(t *testing.T) {
	// Fake Jira server with:
	// - /rest/api/3/field/search (custom fields paging)
	// - /rest/api/3/search/jql (issues paging via nextPageToken)
	// Returns:
	//   - custom field "Story Points" => custom_field_10100
	//   - first issues page IsLast=false NextPageToken=tok-2
	//   - second page IsLast=true
	user := "u@example.com"
	token := "Capitoline123"

	var calls struct {
		fieldPages int
		jqlPages   int
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ah := r.Header.Get("Authorization"); ah == "" {
			t.Fatalf("missing Authorization header")
		}
		if !strings.HasPrefix(r.Header.Get("Authorization"), "Basic ") {
			t.Fatalf("expected Basic auth")
		}
		if acc := r.Header.Get("Accept"); !strings.Contains(acc, "application/json") {
			t.Fatalf("expected Accept: application/json header")
		}

		switch r.URL.Path {
		case "/rest/api/3/field/search":
			calls.fieldPages++

			if r.URL.Query().Get("type") != "custom" {
				t.Fatalf("expected type=custom in field search")
			}

			startAt := r.URL.Query().Get("startAt")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)

			// A single page of custom fields is enough for the test (IsLast: true)
			if startAt == "" || startAt == "0" {
				_ = json.NewEncoder(w).Encode(customFieldSearchResponse{
					Fields: []customField{
						{FieldID: "custom_field_10100", FieldName: "Story Points"},
						{FieldID: "custom_field_10022", FieldName: "Sprint"},
					},
					IsLast:     true,
					StartAt:    0,
					MaxResults: 50,
					Total:      2,
				})
				return
			}
			_ = json.NewEncoder(w).Encode(customFieldSearchResponse{
				Fields:     []customField{},
				IsLast:     true,
				StartAt:    0,
				MaxResults: 50,
				Total:      0,
			})
			return

		case "/rest/api/3/search/jql":
			calls.jqlPages++
			q := r.URL.Query()

			// Ensure fields and expand propagate
			if q.Get("fields") == "" {
				t.Fatalf("expected fields param in JQL search")
			}
			if q.Get("maxResults") == "" {
				t.Fatalf("expected maxResults in JQL search")
			}

			// Page 1:
			if q.Get("nextPageToken") == "" {
				_ = json.NewEncoder(w).Encode(searchJQLResponse{
					Issues: []issue{
						{ID: "10001", Key: "DEMO-1", Fields: map[string]any{"summary": "A1"}},
						{ID: "10002", Key: "DEMO-2", Fields: map[string]any{"summary": "A2"}},
					},
					IsLast:        false,
					NextPageToken: "tok-2",
				})
				return
			}

			// Page 2:
			if q.Get("nextPageToken") != "tok-2" {
				t.Fatalf("expected nextPageToken=tok-2, got %q", q.Get("nextPageToken"))
			}
			_ = json.NewEncoder(w).Encode(searchJQLResponse{
				Issues: []issue{
					{ID: "10003", Key: "DEMO-3", Fields: map[string]any{"summary": "A3"}},
				},
				IsLast: true,
			})
			return

		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	j := &JiraProc{
		BaseURL:    srv.URL,
		Username:   user,
		ApiToken:   token,
		MaxResults: 2,
		RetryOpts:  http_helper.RetryOptions{MaxRetries: 0},
		HttpClient: &http.Client{Timeout: 5 * time.Second},
	}

	// Input asks for issues, custom "Story Points" and nested Sprint.name to
	// ensure custom-field mapping and normalization occur.
	in := jsonInputQuery{
		Resource: "issue",
		Project:  "DEMO",
		Fields:   []string{"summary", "Story Points", "Sprint.name"},
	}
	raw, _ := json.Marshal(in)
	msg := service.NewMessage(raw)

	// Execute
	batch, err := j.Process(t.Context(), msg)
	if err != nil {
		t.Fatalf("Process error: %v", err)
	}

	// Assert: 3 issues across 2 pages
	if len(batch) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(batch))
	}

	// Spot-check first message payload and metadata
	b0, _ := batch[0].AsBytes()
	var out0 issueResponse
	if err := json.Unmarshal(b0, &out0); err != nil {
		t.Fatalf("cannot unmarshal issue response: %v", err)
	}
	if out0.Key != "DEMO-1" {
		t.Fatalf("unexpected issue key: %s", out0.Key)
	}

	// Make sure custom fields were passed through normalization/filtering:
	fields0 := out0.Fields.(map[string]any)
	// We expect fields to include "summary" and possibly "changelog" (added by Transform).
	if _, ok := fields0["summary"]; !ok {
		t.Fatalf("expected summary in filtered fields")
	}

	// Assert server interactions
	if calls.fieldPages < 1 {
		t.Fatalf("expected field search to be called at least once")
	}
	if calls.jqlPages != 2 {
		t.Fatalf("expected two JQL pages, got %d", calls.jqlPages)
	}
}

func TestProcessor_EndToEnd_Projects(t *testing.T) {
	user := "u@example.com"
	token := "Capitoline123"

	callsProject := 0
	callsField := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/rest/api/3/field/search":
			// Processor hits this during prepareJiraQuery for custom fields.
			callsField++
			q := r.URL.Query()
			if q.Get("type") == "" {
				// Don’t use t.Fatalf here — it runs in a different goroutine and will cause EOF.
				t.Errorf("field/search missing type=custom, got %v", q)
			}
			// Return a single-page response (IsLast=true) so we don’t paginate.
			_ = json.NewEncoder(w).Encode(customFieldSearchResponse{
				Fields: []customField{
					{FieldID: "custom_field_10100", FieldName: "Story Points"},
				},
				IsLast:     true,
				StartAt:    0,
				MaxResults: 50,
				Total:      1,
			})
			return

		case "/rest/api/3/project/search":
			callsProject++
			q := r.URL.Query()
			if q.Get("maxResults") == "" {
				t.Errorf("project/search missing maxResults")
			}
			// First call: no startAt -> provide NextPage with startAt=2
			if callsProject == 1 {
				_ = json.NewEncoder(w).Encode(projectSearchResponse{
					Projects: []any{
						map[string]any{"id": "P1", "key": "PRJ-1", "name": "project 1"},
						map[string]any{"id": "P2", "key": "PRJ-2", "name": "project 2"},
					},
					IsLast:   false,
					NextPage: "https://" + r.Host + "/rest/api/3/project/search?startAt=2",
				})
				return
			}
			// Second call: expect startAt=2 and finish.
			if q.Get("startAt") != "2" {
				t.Errorf("expected startAt=2, got %q", q.Get("startAt"))
			}
			_ = json.NewEncoder(w).Encode(projectSearchResponse{
				Projects: []any{
					map[string]any{"id": "P3", "key": "PRJ-3", "name": "project 3"},
				},
				IsLast: true,
			})
			return

		default:
			t.Errorf("unexpected path: %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
	}))
	defer srv.Close()

	j := &JiraProc{
		BaseURL:    srv.URL,
		Username:   user,
		ApiToken:   token,
		MaxResults: 2,
		RetryOpts:  http_helper.RetryOptions{MaxRetries: 0},
		HttpClient: &http.Client{Timeout: 5 * time.Second},
	}

	// Input selects projects; include some fields (ok, because handler now supports field/search).
	in := jsonInputQuery{
		Resource: "project",
		Fields:   []string{"key", "name"},
	}
	raw, _ := json.Marshal(in)
	msg := service.NewMessage(raw)

	batch, err := j.Process(t.Context(), msg)
	if err != nil {
		t.Fatalf("Process error: %v", err)
	}

	if len(batch) != 3 {
		t.Fatalf("expected 3 project messages, got %d", len(batch))
	}

	// Validate one payload & metadata
	b0, _ := batch[0].AsBytes()
	var out0 projectResponse
	if err := json.Unmarshal(b0, &out0); err != nil {
		t.Fatalf("cannot unmarshal project response: %v", err)
	}
	if out0.Key != "PRJ-1" {
		t.Fatalf("unexpected project key: %s", out0.Key)
	}

	// Make sure both endpoints were exercised
	if callsField < 1 {
		t.Fatalf("expected field/search to be called at least once")
	}
	if callsProject != 2 {
		t.Fatalf("expected two project search calls, got %d", callsProject)
	}
}
