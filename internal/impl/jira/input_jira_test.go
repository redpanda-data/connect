// Copyright 2026 Redpanda Data, Inc.
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

package jira

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"

	"github.com/redpanda-data/connect/v4/internal/license"
)

// mockJiraServer is a configurable httptest.Server for jira API responses.
type mockJiraServer struct {
	*httptest.Server
	myselfCalls atomic.Int32
	// handler is the route dispatcher; replace per test.
	handler http.HandlerFunc
}

func newMockJiraServer(t *testing.T) *mockJiraServer {
	m := &mockJiraServer{}
	m.handler = func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/rest/api/3/myself":
			m.myselfCalls.Add(1)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"accountId":"abc","emailAddress":"u@x"}`))
		default:
			http.Error(w, "no route in test fixture: "+r.URL.Path, http.StatusInternalServerError)
		}
	}
	m.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.handler(w, r)
	}))
	t.Cleanup(m.Close)
	return m
}

// buildStream wires the jira input into a stream builder with an in-memory cache
// and a transient consumer; returns the assembled stream and a channel of
// received messages (body + metadata) for assertions.
func buildStream(t *testing.T, yaml string) (*service.Stream, <-chan map[string]any) {
	t.Helper()
	out := make(chan map[string]any, 64)
	builder := service.NewStreamBuilder()
	require.NoError(t, builder.SetLoggerYAML(`level: OFF`))
	require.NoError(t, builder.AddCacheYAML(`
label: jira_state
memory: {}
`))
	require.NoError(t, builder.AddInputYAML(yaml))
	require.NoError(t, builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
		b, err := msg.AsBytes()
		if err != nil {
			return err
		}
		md := map[string]any{}
		err = msg.MetaWalkMut(func(k string, v any) error {
			md[k] = v
			return nil
		})
		if err != nil {
			return err
		}
		out <- map[string]any{"body": string(b), "meta": md}
		return nil
	}))
	s, err := builder.Build()
	require.NoError(t, err)
	license.InjectTestService(s.Resources())
	return s, out
}

func TestInputConfig_Parses(t *testing.T) {
	yaml := `
base_url: https://example.atlassian.net
auth:
  email: user@example.com
  api_token: secret
resource: issues
jql: "project = ENG"
poll_interval: 30s
cursor:
  cache: jira_state
`
	confSpec := newJiraInputConfigSpec()
	parsed, err := confSpec.ParseYAML(yaml, nil)
	require.NoError(t, err)

	cfg, err := parseInputConfig(parsed)
	require.NoError(t, err)
	assert.Equal(t, "https://example.atlassian.net", cfg.httpCfg.BaseURL)
	assert.Equal(t, "issues", cfg.resource)
	assert.Equal(t, "user@example.com", cfg.authEmail)
	assert.Equal(t, "secret", cfg.authAPIToken)
	assert.Equal(t, "project = ENG", cfg.jql)
	assert.Equal(t, 30*time.Second, cfg.pollInterval)
	assert.Equal(t, "jira_state", cfg.cacheName)
	assert.Equal(t, "redpanda_connect_jira_input_issues", cfg.cacheKey)
}

func TestInputConfig_RejectsWorklogsResource(t *testing.T) {
	yaml := `
base_url: https://example.atlassian.net
auth:
  email: user@example.com
  api_token: secret
resource: worklogs
cursor:
  cache: jira_state
`
	confSpec := newJiraInputConfigSpec()
	parsed, err := confSpec.ParseYAML(yaml, nil)
	require.NoError(t, err)

	_, err = parseInputConfig(parsed)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "worklogs")
}

func TestCursor_JSONRoundtrip(t *testing.T) {
	original := cursor{Updated: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), Version: 1}
	b, err := json.Marshal(original)
	require.NoError(t, err)
	assert.JSONEq(t, `{"updated":"2026-01-02T03:04:05Z","v":1}`, string(b))

	var decoded cursor
	require.NoError(t, json.Unmarshal(b, &decoded))
	assert.True(t, original.Updated.Equal(decoded.Updated))
	assert.Equal(t, original.Version, decoded.Version)
}

func TestCursor_DecodeIgnoresUnknownFields(t *testing.T) {
	var c cursor
	require.NoError(t, json.Unmarshal([]byte(`{"updated":"2026-01-02T03:04:05Z","v":1,"future_field":"ignored"}`), &c))
	assert.Equal(t, 1, c.Version)
}

func TestCursor_ZeroValueRepresentsFirstRun(t *testing.T) {
	var c cursor
	assert.True(t, c.Updated.IsZero())
	assert.Equal(t, 0, c.Version)
}

func TestRead_EmitsOneMessagePerIssueWithMetadata(t *testing.T) {
	mock := newMockJiraServer(t)
	mock.handler = func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/rest/api/3/myself":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		case "/rest/api/3/search/jql":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{
				"issues":[
					{"id":"10001","key":"PROJ-1","self":"https://x/rest/api/3/issue/10001","fields":{"project":{"key":"PROJ"},"updated":"2026-06-01T10:00:00.000+0000"}},
					{"id":"10002","key":"PROJ-2","self":"https://x/rest/api/3/issue/10002","fields":{"project":{"key":"PROJ"},"updated":"2026-06-01T10:01:00.000+0000"}}
				]
			}`))
		default:
			http.NotFound(w, r)
		}
	}

	yaml := fmt.Sprintf(`
jira:
  base_url: %q
  auth: {email: u@x, api_token: tok}
  resource: issues
  poll_interval: 10s
  cursor: {cache: jira_state}
`, mock.URL)

	s, out := buildStream(t, yaml)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _ = s.Run(ctx) }()

	got := []map[string]any{}
	for len(got) < 2 {
		select {
		case m := <-out:
			got = append(got, m)
		case <-ctx.Done():
			t.Fatalf("timed out; got %d messages", len(got))
		}
	}
	require.NoError(t, s.StopWithin(time.Second))

	assert.Contains(t, got[0]["body"], `"PROJ-1"`)
	meta0 := got[0]["meta"].(map[string]any)
	assert.Equal(t, "PROJ-1", meta0["jira_id"])
	assert.Equal(t, "PROJ", meta0["jira_project"])
	assert.Equal(t, "2026-06-01T10:00:00Z", meta0["jira_updated"])
	assert.Equal(t, "issue", meta0["jira_event_type"])
}

func TestRead_PaginatesAcrossMultiplePages(t *testing.T) {
	mock := newMockJiraServer(t)
	var calls atomic.Int32
	mock.handler = func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/rest/api/3/myself":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		case "/rest/api/3/search/jql":
			n := calls.Add(1)
			token := r.URL.Query().Get("nextPageToken")
			switch n {
			case 1:
				assert.Empty(t, token)
				_, _ = w.Write([]byte(`{"issues":[{"id":"1","key":"PROJ-1","fields":{"project":{"key":"PROJ"},"updated":"2026-06-01T10:00:00.000+0000"}}],"nextPageToken":"page2"}`))
			case 2:
				assert.Equal(t, "page2", token)
				_, _ = w.Write([]byte(`{"issues":[{"id":"2","key":"PROJ-2","fields":{"project":{"key":"PROJ"},"updated":"2026-06-01T10:05:00.000+0000"}}]}`))
			default:
				_, _ = w.Write([]byte(`{"issues":[]}`))
			}
		default:
			http.NotFound(w, r)
		}
	}

	yaml := fmt.Sprintf(`
jira:
  base_url: %q
  auth: {email: u@x, api_token: tok}
  resource: issues
  poll_interval: 10s
  cursor: {cache: jira_state}
`, mock.URL)

	s, out := buildStream(t, yaml)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _ = s.Run(ctx) }()

	keys := []string{}
	for len(keys) < 2 {
		select {
		case m := <-out:
			md := m["meta"].(map[string]any)
			keys = append(keys, md["jira_id"].(string))
		case <-ctx.Done():
			t.Fatalf("only got %v", keys)
		}
	}
	require.NoError(t, s.StopWithin(time.Second))
	assert.Equal(t, []string{"PROJ-1", "PROJ-2"}, keys)
}

func TestCursor_AdvancesAfterPageAcked(t *testing.T) {
	mock := newMockJiraServer(t)
	var calls atomic.Int32
	mock.handler = func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/rest/api/3/myself":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		case "/rest/api/3/search/jql":
			n := calls.Add(1)
			jql := r.URL.Query().Get("jql")
			if n == 1 {
				assert.NotContains(t, jql, "updated >=")
				_, _ = w.Write([]byte(`{"issues":[{"id":"1","key":"PROJ-1","fields":{"project":{"key":"PROJ"},"updated":"2026-06-01T10:00:00.000+0000"}}]}`))
			} else {
				assert.Contains(t, jql, "updated >=")
				_, _ = w.Write([]byte(`{"issues":[]}`))
			}
		}
	}

	yaml := fmt.Sprintf(`
jira:
  base_url: %q
  auth: {email: u@x, api_token: tok}
  resource: issues
  poll_interval: 60s
  cursor: {cache: jira_state, overlap: 0s}
`, mock.URL)

	s, out := buildStream(t, yaml)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _ = s.Run(ctx) }()

	select {
	case <-out:
	case <-ctx.Done():
		t.Fatal("no message")
	}
	require.Eventually(t, func() bool { return calls.Load() >= 2 }, 2*time.Second, 25*time.Millisecond)
	require.NoError(t, s.StopWithin(time.Second))
}

func TestNack_PreventsCursorAdvance(t *testing.T) {
	mock := newMockJiraServer(t)
	var calls atomic.Int32
	var jqlValues atomic.Value
	jqlValues.Store([]string{})
	var jqlMu sync.Mutex
	recordJQL := func(jql string) {
		jqlMu.Lock()
		defer jqlMu.Unlock()
		jqlValues.Store(append(jqlValues.Load().([]string), jql))
	}
	mock.handler = func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/rest/api/3/myself":
			_, _ = w.Write([]byte(`{}`))
		case "/rest/api/3/search/jql":
			n := calls.Add(1)
			recordJQL(r.URL.Query().Get("jql"))
			if n == 1 {
				_, _ = w.Write([]byte(`{"issues":[{"id":"1","key":"PROJ-1","fields":{"project":{"key":"PROJ"},"updated":"2026-06-01T10:00:00.000+0000"}}]}`))
			} else {
				// Subsequent polls return an empty page so the input goes back
				// to its polling sleep instead of spinning. The assertion is
				// purely that a second request fires AND its JQL has no
				// `updated >=` predicate (cursor did not advance).
				_, _ = w.Write([]byte(`{"issues":[]}`))
			}
		default:
			http.NotFound(w, r)
		}
	}

	emitted := make(chan struct{}, 4)
	builder := service.NewStreamBuilder()
	require.NoError(t, builder.SetLoggerYAML(`level: OFF`))
	require.NoError(t, builder.AddCacheYAML(`
label: jira_state
memory: {}
`))
	require.NoError(t, builder.AddInputYAML(fmt.Sprintf(`
jira:
  base_url: %q
  auth: {email: u@x, api_token: tok}
  resource: issues
  poll_interval: 10s
  cursor: {cache: jira_state, overlap: 0s}
  auto_replay_nacks: false
`, mock.URL)))
	var nackOnce atomic.Bool
	require.NoError(t, builder.AddConsumerFunc(func(_ context.Context, _ *service.Message) error {
		emitted <- struct{}{}
		if nackOnce.CompareAndSwap(false, true) {
			return errors.New("forced nack")
		}
		return nil
	}))
	s, err := builder.Build()
	require.NoError(t, err)
	license.InjectTestService(s.Resources())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = s.Run(ctx) }()

	// First emission triggers a nack.
	select {
	case <-emitted:
	case <-ctx.Done():
		t.Fatal("no message emitted")
	}
	// The cursor must NOT advance after the nack: the next JQL must therefore
	// still match the same (zero-cursor) page, so a second request fires with
	// no `updated >=` predicate.
	require.Eventually(t, func() bool { return calls.Load() >= 2 }, 3*time.Second, 25*time.Millisecond)
	require.NoError(t, s.StopWithin(2*time.Second))

	jqls := jqlValues.Load().([]string)
	require.GreaterOrEqual(t, len(jqls), 2, "expected at least 2 JQL requests, got %v", jqls)
	assert.NotContains(t, jqls[0], "updated >=", "first request should have no cursor predicate")
	assert.NotContains(t, jqls[1], "updated >=", "nacked page must not advance the cursor")
}

// TestNack_RestartsPaginationFromCurrentCursor verifies that a nack on a
// multi-page response restarts the pagination run from the current
// (unadvanced) cursor. The mock returns page 1 with nextPageToken=page2; the
// consumer nacks the first message. The next /search/jql request after the
// nack must NOT carry nextPageToken=page2 in its query string — otherwise the
// nacked records would be skipped past forever (the next page-2 ack would
// advance the cursor beyond them).
func TestNack_RestartsPaginationFromCurrentCursor(t *testing.T) {
	mock := newMockJiraServer(t)
	var calls atomic.Int32
	var tokensMu sync.Mutex
	var tokens []string
	recordToken := func(tok string) {
		tokensMu.Lock()
		defer tokensMu.Unlock()
		tokens = append(tokens, tok)
	}
	mock.handler = func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/rest/api/3/myself":
			_, _ = w.Write([]byte(`{}`))
		case "/rest/api/3/search/jql":
			n := calls.Add(1)
			tok := r.URL.Query().Get("nextPageToken")
			recordToken(tok)
			switch n {
			case 1:
				// First request of the (nacked) run: returns page 1 with a
				// nextPageToken so the input would normally fetch page 2.
				_, _ = w.Write([]byte(`{"issues":[{"id":"1","key":"PROJ-1","fields":{"project":{"key":"PROJ"},"updated":"2026-06-01T10:00:00.000+0000"}}],"nextPageToken":"page2"}`))
			default:
				// After the nack we expect a fresh request with NO
				// nextPageToken; serve an empty page so the input idles.
				_, _ = w.Write([]byte(`{"issues":[]}`))
			}
		default:
			http.NotFound(w, r)
		}
	}

	emitted := make(chan struct{}, 4)
	builder := service.NewStreamBuilder()
	require.NoError(t, builder.SetLoggerYAML(`level: OFF`))
	require.NoError(t, builder.AddCacheYAML(`
label: jira_state
memory: {}
`))
	require.NoError(t, builder.AddInputYAML(fmt.Sprintf(`
jira:
  base_url: %q
  auth: {email: u@x, api_token: tok}
  resource: issues
  poll_interval: 10s
  cursor: {cache: jira_state, overlap: 0s}
  auto_replay_nacks: false
`, mock.URL)))
	var nackOnce atomic.Bool
	require.NoError(t, builder.AddConsumerFunc(func(_ context.Context, _ *service.Message) error {
		emitted <- struct{}{}
		if nackOnce.CompareAndSwap(false, true) {
			return errors.New("forced nack")
		}
		return nil
	}))
	s, err := builder.Build()
	require.NoError(t, err)
	license.InjectTestService(s.Resources())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = s.Run(ctx) }()

	// First emission triggers a nack on the only message of page 1.
	select {
	case <-emitted:
	case <-ctx.Done():
		t.Fatal("no message emitted")
	}
	// Wait until at least a second JQL request fires after the nack. The
	// invariant under test is that this second request carries NO
	// nextPageToken — the pagination run restarts from the cursor.
	require.Eventually(t, func() bool { return calls.Load() >= 2 }, 3*time.Second, 25*time.Millisecond)
	require.NoError(t, s.StopWithin(2*time.Second))

	tokensMu.Lock()
	defer tokensMu.Unlock()
	require.GreaterOrEqual(t, len(tokens), 2, "expected at least 2 search/jql requests, got %v", tokens)
	assert.Empty(t, tokens[0], "first request should have no nextPageToken")
	assert.Empty(t, tokens[1], "after nack the next request must NOT reuse nextPageToken=page2")
}

func TestConnect_CallsMyselfAndStartsFromZeroCursor(t *testing.T) {
	mock := newMockJiraServer(t)

	yaml := fmt.Sprintf(`
jira:
  base_url: %q
  auth: {email: u@x, api_token: tok}
  resource: issues
  jql: ""
  poll_interval: 10s
  cursor: {cache: jira_state}
`, mock.URL)

	s, _ := buildStream(t, yaml)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// run a brief stream lifecycle: assert no error connecting; we don't yet emit messages.
	go func() { _ = s.Run(ctx) }()
	require.Eventually(t, func() bool { return mock.myselfCalls.Load() >= 1 }, 1500*time.Millisecond, 25*time.Millisecond)
	require.NoError(t, s.StopWithin(time.Second))
}

// TestResource_Comments verifies that resource=comments performs a per-issue
// child fetch against /rest/api/3/issue/<key>/comment and emits one message
// per comment with metadata populated from the comment payload while still
// referencing the parent issue's key and project.
func TestResource_Comments(t *testing.T) {
	mock := newMockJiraServer(t)
	mock.handler = func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/rest/api/3/myself":
			_, _ = w.Write([]byte(`{}`))
		case "/rest/api/3/search/jql":
			_, _ = w.Write([]byte(`{"issues":[{"id":"10001","key":"PROJ-1","fields":{"project":{"key":"PROJ"},"updated":"2026-06-01T10:00:00.000+0000"}}]}`))
		case "/rest/api/3/issue/PROJ-1/comment":
			_, _ = w.Write([]byte(`{"comments":[
				{"id":"100","self":"https://x/rest/api/3/issue/10001/comment/100","updated":"2026-06-01T10:00:01.000+0000","body":"hi"},
				{"id":"101","self":"https://x/rest/api/3/issue/10001/comment/101","updated":"2026-06-01T10:00:02.000+0000","body":"there"}
			]}`))
		default:
			http.Error(w, r.URL.Path, http.StatusNotFound)
		}
	}

	yaml := fmt.Sprintf(`
jira:
  base_url: %q
  auth: {email: u@x, api_token: tok}
  resource: comments
  poll_interval: 10s
  cursor: {cache: jira_state}
`, mock.URL)

	s, out := buildStream(t, yaml)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _ = s.Run(ctx) }()

	got := []map[string]any{}
	for len(got) < 2 {
		select {
		case m := <-out:
			got = append(got, m)
		case <-ctx.Done():
			t.Fatalf("got only %d", len(got))
		}
	}
	require.NoError(t, s.StopWithin(time.Second))

	meta0 := got[0]["meta"].(map[string]any)
	assert.Equal(t, "100", meta0["jira_id"])
	assert.Equal(t, "PROJ-1", meta0["jira_issue_key"])
	assert.Equal(t, "PROJ", meta0["jira_project"])
	assert.Equal(t, "2026-06-01T10:00:01Z", meta0["jira_updated"])
	assert.Equal(t, "comment", meta0["jira_event_type"])
	assert.Equal(t, "https://x/rest/api/3/issue/10001/comment/100", meta0["jira_self"])

	meta1 := got[1]["meta"].(map[string]any)
	assert.Equal(t, "101", meta1["jira_id"])
	assert.Equal(t, "PROJ-1", meta1["jira_issue_key"])
}

// TestResource_Changelog verifies that when resource=changelog the input
// auto-augments the search URL with expand=changelog and emits one message per
// entry in each issue's changelog.histories[] array, with metadata referencing
// the parent issue.
func TestResource_Changelog(t *testing.T) {
	mock := newMockJiraServer(t)
	mock.handler = func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/rest/api/3/myself" {
			_, _ = w.Write([]byte(`{}`))
			return
		}
		assert.Contains(t, r.URL.Query().Get("expand"), "changelog")
		_, _ = w.Write([]byte(`{"issues":[{
			"id":"1","key":"PROJ-1","self":"https://x/rest/api/3/issue/1",
			"fields":{"project":{"key":"PROJ"},"updated":"2026-06-01T10:00:00.000+0000"},
			"changelog":{"histories":[
				{"id":"500","author":{"emailAddress":"a@x"},"created":"2026-06-01T09:00:00.000+0000","items":[{"field":"status","fromString":"Open","toString":"Done"}]},
				{"id":"501","author":{"emailAddress":"a@x"},"created":"2026-06-01T10:00:00.000+0000","items":[{"field":"assignee"}]}
			]}
		}]}`))
	}

	yaml := fmt.Sprintf(`
jira:
  base_url: %q
  auth: {email: u@x, api_token: tok}
  resource: changelog
  poll_interval: 10s
  cursor: {cache: jira_state}
`, mock.URL)

	s, out := buildStream(t, yaml)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _ = s.Run(ctx) }()

	got := []map[string]any{}
	for len(got) < 2 {
		select {
		case m := <-out:
			got = append(got, m)
		case <-ctx.Done():
			t.Fatalf("only got %d messages", len(got))
		}
	}
	require.NoError(t, s.StopWithin(time.Second))

	ids := []string{}
	for _, m := range got {
		md := m["meta"].(map[string]any)
		ids = append(ids, md["jira_id"].(string))
		assert.Equal(t, "PROJ-1", md["jira_issue_key"])
		assert.Equal(t, "PROJ", md["jira_project"])
		assert.Equal(t, "changelog", md["jira_event_type"])
		// History entries have no `self` of their own; jira_self falls back to
		// the parent issue's self URL so every message satisfies the metadata
		// contract advertised in the input's Description.
		assert.Equal(t, "https://x/rest/api/3/issue/1", md["jira_self"])
	}
	assert.ElementsMatch(t, []string{"500", "501"}, ids)

	// The body of each message should be the raw history entry JSON, including
	// the items[] array so consumers can inspect field-level changes.
	for _, m := range got {
		body := m["body"].(string)
		assert.Contains(t, body, `"items"`)
	}

	// Per-message jira_updated should reflect history.created, not issue.updated.
	for _, m := range got {
		md := m["meta"].(map[string]any)
		switch md["jira_id"].(string) {
		case "500":
			assert.Equal(t, "2026-06-01T09:00:00Z", md["jira_updated"])
		case "501":
			assert.Equal(t, "2026-06-01T10:00:00Z", md["jira_updated"])
		}
	}
}

// capturingLogger is a service.PrintLogger that records every log line so
// tests can assert against the runtime's reaction to a Connect failure.
type capturingLogger struct {
	mu    sync.Mutex
	lines []string
}

func (c *capturingLogger) Printf(format string, v ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lines = append(c.lines, fmt.Sprintf(format, v...))
}

func (c *capturingLogger) Println(v ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lines = append(c.lines, fmt.Sprint(v...))
}

func (c *capturingLogger) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.lines))
	copy(out, c.lines)
	return out
}

// TestConnect_Returns401AsHardError verifies that a 401 from /myself causes
// Connect to fail and the runtime to surface the auth error - the input must
// not emit any messages and the failure must be observable in the logs as a
// 401-flavored error (jirahttp returns it via DropStatuses without retry,
// configured by jiraauth.BuildClient).
func TestConnect_Returns401AsHardError(t *testing.T) {
	mock := newMockJiraServer(t)
	mock.handler = func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}

	yaml := fmt.Sprintf(`
jira:
  base_url: %q
  auth: {email: u@x, api_token: bad}
  resource: issues
  poll_interval: 10s
  cursor: {cache: jira_state}
`, mock.URL)

	logs := &capturingLogger{}
	out := make(chan map[string]any, 4)
	builder := service.NewStreamBuilder()
	builder.SetPrintLogger(logs)
	require.NoError(t, builder.AddCacheYAML(`
label: jira_state
memory: {}
`))
	require.NoError(t, builder.AddInputYAML(yaml))
	require.NoError(t, builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
		b, _ := msg.AsBytes()
		out <- map[string]any{"body": string(b)}
		return nil
	}))
	s, err := builder.Build()
	require.NoError(t, err)
	license.InjectTestService(s.Resources())

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	runErrCh := make(chan error, 1)
	go func() { runErrCh <- s.Run(ctx) }()

	// The 401 must be observable in the logs: jirahttp's HTTPError formats as
	// "http error: status=401 ...", wrapped by Connect as "authenticating with
	// jira: ...". DropStatuses means no retry on the HTTP layer; the runtime
	// will re-call Connect with backoff but every attempt must log the 401.
	require.Eventually(t, func() bool {
		for _, line := range logs.snapshot() {
			if strings.Contains(line, "401") {
				return true
			}
		}
		return false
	}, 1500*time.Millisecond, 25*time.Millisecond, "expected a 401-flavored error to appear in the logs")

	// No messages can possibly have been emitted - Connect never succeeded.
	select {
	case msg := <-out:
		t.Fatalf("input emitted a message despite 401 on /myself: %v", msg)
	default:
	}

	// Stop the stream and assert that, if Run returned non-nil during the wait,
	// the error is 401-flavored. (Connect retries with backoff so Run typically
	// returns only after StopWithin; either way no 401 should be silently
	// swallowed.)
	require.NoError(t, s.StopWithin(2*time.Second))
	cancel()

	select {
	case runErr := <-runErrCh:
		if runErr != nil && !errors.Is(runErr, context.Canceled) && !errors.Is(runErr, context.DeadlineExceeded) {
			assert.Contains(t, runErr.Error(), "401",
				"Run error must reference the 401 status if it propagates")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit after StopWithin")
	}
}

// failingCache implements service.Cache: Get behaves like an empty memory cache,
// Set always returns an error. Used by TestCacheSet_SoftFails to verify the
// input keeps polling when the cursor cache write fails.
type failingCache struct{}

func (failingCache) Get(_ context.Context, _ string) ([]byte, error) {
	return nil, service.ErrKeyNotFound
}

func (failingCache) Set(_ context.Context, _ string, _ []byte, _ *time.Duration) error {
	return errors.New("simulated cache failure")
}

func (failingCache) Add(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	return failingCache{}.Set(ctx, key, value, ttl)
}

func (failingCache) Delete(_ context.Context, _ string) error { return nil }
func (failingCache) Close(_ context.Context) error            { return nil }

func init() {
	_ = service.RegisterCache("failing_cache_for_test", service.NewConfigSpec(),
		func(_ *service.ParsedConfig, _ *service.Resources) (service.Cache, error) {
			return failingCache{}, nil
		})
}

// TestCacheSet_SoftFails asserts that when the cursor cache returns an error
// from Set, the input does not error out the stream and keeps polling.
// The metric increment is exercised via writeCursor but not directly observed
// here (no easy hook on *service.MetricCounter); the assertion is on the
// observable behavior: multiple HTTP polls fire without the stream dying.
func TestCacheSet_SoftFails(t *testing.T) {
	mock := newMockJiraServer(t)
	var pollCount atomic.Int32
	mock.handler = func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/rest/api/3/myself":
			_, _ = w.Write([]byte(`{}`))
		case "/rest/api/3/search/jql":
			pollCount.Add(1)
			_, _ = w.Write([]byte(`{"issues":[{"id":"1","key":"PROJ-1","fields":{"project":{"key":"PROJ"},"updated":"2026-06-01T10:00:00.000+0000"}}]}`))
		default:
			http.Error(w, "no route in test fixture: "+r.URL.Path, http.StatusInternalServerError)
		}
	}

	yaml := fmt.Sprintf(`
jira:
  base_url: %q
  auth: {email: u@x, api_token: tok}
  resource: issues
  poll_interval: 10s
  cursor: {cache: failing_jira_state, overlap: 0s}
`, mock.URL)

	builder := service.NewStreamBuilder()
	require.NoError(t, builder.SetLoggerYAML(`level: OFF`))
	require.NoError(t, builder.AddCacheYAML(`
label: failing_jira_state
failing_cache_for_test: {}
`))
	require.NoError(t, builder.AddInputYAML(yaml))
	require.NoError(t, builder.AddConsumerFunc(func(_ context.Context, _ *service.Message) error { return nil }))
	s, err := builder.Build()
	require.NoError(t, err)
	license.InjectTestService(s.Resources())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	runErrCh := make(chan error, 1)
	go func() { runErrCh <- s.Run(ctx) }()

	// poll_interval is 10s (enforced minimum), but the first two polls fire
	// back-to-back: poll #1 happens immediately on first Read, the page acks
	// quickly (consumer is a no-op), and the subsequent fetchNextPage runs
	// without hitting the inter-poll sleep because the buffer was non-empty.
	require.Eventually(t, func() bool { return pollCount.Load() >= 2 }, 4*time.Second, 25*time.Millisecond,
		"expected multiple polls — Set should soft-fail and not block the input")

	require.NoError(t, s.StopWithin(2*time.Second))
	cancel()

	select {
	case runErr := <-runErrCh:
		if runErr != nil && !errors.Is(runErr, context.Canceled) && !errors.Is(runErr, context.DeadlineExceeded) {
			t.Fatalf("stream Run returned unexpected error: %v", runErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit after StopWithin")
	}
}

// TestRestart_ResumesFromCachedCursor exercises the full restart cycle: the
// first run writes a cursor to a file-backed cache, the second run builds a
// fresh stream pointed at the same cache directory and the first JQL of the
// second run must contain the `updated >=` predicate loaded from the cache.
// This is the only test that validates the spec acceptance criterion "Cursor
// checkpoint survives restarts via cache resource."
func TestRestart_ResumesFromCachedCursor(t *testing.T) {
	mock := newMockJiraServer(t)
	var calls atomic.Int32
	var jqls []string
	var jqlsMu sync.Mutex
	mock.handler = func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/rest/api/3/myself":
			_, _ = w.Write([]byte(`{}`))
		case "/rest/api/3/search/jql":
			n := calls.Add(1)
			jql := r.URL.Query().Get("jql")
			jqlsMu.Lock()
			jqls = append(jqls, jql)
			jqlsMu.Unlock()
			// Return one issue on call 1; empty thereafter so the stream idles
			// and the second run also sees an empty page after its first JQL.
			if n == 1 {
				_, _ = w.Write([]byte(`{"issues":[{"id":"1","key":"PROJ-1","fields":{"project":{"key":"PROJ"},"updated":"2026-06-01T10:00:00.000+0000"}}]}`))
			} else {
				_, _ = w.Write([]byte(`{"issues":[]}`))
			}
		default:
			http.Error(w, "no route in test fixture: "+r.URL.Path, http.StatusInternalServerError)
		}
	}

	cacheDir := t.TempDir()
	inputYAML := fmt.Sprintf(`
jira:
  base_url: %q
  auth: {email: u@x, api_token: tok}
  resource: issues
  poll_interval: 10s
  cursor: {cache: jira_state, overlap: 0s}
`, mock.URL)
	cacheYAML := fmt.Sprintf(`
label: jira_state
file:
  directory: %s
`, cacheDir)

	// run builds a fresh stream wired to the file-backed cache, waits until at
	// least one additional poll fires after the initial fetch (so the cursor
	// has been written to disk via the ack-drain path), then tears down.
	run := func(t *testing.T, minCalls int32) {
		t.Helper()
		out := make(chan struct{}, 1)
		builder := service.NewStreamBuilder()
		require.NoError(t, builder.SetLoggerYAML(`level: OFF`))
		require.NoError(t, builder.AddCacheYAML(cacheYAML))
		require.NoError(t, builder.AddInputYAML(inputYAML))
		require.NoError(t, builder.AddConsumerFunc(func(_ context.Context, _ *service.Message) error {
			select {
			case out <- struct{}{}:
			default:
			}
			return nil
		}))
		s, err := builder.Build()
		require.NoError(t, err)
		license.InjectTestService(s.Resources())
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		go func() { _ = s.Run(ctx) }()
		// Wait until at least minCalls JQL calls have fired - this guarantees
		// the in-flight page has been acked and the cursor flushed to the cache.
		require.Eventually(t, func() bool { return calls.Load() >= minCalls }, 2500*time.Millisecond, 25*time.Millisecond)
		require.NoError(t, s.StopWithin(2*time.Second))
	}

	// Run 1: cache starts empty, first JQL must NOT contain "updated >=".
	run(t, 2)
	callsAfterFirst := calls.Load()
	jqlsMu.Lock()
	require.NotEmpty(t, jqls, "expected at least one JQL recorded after run 1")
	firstJQLOfFirstRun := jqls[0]
	jqlsMu.Unlock()
	assert.NotContains(t, firstJQLOfFirstRun, "updated >=",
		"first JQL of first run must not have cursor predicate (cache is empty)")

	// Run 2 (restart): same cache dir; the cursor written during run 1 must be
	// loaded by Connect and applied to the first JQL of this run.
	run(t, callsAfterFirst+1)
	jqlsMu.Lock()
	require.Greater(t, len(jqls), int(callsAfterFirst), "run 2 did not record a new JQL")
	firstJQLOfSecondRun := jqls[callsAfterFirst]
	jqlsMu.Unlock()
	assert.Contains(t, firstJQLOfSecondRun, "updated >=",
		"first JQL of second run must include cursor predicate loaded from cache")
}

// TestIdle_BoundaryIssueNotReemittedEveryPoll pins the seen-set dedup: an idle
// Jira instance keeps returning the same boundary issue on every poll because
// the cursor predicate is `updated >=`, so without dedup the input would emit
// a duplicate of every boundary issue once per poll interval, forever.
func TestIdle_BoundaryIssueNotReemittedEveryPoll(t *testing.T) {
	mock := newMockJiraServer(t)
	var calls atomic.Int32
	var secondJQL atomic.Value
	mock.handler = func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/rest/api/3/myself":
			_, _ = w.Write([]byte(`{}`))
		case "/rest/api/3/search/jql":
			if calls.Add(1) == 2 {
				secondJQL.Store(r.URL.Query().Get("jql"))
			}
			// Same issue on every poll, exactly as real Jira behaves while
			// no new updates arrive.
			_, _ = w.Write([]byte(`{"issues":[{"id":"1","key":"PROJ-1","fields":{"project":{"key":"PROJ"},"updated":"2026-06-01T10:00:00.000+0000"}}]}`))
		default:
			http.NotFound(w, r)
		}
	}

	yaml := fmt.Sprintf(`
jira:
  base_url: %q
  auth: {email: u@x, api_token: tok}
  resource: issues
  poll_interval: 10s
  cursor: {cache: jira_state, overlap: 0s}
`, mock.URL)

	s, out := buildStream(t, yaml)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = s.Run(ctx) }()

	select {
	case <-out:
	case <-ctx.Done():
		t.Fatal("no first message")
	}
	// The second poll fires immediately (page 1 was non-empty); its response
	// contains the same issue version, which must be suppressed.
	require.Eventually(t, func() bool { return calls.Load() >= 2 }, 3*time.Second, 25*time.Millisecond)
	select {
	case m := <-out:
		t.Fatalf("boundary issue re-emitted on idle poll: %v", m)
	case <-time.After(300 * time.Millisecond):
	}
	require.NoError(t, s.StopWithin(2*time.Second))

	jql, _ := secondJQL.Load().(string)
	assert.Contains(t, jql, "updated >=", "second poll must carry the cursor predicate")
}

// TestUpdatedIssue_IsReemitted is the counterpart to the idle dedup test: when
// a boundary issue is genuinely updated again (newer `updated` timestamp), the
// new version must be emitted despite the issue key being in the seen set.
func TestUpdatedIssue_IsReemitted(t *testing.T) {
	mock := newMockJiraServer(t)
	var calls atomic.Int32
	mock.handler = func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/rest/api/3/myself":
			_, _ = w.Write([]byte(`{}`))
		case "/rest/api/3/search/jql":
			if calls.Add(1) == 1 {
				_, _ = w.Write([]byte(`{"issues":[{"id":"1","key":"PROJ-1","fields":{"project":{"key":"PROJ"},"updated":"2026-06-01T10:00:00.000+0000"}}]}`))
			} else {
				_, _ = w.Write([]byte(`{"issues":[{"id":"1","key":"PROJ-1","fields":{"project":{"key":"PROJ"},"updated":"2026-06-01T10:05:00.000+0000"}}]}`))
			}
		default:
			http.NotFound(w, r)
		}
	}

	yaml := fmt.Sprintf(`
jira:
  base_url: %q
  auth: {email: u@x, api_token: tok}
  resource: issues
  poll_interval: 10s
  cursor: {cache: jira_state, overlap: 0s}
`, mock.URL)

	s, out := buildStream(t, yaml)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = s.Run(ctx) }()

	updated := []string{}
	for len(updated) < 2 {
		select {
		case m := <-out:
			md := m["meta"].(map[string]any)
			updated = append(updated, md["jira_updated"].(string))
		case <-ctx.Done():
			t.Fatalf("only got %d messages: %v", len(updated), updated)
		}
	}
	require.NoError(t, s.StopWithin(2*time.Second))
	assert.Equal(t, []string{"2026-06-01T10:00:00Z", "2026-06-01T10:05:00Z"}, updated)
}

// TestCursor_PersistedAfterEachAckedPage pins mid-backfill restartability: the
// cursor checkpoint must be written to the cache after page 1 is acked and
// BEFORE the page-2 request is issued, so a restart mid-run resumes from the
// last acked page instead of the beginning of the backfill. The write and the
// next fetch happen on the same goroutine, so observing the cache from the
// page-2 handler is deterministic.
func TestCursor_PersistedAfterEachAckedPage(t *testing.T) {
	mock := newMockJiraServer(t)
	cacheDir := t.TempDir()
	var midRunCursor atomic.Value
	mock.handler = func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/rest/api/3/myself":
			_, _ = w.Write([]byte(`{}`))
		case "/rest/api/3/search/jql":
			if r.URL.Query().Get("nextPageToken") == "page2" {
				var content strings.Builder
				entries, _ := os.ReadDir(cacheDir)
				for _, e := range entries {
					b, _ := os.ReadFile(filepath.Join(cacheDir, e.Name()))
					content.Write(b)
				}
				midRunCursor.Store(content.String())
				_, _ = w.Write([]byte(`{"issues":[{"id":"2","key":"PROJ-2","fields":{"project":{"key":"PROJ"},"updated":"2026-06-01T10:05:00.000+0000"}}]}`))
				return
			}
			_, _ = w.Write([]byte(`{"issues":[{"id":"1","key":"PROJ-1","fields":{"project":{"key":"PROJ"},"updated":"2026-06-01T10:00:00.000+0000"}}],"nextPageToken":"page2"}`))
		default:
			http.NotFound(w, r)
		}
	}

	builder := service.NewStreamBuilder()
	require.NoError(t, builder.SetLoggerYAML(`level: OFF`))
	require.NoError(t, builder.AddCacheYAML(fmt.Sprintf(`
label: jira_state
file:
  directory: %s
`, cacheDir)))
	require.NoError(t, builder.AddInputYAML(fmt.Sprintf(`
jira:
  base_url: %q
  auth: {email: u@x, api_token: tok}
  resource: issues
  poll_interval: 10s
  cursor: {cache: jira_state, overlap: 0s}
`, mock.URL)))
	got := make(chan string, 8)
	require.NoError(t, builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
		id, _ := msg.MetaGet("jira_id")
		got <- id
		return nil
	}))
	s, err := builder.Build()
	require.NoError(t, err)
	license.InjectTestService(s.Resources())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = s.Run(ctx) }()

	keys := []string{}
	for len(keys) < 2 {
		select {
		case k := <-got:
			keys = append(keys, k)
		case <-ctx.Done():
			t.Fatalf("only got %v", keys)
		}
	}
	require.NoError(t, s.StopWithin(2*time.Second))

	assert.Equal(t, []string{"PROJ-1", "PROJ-2"}, keys)
	cur, _ := midRunCursor.Load().(string)
	require.NotEmpty(t, cur, "cursor must be on disk before the page-2 request is issued")
	assert.Contains(t, cur, "2026-06-01T10:00:00Z", "mid-run checkpoint must carry page 1's max updated")
	assert.Contains(t, cur, "PROJ-1", "mid-run checkpoint must carry page 1's seen entries")
}

func TestPruneSeen(t *testing.T) {
	now := time.Date(2026, 6, 1, 10, 10, 0, 0, time.UTC)
	seen := map[string]time.Time{
		"OLD-1":  now.Add(-10 * time.Minute),
		"EDGE-1": now.Add(-90 * time.Second), // inside cursor - overlap(1m) - 1m slack
		"NEW-1":  now,
	}
	pruneSeen(seen, now, time.Minute)
	assert.NotContains(t, seen, "OLD-1")
	assert.Contains(t, seen, "EDGE-1")
	assert.Contains(t, seen, "NEW-1")

	// A zero cursor (first run) must not prune anything.
	seen2 := map[string]time.Time{"K-1": now}
	pruneSeen(seen2, time.Time{}, time.Minute)
	assert.Contains(t, seen2, "K-1")
}

func TestCursor_SeenRoundtripAndV1Compat(t *testing.T) {
	orig := cursor{
		Updated: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		Seen:    map[string]time.Time{"PROJ-1": time.Date(2026, 1, 2, 3, 4, 0, 0, time.UTC)},
		Version: cursorSchemaVersion,
	}
	b, err := json.Marshal(orig)
	require.NoError(t, err)
	var decoded cursor
	require.NoError(t, json.Unmarshal(b, &decoded))
	assert.True(t, orig.Updated.Equal(decoded.Updated))
	require.Contains(t, decoded.Seen, "PROJ-1")
	assert.True(t, orig.Seen["PROJ-1"].Equal(decoded.Seen["PROJ-1"]))

	// A v1 cursor (no seen field) must decode cleanly with a nil seen map.
	var v1 cursor
	require.NoError(t, json.Unmarshal([]byte(`{"updated":"2026-01-02T03:04:05Z","v":1}`), &v1))
	assert.Nil(t, v1.Seen)
	assert.Equal(t, 1, v1.Version)
}
