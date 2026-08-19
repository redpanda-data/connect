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
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/license"
)

// TestIntegration_JiraInput_FirstRunHasNoCursorPredicate exercises the jira
// input end-to-end through a service.StreamBuilder against a mock Jira API.
//
// It asserts that a fresh start (with an empty cache) issues a JQL query that
// does NOT include the `updated >=` cursor predicate. Asserting "restart
// resumes from cursor" against the in-memory cache resource is genuinely
// difficult to express through StreamBuilder (cache state is per-builder), so
// the more rigorous cursor advancement and predicate format coverage lives in
// the unit tests in input_jira_test.go.
func TestIntegration_JiraInput_FirstRunHasNoCursorPredicate(t *testing.T) {
	integration.CheckSkip(t)

	var lastJQL atomic.Value
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/rest/api/3/myself":
			_, _ = w.Write([]byte(`{}`))
		case "/rest/api/3/search/jql":
			lastJQL.Store(r.URL.Query().Get("jql"))
			_, _ = w.Write([]byte(`{"issues":[{"id":"1","key":"PROJ-1","fields":{"project":{"key":"PROJ"},"updated":"2026-06-01T10:00:00.000+0000"}}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	out := make(chan struct{}, 4)
	b := service.NewStreamBuilder()
	require.NoError(t, b.SetLoggerYAML(`level: OFF`))
	require.NoError(t, b.AddCacheYAML(`
label: jira_state
memory: {}
`))
	require.NoError(t, b.AddInputYAML(fmt.Sprintf(`
jira:
  base_url: %q
  auth: {email: u@x, api_token: tok}
  resource: issues
  poll_interval: 10s
  cursor: {cache: jira_state, overlap: 0s}
`, server.URL)))
	require.NoError(t, b.AddConsumerFunc(func(_ context.Context, _ *service.Message) error {
		out <- struct{}{}
		return nil
	}))
	s, err := b.Build()
	require.NoError(t, err)
	license.InjectTestService(s.Resources())

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = s.Run(ctx) }()

	select {
	case <-out:
	case <-ctx.Done():
		t.Fatal("timed out waiting for first message")
	}
	require.NoError(t, s.StopWithin(time.Second))

	jql, _ := lastJQL.Load().(string)
	assert.NotContains(t, jql, "updated >=", "first run should not include cursor predicate")
}
