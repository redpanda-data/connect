// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package notion

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"

	"github.com/redpanda-data/connect/v4/internal/license"
)

func TestGetUsersProcessor(t *testing.T) {
	usersResponse := map[string]any{
		"object": "list",
		"results": []any{
			map[string]any{
				"object":     "user",
				"id":         "6794760a-1f15-45cd-9c65-0dfe42f5135a",
				"name":       "Aman Gupta",
				"avatar_url": nil,
				"type":       "person",
				"person": map[string]any{
					"email": "aman@example.com",
				},
			},
			map[string]any{
				"object":     "user",
				"id":         "92a680bb-6970-4726-952b-4f4c03bff617",
				"name":       "Test Bot",
				"avatar_url": nil,
				"type":       "bot",
				"bot": map[string]any{
					"owner": map[string]any{
						"type":      "workspace",
						"workspace": true,
					},
				},
			},
		},
		"next_cursor": nil,
		"has_more":    false,
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/users", r.URL.Path)
		assert.Equal(t, http.MethodGet, r.Method)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		assert.NoError(t, json.NewEncoder(w).Encode(usersResponse))
	}))
	t.Cleanup(srv.Close)

	captured := runNotionPipeline(t, fmt.Sprintf(`
notion:
  - label: test_notion
    api_key: "test-secret-key"
    notion_version: "2022-06-28"
    base_url: "%s"

input:
  generate:
    count: 1
    interval: 1ms
    mapping: 'root = {}'
  processors:
    - notion_get_users:
        client: test_notion

output:
  drop: {}

logger:
  level: INFO
`, srv.URL))

	require.Len(t, captured, 1)

	result, ok := captured[0].(map[string]any)
	require.True(t, ok, "expected map, got %T", captured[0])

	envelope, ok := result["response"].(map[string]any)
	require.True(t, ok, "expected response envelope")

	assert.Equal(t, 200, envelope["status_code"])

	body, ok := envelope["body"].(map[string]any)
	require.True(t, ok, "expected body to be a map")

	results, ok := body["results"].([]any)
	require.True(t, ok, "expected results to be a list")
	assert.Len(t, results, 2)

	firstUser, ok := results[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Aman Gupta", firstUser["name"])
	assert.Equal(t, "person", firstUser["type"])

	secondUser, ok := results[1].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Test Bot", secondUser["name"])
	assert.Equal(t, "bot", secondUser["type"])
}

func TestPostSearchWithMappings(t *testing.T) {
	searchResponse := map[string]any{
		"object": "list",
		"results": []any{
			map[string]any{
				"object": "page",
				"id":     "aaa-bbb-ccc",
				"properties": map[string]any{
					"title": map[string]any{
						"type": "title",
						"title": []any{
							map[string]any{"plain_text": "Meeting Notes"},
						},
					},
				},
			},
		},
		"next_cursor": nil,
		"has_more":    false,
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/search", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		assert.NoError(t, json.NewEncoder(w).Encode(searchResponse))
	}))
	t.Cleanup(srv.Close)

	// request_mapping: transforms input {"q":"meeting"} into valid PostSearchReq {"query":"meeting"}
	// response_mapping: extracts just the results array from the response envelope
	captured := runNotionPipeline(t, fmt.Sprintf(`
notion:
  - label: test_notion
    api_key: "test-secret-key"
    base_url: "%s"

input:
  generate:
    count: 1
    interval: 1ms
    mapping: 'root.q = "meeting"'
  processors:
    - notion_post_search:
        client: test_notion
        request_mapping: 'root.query = this.q'
        response_mapping: 'root = this.response.body.results'

output:
  drop: {}

logger:
  level: INFO
`, srv.URL))

	require.Len(t, captured, 1)

	results, ok := captured[0].([]any)
	require.True(t, ok, "expected array after response_mapping, got %T", captured[0])
	require.Len(t, results, 1)

	page, ok := results[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "page", page["object"])
	assert.Equal(t, "aaa-bbb-ccc", page["id"])
}

// runNotionPipeline builds and runs a stream from the given YAML config,
// capturing all output messages. It injects an enterprise test license and
// blocks until the stream completes or the timeout expires.
func runNotionPipeline(t *testing.T, yaml string) []any {
	t.Helper()

	env := service.GlobalEnvironment()
	b := env.NewStreamBuilder()
	b.SetSchema(env.FullConfigSchema("", ""))
	require.NoError(t, b.SetYAML(yaml))

	var captured []any
	require.NoError(t, b.AddConsumerFunc(func(_ context.Context, m *service.Message) error {
		s, err := m.AsStructured()
		if err != nil {
			return err
		}
		captured = append(captured, s)
		return nil
	}))

	stream, err := b.Build()
	require.NoError(t, err)

	license.InjectTestService(stream.Resources())

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	require.NoError(t, stream.Run(ctx))

	return captured
}
