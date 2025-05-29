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

package tools_test

import (
	"context"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/mcp/tools"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
)

type discardHandler struct{}

func (discardHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (discardHandler) Handle(context.Context, slog.Record) error { return nil }
func (dh discardHandler) WithAttrs([]slog.Attr) slog.Handler     { return dh }
func (dh discardHandler) WithGroup(string) slog.Handler          { return dh }

func TestResourcesWrappersCacheHappy(t *testing.T) {
	s := server.NewMCPServer("Testing", "1.0.0")

	r := tools.NewResourcesWrapper(slog.New(discardHandler{}), s, nil, nil)

	require.NoError(t, r.AddCacheYAML([]byte(`
label: foocache
memory: {}
meta:
  mcp:
    enabled: true
    description: my foo cache
`)))

	require.NoError(t, r.AddCacheYAML([]byte(`
label: barcache
memory: {}
meta:
  mcp:
    enabled: false
`)))

	require.NoError(t, r.AddCacheYAML([]byte(`
label: bazcache
memory: {}
`)))

	res, err := r.Build()
	require.NoError(t, err)

	ctx, done := context.WithTimeout(t.Context(), time.Minute)
	defer done()

	toolsList, ok := s.HandleMessage(ctx, []byte(`{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/list"
}`)).(mcp.JSONRPCResponse)
	require.True(t, ok)

	tools := toolsList.Result.(mcp.ListToolsResult).Tools
	assert.Len(t, tools, 2)

	assert.Equal(t, "get-foocache", tools[0].Name)
	assert.Contains(t, tools[0].Description, "my foo cache")

	assert.Equal(t, "set-foocache", tools[1].Name)
	assert.Contains(t, tools[1].Description, "my foo cache")

	assert.True(t, res.HasCache("bazcache"))

	defer r.Close(ctx)
}

func TestResourcesWrappersTagFiltering(t *testing.T) {
	s := server.NewMCPServer("Testing", "1.0.0")

	r := tools.NewResourcesWrapper(slog.New(discardHandler{}), s, nil, func(tags []string) bool {
		if slices.Contains(tags, "foo") || slices.Contains(tags, "bar") {
			return true
		}
		return false
	})

	require.NoError(t, r.AddCacheYAML([]byte(`
label: foocache
memory: {}
meta:
  mcp:
    enabled: true
    description: my foo cache
`)))

	require.NoError(t, r.AddCacheYAML([]byte(`
label: barcache
memory: {}
meta:
  tags: [ bar ]
  mcp:
    enabled: true
    description: my bar cache
`)))

	require.NoError(t, r.AddCacheYAML([]byte(`
label: bazcache
memory: {}
`)))

	require.NoError(t, r.AddCacheYAML([]byte(`
label: buzcache
memory: {}
meta:
  tags: [ nope, foo ]
`)))

	res, err := r.Build()
	require.NoError(t, err)

	ctx, done := context.WithTimeout(t.Context(), time.Minute)
	defer done()

	toolsList, ok := s.HandleMessage(ctx, []byte(`{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/list"
}`)).(mcp.JSONRPCResponse)
	require.True(t, ok)

	tools := toolsList.Result.(mcp.ListToolsResult).Tools
	assert.Len(t, tools, 2)

	assert.Equal(t, "get-barcache", tools[0].Name)
	assert.Contains(t, tools[0].Description, "my bar cache")

	assert.Equal(t, "set-barcache", tools[1].Name)
	assert.Contains(t, tools[1].Description, "my bar cache")

	assert.False(t, res.HasCache("bazcache"))
	assert.True(t, res.HasCache("buzcache"))

	defer r.Close(ctx)
}
