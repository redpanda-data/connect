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

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xeipuuv/gojsonschema"

	"github.com/redpanda-data/connect/v4/internal/mcp/tools"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
)

type discardHandler struct{}

func (discardHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (discardHandler) Handle(context.Context, slog.Record) error { return nil }
func (dh discardHandler) WithAttrs([]slog.Attr) slog.Handler     { return dh }
func (dh discardHandler) WithGroup(string) slog.Handler          { return dh }

func TestResourcesWrappersCacheHappy(t *testing.T) {
	s := mcp.NewServer(&mcp.Implementation{
		Name:    "Testing",
		Version: "1.0.0",
	}, nil)

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

	// Use in-memory transport to test
	serverTransport, clientTransport := mcp.NewInMemoryTransports()

	// Start server in background
	go func() {
		_ = s.Run(ctx, serverTransport)
	}()

	// Connect client
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client"}, nil)
	session, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer session.Close()

	// List tools
	result, err := session.ListTools(ctx, &mcp.ListToolsParams{})
	require.NoError(t, err)

	assert.Len(t, result.Tools, 2)
	assert.Equal(t, "get-foocache", result.Tools[0].Name)
	assert.Contains(t, result.Tools[0].Description, "my foo cache")
	assert.Equal(t, "set-foocache", result.Tools[1].Name)
	assert.Contains(t, result.Tools[1].Description, "my foo cache")

	assert.True(t, res.HasCache("bazcache"))

	defer r.Close(ctx)
}

func TestResourcesWrappersTagFiltering(t *testing.T) {
	s := mcp.NewServer(&mcp.Implementation{
		Name:    "Testing",
		Version: "1.0.0",
	}, nil)

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

	// Use in-memory transport to test
	serverTransport, clientTransport := mcp.NewInMemoryTransports()

	// Start server in background
	go func() {
		_ = s.Run(ctx, serverTransport)
	}()

	// Connect client
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client"}, nil)
	session, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer session.Close()

	// List tools
	result, err := session.ListTools(ctx, &mcp.ListToolsParams{})
	require.NoError(t, err)

	assert.Len(t, result.Tools, 2)
	assert.Equal(t, "get-barcache", result.Tools[0].Name)
	assert.Contains(t, result.Tools[0].Description, "my bar cache")
	assert.Equal(t, "set-barcache", result.Tools[1].Name)
	assert.Contains(t, result.Tools[1].Description, "my bar cache")

	assert.False(t, res.HasCache("bazcache"))
	assert.True(t, res.HasCache("buzcache"))

	defer r.Close(ctx)
}

func TestOutputSchemaDefaultProps(t *testing.T) {
	s := mcp.NewServer(&mcp.Implementation{
		Name:    "Testing",
		Version: "1.0.0",
	}, nil)

	r := tools.NewResourcesWrapper(slog.New(discardHandler{}), s, nil, nil)

	require.NoError(t, r.AddOutputYAML([]byte(`
label: foooutput
drop: {}
meta:
  mcp:
    enabled: true
    description: my foo output
`)))

	_, err := r.Build()
	require.NoError(t, err)

	ctx, done := context.WithTimeout(t.Context(), time.Minute)
	defer done()

	// Use in-memory transport to test
	serverTransport, clientTransport := mcp.NewInMemoryTransports()

	// Start server in background
	go func() {
		_ = s.Run(ctx, serverTransport)
	}()

	// Connect client
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client"}, nil)
	session, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer session.Close()

	// List tools
	result, err := session.ListTools(ctx, &mcp.ListToolsParams{})
	require.NoError(t, err)
	require.Len(t, result.Tools, 1)

	tool := result.Tools[0]
	assert.Equal(t, "foooutput", tool.Name)
	assert.Contains(t, tool.Description, "my foo output")

	_, err = gojsonschema.NewSchemaLoader().Compile(gojsonschema.NewGoLoader(tool.InputSchema))
	require.NoError(t, err)

	defer r.Close(ctx)
}

func TestOutputSchemaCustomProps(t *testing.T) {
	s := mcp.NewServer(&mcp.Implementation{
		Name:    "Testing",
		Version: "1.0.0",
	}, nil)

	r := tools.NewResourcesWrapper(slog.New(discardHandler{}), s, nil, nil)

	require.NoError(t, r.AddOutputYAML([]byte(`
label: baroutput
drop: {}
meta:
  mcp:
    enabled: true
    properties:
      - name: topic_name
        type: string
        required: true
        description: "The topic name"

      - name: content
        type: string
        description: "The content"
        required: true
    description: my bar output
`)))

	_, err := r.Build()
	require.NoError(t, err)

	ctx, done := context.WithTimeout(t.Context(), time.Minute)
	defer done()

	// Use in-memory transport to test
	serverTransport, clientTransport := mcp.NewInMemoryTransports()

	// Start server in background
	go func() {
		_ = s.Run(ctx, serverTransport)
	}()

	// Connect client
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client"}, nil)
	session, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer session.Close()

	// List tools
	result, err := session.ListTools(ctx, &mcp.ListToolsParams{})
	require.NoError(t, err)
	require.Len(t, result.Tools, 1)

	tool := result.Tools[0]
	assert.Equal(t, "baroutput", tool.Name)
	assert.Contains(t, tool.Description, "my bar output")

	_, err = gojsonschema.NewSchemaLoader().Compile(gojsonschema.NewGoLoader(tool.InputSchema))
	require.NoError(t, err)

	defer r.Close(ctx)
}
