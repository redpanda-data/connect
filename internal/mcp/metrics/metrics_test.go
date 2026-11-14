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

package metrics_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/mcp/metrics"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	_ "github.com/redpanda-data/connect/v4/public/components/prometheus"
)

func TestToolMetricsCollapseUnknownToolNames(t *testing.T) {
	mux := http.NewServeMux()
	builder := service.NewResourceBuilder()
	builder.SetHTTPMux(mux)
	require.NoError(t, builder.SetMetricsYAML(`prometheus: {}`))
	res, closeFn, err := builder.Build()
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := closeFn(context.Background()); err != nil {
			t.Log(err)
		}
	})

	m := metrics.NewMetrics(res.Metrics(), func(name string) bool { return name == "echo" })
	handler := m.ReceivingMiddleware(func(context.Context, string, mcp.Request) (mcp.Result, error) {
		return &mcp.CallToolResult{}, nil
	})
	for _, name := range []string{"echo", "made-up-1", "made-up-2"} {
		_, err := handler(t.Context(), "tools/call", &mcp.CallToolRequest{
			Params: &mcp.CallToolParamsRaw{Name: name},
		})
		require.NoError(t, err)
	}

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()

	assert.Contains(t, body, `mcp_tool_invocations_total{status="success",tool_name="echo"} 1`)
	assert.Contains(t, body, `mcp_tool_invocations_total{status="success",tool_name="unknown"} 2`)
	assert.NotContains(t, body, "made-up")
}
