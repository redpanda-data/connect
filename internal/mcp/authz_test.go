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

package mcp_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/common-go/authz"

	"github.com/redpanda-data/connect/v4/internal/gateway"
	mcpinternal "github.com/redpanda-data/connect/v4/internal/mcp"
)

const (
	authzTestServer    authz.ResourceName = "organizations/acme/resourcegroups/default/dataplanes/dp/mcpservers/srv"
	authzTestPrincipal authz.PrincipalID  = "User:alice@example.com"
)

var authzTestTools = []string{"read_orders", "read_users", "write_orders"}

// newTestAuthorizer creates an authorizer whose policy lets the test principal
// connect and list tools on the server, plus a tools/call binding on each of
// the given scopes (relative to the server).
func newTestAuthorizer(t *testing.T, callScopes ...string) (*mcpinternal.Authorizer, *bytes.Buffer) {
	t.Helper()

	var policy strings.Builder
	fmt.Fprintf(&policy, `
roles:
  - id: connect
    permissions:
      - dataplane_mcpserver_initialize
      - dataplane_mcpserver_tools_list
  - id: caller
    permissions:
      - dataplane_mcpserver_tools_call
bindings:
  - role: connect
    principal: %s
    scope: %s
`, authzTestPrincipal, authzTestServer)
	for _, scope := range callScopes {
		fmt.Fprintf(&policy, "  - role: caller\n    principal: %s\n    scope: %s%s\n",
			authzTestPrincipal, authzTestServer, scope)
	}

	file := filepath.Join(t.TempDir(), "policy.yaml")
	require.NoError(t, os.WriteFile(file, []byte(policy.String()), 0o644))

	var logs bytes.Buffer
	auth, err := mcpinternal.NewAuthorizer(authzTestServer, file, slog.New(slog.NewTextHandler(&logs, nil)))
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := auth.Close(); err != nil {
			t.Log(err)
		}
	})
	return auth, &logs
}

func authzTestHandler(auth *mcpinternal.Authorizer) mcp.MethodHandler {
	return auth.Middleware(func(_ context.Context, method string, _ mcp.Request) (mcp.Result, error) {
		if method != "tools/list" {
			return &mcp.CallToolResult{}, nil
		}
		res := &mcp.ListToolsResult{}
		for _, name := range authzTestTools {
			res.Tools = append(res.Tools, &mcp.Tool{Name: name})
		}
		return res, nil
	})
}

func callToolRequest(name string) *mcp.CallToolRequest {
	return &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Name: name}}
}

func TestAuthorizerPerToolAccess(t *testing.T) {
	tests := []struct {
		name       string
		callScopes []string
		allowed    []string
	}{
		{
			name:       "server binding grants every tool",
			callScopes: []string{""},
			allowed:    authzTestTools,
		},
		{
			name:       "tool binding grants only that tool",
			callScopes: []string{"/tools/read_orders"},
			allowed:    []string{"read_orders"},
		},
		{
			name:       "wildcard tool binding grants matching tools",
			callScopes: []string{"/tools/read_*"},
			allowed:    []string{"read_orders", "read_users"},
		},
		{
			name:    "no tools/call binding grants nothing",
			allowed: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			auth, _ := newTestAuthorizer(t, test.callScopes...)
			handler := authzTestHandler(auth)
			ctx := gateway.ContextWithValidatedPrincipalID(t.Context(), authzTestPrincipal)

			res, err := handler(ctx, "tools/list", &mcp.ListToolsRequest{Params: &mcp.ListToolsParams{}})
			require.NoError(t, err)
			list, ok := res.(*mcp.ListToolsResult)
			require.True(t, ok)
			var listed []string
			for _, tool := range list.Tools {
				listed = append(listed, tool.Name)
			}
			assert.ElementsMatch(t, test.allowed, listed)

			for _, tool := range authzTestTools {
				_, err := handler(ctx, "tools/call", callToolRequest(tool))
				if slices.Contains(test.allowed, tool) {
					assert.NoError(t, err, tool)
				} else {
					assert.ErrorContains(t, err, "permission denied", tool)
				}
			}
		})
	}
}

func TestAuthorizerLogsDenials(t *testing.T) {
	auth, logs := newTestAuthorizer(t, "/tools/read_orders")
	handler := authzTestHandler(auth)
	ctx := gateway.ContextWithValidatedPrincipalID(t.Context(), authzTestPrincipal)

	_, err := handler(ctx, "tools/call", callToolRequest("write_orders"))
	require.ErrorContains(t, err, "permission denied")
	assert.Contains(t, logs.String(), `msg="Authorization denied"`)
	assert.Contains(t, logs.String(), "principal=User:alice@example.com")
	assert.Contains(t, logs.String(), "permission=dataplane_mcpserver_tools_call")
	assert.Contains(t, logs.String(), "resource_type=tools resource_id=write_orders")
	assert.Contains(t, logs.String(), "reason=forbidden")

	logs.Reset()
	_, err = handler(ctx, "tools/call", callToolRequest(""))
	require.ErrorContains(t, err, "permission denied")
	assert.Contains(t, logs.String(), "reason=empty_tool_name")

	// Notifications have no permission mapping: denied, but not a policy
	// decision worth logging.
	logs.Reset()
	_, err = handler(ctx, "notifications/initialized", &mcp.InitializedRequest{Params: &mcp.InitializedParams{}})
	require.ErrorContains(t, err, "permission denied")
	assert.Empty(t, logs.String())
}
