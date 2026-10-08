// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mcp

import (
	"context"
	"errors"
	"log/slog"
	"slices"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.opentelemetry.io/otel/trace"

	"github.com/redpanda-data/common-go/authz"
	"github.com/redpanda-data/connect/v4/internal/gateway"
)

const (
	permissionInitialize             authz.PermissionName = "dataplane_mcpserver_initialize"
	permissionPing                   authz.PermissionName = "dataplane_mcpserver_ping"
	permissionResourcesList          authz.PermissionName = "dataplane_mcpserver_resources_list"
	permissionResourcesTemplatesList authz.PermissionName = "dataplane_mcpserver_resources_templates_list"
	permissionResourcesRead          authz.PermissionName = "dataplane_mcpserver_resources_read"
	permissionPromptsList            authz.PermissionName = "dataplane_mcpserver_prompts_list"
	permissionPromptsGet             authz.PermissionName = "dataplane_mcpserver_prompts_get"
	permissionToolsList              authz.PermissionName = "dataplane_mcpserver_tools_list"
	permissionToolsCall              authz.PermissionName = "dataplane_mcpserver_tools_call"
	permissionLoggingSetLevel        authz.PermissionName = "dataplane_mcpserver_logging_set_level"
)

// toolResourceType is the resource type of tools within an MCP server, so a
// tool's resource name is <server>/tools/<tool name>.
const toolResourceType authz.ResourceType = "tools"

var errPermissionDenied = errors.New("permission denied")

var allPermissions = []authz.PermissionName{
	permissionInitialize,
	permissionPing,
	permissionResourcesList,
	permissionResourcesTemplatesList,
	permissionResourcesRead,
	permissionPromptsList,
	permissionPromptsGet,
	permissionToolsList,
	permissionToolsCall,
	permissionLoggingSetLevel,
}

var methodToPerm = map[string]authz.PermissionName{
	"initialize":               permissionInitialize,
	"ping":                     permissionPing,
	"resources/list":           permissionResourcesList,
	"resources/templates/list": permissionResourcesTemplatesList,
	"resources/read":           permissionResourcesRead,
	"prompts/list":             permissionPromptsList,
	"prompts/get":              permissionPromptsGet,
	"tools/list":               permissionToolsList,
	"tools/call":               permissionToolsCall,
	"logging/setLevel":         permissionLoggingSetLevel,
}

// NewAuthorizer returns an MCP server authorizer which dynamically loads
// (and watches) the policy file for policy enforcement.
func NewAuthorizer(name authz.ResourceName, file string, logger *slog.Logger) (*Authorizer, error) {
	notifyError := func(err error) {
		logger.Warn("authorization policy error", "err", err)
	}
	policy, err := gateway.NewFileWatchingAuthzResourcePolicy(name, file, allPermissions, notifyError)
	if err != nil {
		return nil, err
	}
	return &Authorizer{policy: policy, logger: logger}, nil
}

// NewAuthorizerFromEndpoint returns an MCP server authorizer which streams
// policy updates from a gRPC policy-materializer endpoint.
func NewAuthorizerFromEndpoint(name authz.ResourceName, endpoint string, logger *slog.Logger) (*Authorizer, error) {
	notifyError := func(err error) {
		logger.Warn("authorization policy error", "err", err)
	}
	policy, err := gateway.NewEndpointWatchingAuthzResourcePolicy(name, endpoint, allPermissions, notifyError)
	if err != nil {
		return nil, err
	}
	return &Authorizer{policy: policy, logger: logger}, nil
}

// Authorizer provides middleware for enforcing authorization policies on MCP method calls.
type Authorizer struct {
	policy *gateway.FileWatchingAuthzResourcePolicy
	logger *slog.Logger
}

// Middleware returns an MCP method handler that enforces authorization checks before invoking the next handler.
//
// Tool calls are authorized against the called tool as a sub-resource of the server
// (<server>/tools/<name>), so a policy can grant access to individual tools,
// and tools/list results only include the tools the principal may call.
// Bindings on the server itself apply to all of its tools. A wildcard binding such as
// <server>/tools/read_* also covers matching tools added later, so what it grants can
// widen without the policy changing.
func (a *Authorizer) Middleware(next mcp.MethodHandler) mcp.MethodHandler {
	return func(ctx context.Context, method string, req mcp.Request) (result mcp.Result, err error) {
		perm := methodToPerm[method]
		principal, ok := gateway.ValidatedPrincipalIDFromContext(ctx)
		if !ok {
			a.logDenied(ctx, method, principal, perm, "", "unauthenticated")
			return nil, errPermissionDenied
		}

		enforcer := a.policy.Authorizer(perm)
		var toolName string
		if method == "tools/call" {
			if toolName = calledToolName(req); toolName == "" {
				a.logDenied(ctx, method, principal, perm, toolName, "empty_tool_name")
				return nil, errPermissionDenied
			}
			enforcer = a.policy.SubResourceAuthorizer(toolResourceType, authz.ResourceID(toolName), perm)
		}
		if !enforcer.Check(principal) {
			a.logDenied(ctx, method, principal, perm, toolName, "forbidden")
			return nil, errPermissionDenied
		}

		result, err = next(ctx, method, req)
		if list, isList := result.(*mcp.ListToolsResult); isList && err == nil {
			list.Tools = slices.DeleteFunc(list.Tools, func(t *mcp.Tool) bool {
				return !a.policy.SubResourceAuthorizer(toolResourceType, authz.ResourceID(t.Name), permissionToolsCall).Check(principal)
			})
		}
		return result, err
	}
}

// logDenied records a policy decision to deny a request. Methods without a mapped permission,
// such as client notifications, are always denied and aren't policy decisions, so they aren't logged.
// Tool arguments are never logged, since they can carry customer data or secrets; the trace ID of
// the incoming request, when present, links the denial to its trace instead.
func (a *Authorizer) logDenied(ctx context.Context, method string, principal authz.PrincipalID, perm authz.PermissionName, toolName, reason string) {
	if perm == "" {
		return
	}
	attrs := []any{
		"method", method,
		"principal", string(principal),
		"permission", string(perm),
		"reason", reason,
	}
	if toolName != "" {
		attrs = append(attrs, "resource_type", string(toolResourceType), "resource_id", toolName)
	}
	if spanCtx := trace.SpanContextFromContext(ctx); spanCtx.HasTraceID() {
		attrs = append(attrs, "trace_id", spanCtx.TraceID().String())
	}
	a.logger.Warn("Authorization denied", attrs...)
}

func calledToolName(req mcp.Request) string {
	if params, ok := req.GetParams().(*mcp.CallToolParamsRaw); ok {
		return params.Name
	}
	return ""
}

// Close closes the resource policy and stops watching the policy file.
func (a *Authorizer) Close() error {
	return a.policy.Close()
}
