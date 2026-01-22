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

	"github.com/modelcontextprotocol/go-sdk/mcp"

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
// (and watches) the configuration file for policy enforcement.
func NewAuthorizer(name authz.ResourceName, file string, logger *slog.Logger) (*Authorizer, error) {
	notifyError := func(err error) {
		logger.Warn("authorization policy error", "err", err)
	}
	policy, err := gateway.NewFileWatchingAuthzResourcePolicy(name, file, allPermissions, notifyError)
	if err != nil {
		return nil, err
	}
	return &Authorizer{policy: policy}, nil
}

// Authorizer provides middleware for enforcing authorization policies on MCP method calls.
type Authorizer struct {
	policy *gateway.FileWatchingAuthzResourcePolicy
}

// Middleware returns an MCP method handler that enforces authorization checks before invoking the next handler.
func (a *Authorizer) Middleware(next mcp.MethodHandler) mcp.MethodHandler {
	return func(ctx context.Context, method string, req mcp.Request) (result mcp.Result, err error) {
		principal, ok := gateway.ValidatedPrincipalIDFromContext(ctx)
		enforcer := a.policy.Authorizer(methodToPerm[method])
		if !ok || !enforcer.Check(principal) {
			return nil, errors.New("permission denied")
		}
		return next(ctx, method, req)
	}
}

// Close closes the resource policy and stops watching the policy file.
func (a *Authorizer) Close() error {
	return a.policy.Close()
}
