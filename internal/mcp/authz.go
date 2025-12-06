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
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/redpanda-data/common-go/authz"
	"github.com/redpanda-data/common-go/authz/loader"
	"github.com/redpanda-data/connect/v4/internal/gateway"
)

const (
	permissionInitialize             authz.PermissionName = "dataplane_mcp_server_initialize"
	permissionPing                   authz.PermissionName = "dataplane_mcp_server_ping"
	permissionResourcesList          authz.PermissionName = "dataplane_mcp_server_resources_list"
	permissionResourcesTemplatesList authz.PermissionName = "dataplane_mcp_server_resources_templates_list"
	permissionResourcesRead          authz.PermissionName = "dataplane_mcp_server_resources_read"
	permissionPromptsList            authz.PermissionName = "dataplane_mcp_server_prompts_list"
	permissionPromptsGet             authz.PermissionName = "dataplane_mcp_server_prompts_get"
	permissionToolsList              authz.PermissionName = "dataplane_mcp_server_tools_list"
	permissionToolsCall              authz.PermissionName = "dataplane_mcp_server_tools_call"
	permissionLoggingSetLevel        authz.PermissionName = "dataplane_mcp_server_logging_set_level"
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

// NewAuthorizer returns an MCP server authorizer which dynamically loads (and watches) the configuration file
// for policy enforcement.
func NewAuthorizer(name authz.ResourceName, file string, logger *slog.Logger) (*Authorizer, error) {
	a := &Authorizer{}
	policy, unwatch, err := loader.WatchPolicyFile(file, func(policy authz.Policy, err error) {
		if err != nil {
			logger.Warn("error watching authorization policy file", "err", err)
			return
		}
		rp, err := authz.NewResourcePolicy(policy, name, allPermissions)
		if err != nil {
			logger.Warn("error loading authorization policy file", "err", err)
			return
		}
		logger.Info("reloaded updated policy file", "file", file)
		a.value.Store(rp)
	})
	if err != nil {
		return nil, fmt.Errorf("unable to load authorization policy: %w", err)
	}
	a.unwatch = unwatch
	rp, err := authz.NewResourcePolicy(policy, name, allPermissions)
	if err != nil {
		return nil, fmt.Errorf("unable to compile authorization policy: %w", err)
	}
	a.value.Store(rp)
	return a, nil
}

// Authorizer provides middleware for enforcing authorization policies on MCP method calls.
type Authorizer struct {
	unwatch loader.PolicyUnwatch
	value   atomic.Pointer[authz.ResourcePolicy]
}

// Middleware returns an MCP method handler that enforces authorization checks before invoking the next handler.
func (a *Authorizer) Middleware(next mcp.MethodHandler) mcp.MethodHandler {
	return func(ctx context.Context, method string, req mcp.Request) (result mcp.Result, err error) {
		principal, ok := gateway.ValidatedPrincipalIDFromContext(ctx)
		enforcer := a.value.Load().Authorizer(methodToPerm[method])
		if !ok || !enforcer.Check(principal) {
			return nil, errors.New("permission denied")
		}
		return next(ctx, method, req)
	}
}

func (a *Authorizer) Close(_ context.Context) error {
	return a.unwatch()
}
