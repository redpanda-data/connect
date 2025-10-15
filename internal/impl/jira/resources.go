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

// resources.go defines the jiraProc jiraProcessor struct and implements the resource dispatcher.
// The searchResource function routes incoming queries to the appropriate
// Jira resource handler (issues, projects, users, roles, etc.).

package jira

import (
	"context"
	"fmt"

	"github.com/redpanda-data/connect/v4/internal/impl/jira/jirahttp"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// searchResource performs a search for a specific resource
func (j *jiraProcessor) searchResource(
	ctx context.Context,
	resource jirahttp.ResourceType,
	inputQuery *jirahttp.JsonInputQuery,
	customFields map[string]string,
	params map[string]string,
) (service.MessageBatch, error) {
	switch resource {
	case jirahttp.ResourceIssue:
		return j.client.SearchIssuesResource(ctx, inputQuery, customFields, params)
	case jirahttp.ResourceIssueTransition:
		return j.client.SearchIssueTransitionsResource(ctx, inputQuery, customFields, params)
	case jirahttp.ResourceProject:
		return j.client.SearchProjectsResource(ctx, inputQuery, customFields, params)
	case jirahttp.ResourceProjectType:
		return j.client.SearchProjectTypesResource(ctx, inputQuery, customFields)
	case jirahttp.ResourceProjectCategory:
		return j.client.SearchProjectCategoriesResource(ctx, inputQuery, customFields)
	case jirahttp.ResourceRole:
		return j.client.SearchRolesResource(ctx, inputQuery, customFields)
	case jirahttp.ResourceProjectVersion:
		return j.client.SearchProjectVersionsResource(ctx, inputQuery, customFields)
	case jirahttp.ResourceUser:
		return j.client.SearchUsersResource(ctx, inputQuery, customFields, params)
	default:
		return nil, fmt.Errorf("unhandled resource type: %s", resource)
	}
}
