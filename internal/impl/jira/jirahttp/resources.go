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

// resources.go defines the jiraProc processor struct and implements the resource dispatcher.
// The searchResource function routes incoming queries to the appropriate
// Jira resource handler (issues, projects, users, roles, etc.).

package jirahttp

import (
	"context"
	"fmt"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// searchResource performs a search for a specific resource
func searchResource(
	ctx context.Context,
	j *JiraProc,
	resource resourceType,
	inputQuery *jsonInputQuery,
	customFields map[string]string,
	params map[string]string,
) (service.MessageBatch, error) {
	switch resource {
	case ResourceIssue:
		return j.searchIssuesResource(ctx, inputQuery, customFields, params)
	case ResourceIssueTransition:
		return j.searchIssueTransitionsResource(ctx, inputQuery, customFields, params)
	case ResourceProject:
		return j.searchProjectsResource(ctx, inputQuery, customFields, params)
	case ResourceProjectType:
		return j.searchProjectTypesResource(ctx, inputQuery, customFields)
	case ResourceProjectCategory:
		return j.searchProjectCategoriesResource(ctx, inputQuery, customFields)
	case ResourceRole:
		return j.searchRolesResource(ctx, inputQuery, customFields)
	case ResourceProjectVersion:
		return j.searchProjectVersionsResource(ctx, inputQuery, customFields)
	case ResourceUser:
		return j.searchUsersResource(ctx, inputQuery, customFields, params)
	default:
		return nil, fmt.Errorf("unhandled resource type: %s", resource)
	}
}
