// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

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

// searchResource performs a search for a specific resource.
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
