package jira

import (
	"context"
	"fmt"
	"net/http"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/jira/helpers/http_helper"
)

type jiraProc struct {
	baseURL    string
	username   string
	apiToken   string
	maxResults int
	retryOpts  http_helper.RetryOptions
	httpClient *http.Client
	log        *service.Logger
}

// SearchResource performs a search for a specific resource
func SearchResource(
	ctx context.Context,
	j *jiraProc,
	resource ResourceType,
	inputQuery *JsonInputQuery,
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
