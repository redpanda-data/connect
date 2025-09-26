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

// resources_projects.go implements Jira resource handlers for projects,
// including project search, types, categories, and versions.
// These helpers fetch and transform project-related data into service messages.

package jira

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// searchProjectsResource retrieves Jira projects based on the provided parameters
// and returns them as a batch of service messages.
// Parameters:
// - ctx: context.Context → request context for cancellation and timeouts
// - inputQuery: *JsonInputQuery → query object containing requested fields
// - customFields: map[string]string → mapping of display names to custom field keys
// - params: map[string]string → query parameters for the Jira API request
// Returns:
// - service.MessageBatch → batch of messages containing transformed projects
// - error → error if the API call, response parsing, or field processing fails
func (j *jiraProc) searchProjectsResource(
	ctx context.Context,
	inputQuery *JsonInputQuery,
	customFields map[string]string,
	params map[string]string,
) (service.MessageBatch, error) {
	var batch service.MessageBatch

	projects, err := j.searchAllProjects(ctx, params)
	if err != nil {
		return nil, err
	}
	if len(projects) == 0 {
		return batch, nil
	}

	normalizeInputFields(inputQuery, customFields)

	tree, err := SelectorTreeFrom(j.log, inputQuery.Fields, customFields)
	if err != nil {
		return nil, err
	}
	customRev := reverseCustomFields(customFields)

	for _, project := range projects {
		projectResponse := TransformProject(project)
		if len(tree) > 0 {
			filtered, err := j.filter(projectResponse.Fields, tree, customRev)
			if err != nil {
				return nil, err
			}
			projectResponse.Fields = filtered
		}
		projectBytes, err := json.Marshal(projectResponse)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal project: %w", err)
		}
		newMsg := service.NewMessage(projectBytes)
		newMsg.MetaSet("jira_project_key", projectResponse.Key)
		newMsg.MetaSet("jira_project_id", projectResponse.ID)
		batch = append(batch, newMsg)
	}
	return batch, nil
}

// searchAllProjects retrieves all Jira projects by performing paginated API calls until all results are collected.
// Parameters:
// - ctx: context.Context → request context for cancellation and timeouts
// - queryParams: map[string]string → query parameters for the Jira API request
// Returns:
// - []any → list of all retrieved projects
// - error → error if a paginated request or response parsing fails
func (j *jiraProc) searchAllProjects(ctx context.Context, queryParams map[string]string) ([]any, error) {
	var all []any
	startAt := 0
	for {
		res, err := j.searchProjectsPage(ctx, queryParams, startAt)
		if err != nil {
			return nil, err
		}
		all = append(all, res.Projects...)
		if res.IsLast {
			break
		}
		next := res.NextPage
		parsed, err := url.Parse(next)
		if err != nil {
			return nil, fmt.Errorf("invalid URL: %v", err)
		}
		off := parsed.Query().Get("startAt")
		if off == "" {
			break
		}
		startAt, err = strconv.Atoi(off)
		if err != nil {
			return nil, fmt.Errorf("invalid next page offset: %v", err)
		}
	}
	return all, nil
}

// Function to get a single page of issues using startAt offset strategy
// The maxResults can be overridden by the processor parameters (up to 5000 - default 50)
func (j *jiraProc) searchProjectsPage(ctx context.Context, qp map[string]string, startAt int) (*ProjectSearchResponse, error) {
	urlString, err := url.Parse(j.baseURL + JiraAPIBasePath + "/project/search")
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %v", err)
	}

	query := urlString.Query()
	for key, value := range qp {
		query.Set(key, value)
	}
	query.Set("maxResults", strconv.Itoa(j.maxResults))
	if startAt != 0 {
		query.Set("startAt", strconv.Itoa(startAt))
	}
	urlString.RawQuery = query.Encode()

	body, err := j.callJiraApi(ctx, urlString)
	if err != nil {
		return nil, err
	}

	var result ProjectSearchResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("cannot map response to struct: %w", err)
	}
	return &result, nil
}

// searchProjectTypesResource retrieves all Jira project types and returns them as a batch of service messages.
// Parameters:
// - ctx: context.Context → request context for cancellation and timeouts
// - q: *JsonInputQuery → query object containing requested fields
// - custom: map[string]string → mapping of display names to custom field keys
// Returns:
// - service.MessageBatch → batch of messages containing transformed project types
// - error → error if the API call, response parsing, or field processing fails
func (j *jiraProc) searchProjectTypesResource(ctx context.Context, q *JsonInputQuery, custom map[string]string) (service.MessageBatch, error) {
	var batch service.MessageBatch

	urlString, err := url.Parse(j.baseURL + JiraAPIBasePath + "/project/type")
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %v", err)
	}
	body, err := j.callJiraApi(ctx, urlString)
	if err != nil {
		return nil, err
	}

	var results []any
	if err := json.Unmarshal(body, &results); err != nil {
		return nil, fmt.Errorf("cannot map response to struct: %w", err)
	}

	normalizeInputFields(q, custom)
	tree, err := SelectorTreeFrom(j.log, q.Fields, custom)
	if err != nil {
		return nil, err
	}
	customRev := reverseCustomFields(custom)

	for _, projectType := range results {
		resp := TransformProjectType(projectType)
		if len(tree) > 0 {
			filtered, err := j.filter(resp.Fields, tree, customRev)
			if err != nil {
				return nil, err
			}
			resp.Fields = filtered
		}
		projectTypeBytes, _ := json.Marshal(resp)
		message := service.NewMessage(projectTypeBytes)
		message.MetaSet("jira_project_type_key", resp.Key)
		message.MetaSet("jira_project_type_formatted_key", resp.FormattedKey)
		batch = append(batch, message)
	}
	return batch, nil
}

// searchProjectCategoriesResource retrieves all Jira project categories and returns them as a batch of service messages.
// Parameters:
// - ctx: context.Context → request context for cancellation and timeouts
// - q: *JsonInputQuery → query object containing requested fields
// - custom: map[string]string → mapping of display names to custom field keys
// Returns:
// - service.MessageBatch → batch of messages containing transformed project categories
// - error → error if the API call, response parsing, or field processing fails
func (j *jiraProc) searchProjectCategoriesResource(ctx context.Context, q *JsonInputQuery, custom map[string]string) (service.MessageBatch, error) {
	var batch service.MessageBatch

	urlString, err := url.Parse(j.baseURL + JiraAPIBasePath + "/projectCategory")
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %v", err)
	}
	body, err := j.callJiraApi(ctx, urlString)
	if err != nil {
		return nil, err
	}

	var results []any
	if err := json.Unmarshal(body, &results); err != nil {
		return nil, fmt.Errorf("cannot map response to struct: %w", err)
	}

	normalizeInputFields(q, custom)
	tree, err := SelectorTreeFrom(j.log, q.Fields, custom)
	if err != nil {
		return nil, err
	}
	customRev := reverseCustomFields(custom)

	for _, projectCategory := range results {
		resp := TransformProjectCategory(projectCategory)
		if len(tree) > 0 {
			filtered, err := j.filter(resp.Fields, tree, customRev)
			if err != nil {
				return nil, err
			}
			resp.Fields = filtered
		}
		bytes, err := json.Marshal(resp)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal project category: %w", err)
		}
		message := service.NewMessage(bytes)
		message.MetaSet("jira_project_category_id", resp.ID)
		batch = append(batch, message)
	}
	return batch, nil
}

// searchProjectVersionsResource retrieves all versions of a given Jira project and
// returns them as a batch of service messages.
// Parameters:
// - ctx: context.Context → request context for cancellation and timeouts
// - inputQuery: *JsonInputQuery → query object containing the project key and requested fields
// - customFields: map[string]string → mapping of display names to custom field keys
// Returns:
// - service.MessageBatch → batch of messages containing transformed project versions
// - error → error if the API call, response parsing, or field processing fails
func (j *jiraProc) searchProjectVersionsResource(
	ctx context.Context,
	inputQuery *JsonInputQuery,
	customFields map[string]string,
) (service.MessageBatch, error) {
	var batch service.MessageBatch

	apiUrl, err := url.Parse(j.baseURL + JiraAPIBasePath + "/project/" + inputQuery.Project + "/versions")
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %v", err)
	}

	body, err := j.callJiraApi(ctx, apiUrl)
	if err != nil {
		return nil, err
	}

	var results []any
	if err := json.Unmarshal(body, &results); err != nil {
		return nil, fmt.Errorf("cannot map response to struct: %w", err)
	}

	normalizeInputFields(inputQuery, customFields)
	tree, err := SelectorTreeFrom(j.log, inputQuery.Fields, customFields)
	if err != nil {
		return nil, err
	}
	customRev := reverseCustomFields(customFields)

	for _, projectVersion := range results {
		resp := TransformProjectVersion(projectVersion)
		if len(tree) > 0 {
			filtered, err := j.filter(resp.Fields, tree, customRev)
			if err != nil {
				return nil, err
			}
			resp.Fields = filtered
		}
		bytes, err := json.Marshal(resp)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal project version: %w", err)
		}
		message := service.NewMessage(bytes)
		message.MetaSet("jira_project_version_id", resp.ID)
		batch = append(batch, message)
	}
	return batch, nil
}
