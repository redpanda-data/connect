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

// resources_issues.go implements Jira resource handlers for issues and issue transitions.
// These functions are called by the resource dispatcher in resources.go.

package jira

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// searchIssuesResource performs a search for the issues resource
func (j *jiraProc) searchIssuesResource(
	ctx context.Context,
	inputQuery *JsonInputQuery,
	customFields map[string]string,
	params map[string]string,
) (service.MessageBatch, error) {
	var batch service.MessageBatch

	issues, err := j.searchAllIssues(ctx, params)
	if err != nil {
		return nil, err
	}
	if len(issues) == 0 {
		return batch, nil
	}

	// Normalize input fields
	normalizeInputFields(inputQuery, customFields)

	tree, err := j.buildSelectorTree(inputQuery.Fields, customFields)
	if err != nil {
		return nil, err
	}

	customRev := reverseCustomFields(customFields)

	for _, iss := range issues {
		resp := TransformIssue(iss)
		if len(tree) > 0 {
			filtered, err := j.filter(resp.Fields, tree, customRev)
			if err != nil {
				return nil, err
			}
			resp.Fields = filtered
		}
		b, err := json.Marshal(resp)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal issue: %w", err)
		}
		m := service.NewMessage(b)
		m.MetaSet("jira_issue_key", resp.Key)
		m.MetaSet("jira_issue_id", resp.ID)
		batch = append(batch, m)
	}

	return batch, nil
}

// searchAllIssues function to get all Issues from Jira API and placing them into an array of issues.
// If the nextPageToken is present in the response, then it will fetch the next page until isLast is true.
// Returns the array of []Issue
func (j *jiraProc) searchAllIssues(ctx context.Context, queryParams map[string]string) ([]Issue, error) {
	var all []Issue
	next := ""
	for {
		res, err := j.searchIssuesPage(ctx, queryParams, next)
		if err != nil {
			return nil, err
		}
		all = append(all, res.Issues...)
		if res.IsLast {
			break
		}
		next = res.NextPageToken
	}
	return all, nil
}

// searchIssuesPage function to get a single page of issues using nextPageToken strategy
// The maxResults can be overridden by the processor parameters (up to 5000 - default 50)
func (j *jiraProc) searchIssuesPage(ctx context.Context, qp map[string]string, nextPageToken string) (*JQLSearchResponse, error) {
	apiUrl, err := url.Parse(j.baseURL + JiraAPIBasePath + "/search/jql")
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %v", err)
	}

	query := apiUrl.Query()
	for k, v := range qp {
		query.Set(k, v)
	}
	query.Set("maxResults", strconv.Itoa(j.maxResults))
	if nextPageToken != "" {
		query.Set("nextPageToken", nextPageToken)
	}
	apiUrl.RawQuery = query.Encode()

	body, err := j.callJiraApi(ctx, apiUrl)
	if err != nil {
		return nil, err
	}

	var result JQLSearchResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("cannot map response to struct: %w", err)
	}
	return &result, nil
}

// searchIssueTransitionsResource retrieves all possible transitions for a given
// Jira issue and converts them into a batch of service messages.
// Parameters:
// - ctx: context.Context → request-scoped context for cancellation and timeouts
// - q: *JsonInputQuery → input query containing issue details and requested fields
// - custom: map[string]string → mapping of display names to custom field keys
// - params: map[string]string → query parameters for the Jira API request
// Returns:
// - service.MessageBatch → batch of messages containing transformed transitions
// - error → error if the API call, response parsing, or field processing fails
func (j *jiraProc) searchIssueTransitionsResource(ctx context.Context, q *JsonInputQuery, custom, params map[string]string) (service.MessageBatch, error) {
	var batch service.MessageBatch

	apiUrl, err := url.Parse(j.baseURL + JiraAPIBasePath + "/issue/" + q.Issue + "/transitions")
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %v", err)
	}

	query := apiUrl.Query()
	for key, value := range params {
		query.Set(key, value)
	}
	apiUrl.RawQuery = query.Encode()

	body, err := j.callJiraApi(ctx, apiUrl)
	if err != nil {
		return nil, err
	}

	var result IssueTransitionsSearchResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("cannot map response to struct: %w", err)
	}
	if len(result.Transitions) == 0 {
		return batch, nil
	}

	normalizeInputFields(q, custom)
	tree, err := j.buildSelectorTree(q.Fields, custom)
	if err != nil {
		return nil, err
	}
	customRev := reverseCustomFields(custom)

	for _, issueTransition := range result.Transitions {
		resp := TransformIssueTransition(issueTransition)
		if len(tree) > 0 {
			filtered, err := j.filter(resp.Fields, tree, customRev)
			if err != nil {
				return nil, err
			}
			resp.Fields = filtered
		}
		bytes, err := json.Marshal(resp)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal issue transition: %w", err)
		}

		message := service.NewMessage(bytes)
		message.MetaSet("jira_transition_issue_id", resp.ID)
		batch = append(batch, message)
	}
	return batch, nil
}
