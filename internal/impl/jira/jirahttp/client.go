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

// client.go implements low-level interactions with the Jira REST API.
// It defines the base API path, provides a helper for making authenticated Jira API requests with retry
// and error handling, and exposes utilities for retrieving custom fields.
//
// These functions are primarily used by the Jira processor when preparing
// queries and resolving custom field identifiers.

package jirahttp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/jira/jirahttp/http_metrics"
)

// jiraAPIBasePath is the base path for Jira Rest API
const jiraAPIBasePath = "/rest/api/3"

// This is the general function that calls Jira API on a specific URL using the URL object.
// It applies standard header parameters to all calls, Authorization, User-Agent and Accept.
// It uses the helper functions to check against possible response codes and handling the retry-after mechanism
func (j *Client) callJiraApi(ctx context.Context, u *url.URL) ([]byte, error) {
	j.log.Debugf("API call: %s", u.String())

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}
	req.SetBasicAuth(j.username, j.apiToken)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "Redpanda-Connect")

	body, err := DoRequestWithRetries(ctx, j.httpClient, req, j.retryOpts)
	if err != nil {
		return nil, fmt.Errorf("request failed: %v", err)
	}

	return body, nil
}

// GetAllCustomFields function to get all Custom Fields from Jira API and placing them into a map
// Then iterate over the map and the fields from a Fields input message to check if any of the fields are custom
//
// Note that this supports custom fields that are nested, like if "Sprint.name" is present into the Fields input message -> this will be translated to "custom_field_10022.name"
// Returns only the custom fields present in the Fields input message as a map[fieldName]=customFieldName
func (j *Client) GetAllCustomFields(ctx context.Context, fieldsToSearch []string) (map[string]string, error) {
	j.log.Debug("Fetching custom fields from API")

	var allFields []CustomField
	startAt := 0

	for {
		response, err := j.getCustomFieldsPage(ctx, startAt)
		if err != nil {
			return nil, err
		}
		allFields = append(allFields, response.Fields...)
		if response.IsLast {
			break
		}
		startAt = response.StartAt + response.MaxResults
	}

	lookup := make(map[string]string, len(allFields))
	for _, f := range allFields {
		lookup[f.FieldName] = f.FieldID
	}

	customFieldsInQuery := make(map[string]string)
	// check for custom fields, remap fields from custom_field_xxxxx to the name of the custom field
	for _, field := range fieldsToSearch {
		if dot := strings.Index(field, "."); dot > -1 {
			field = field[:dot]
		}
		if value, ok := lookup[field]; ok {
			customFieldsInQuery[field] = value
		}
	}
	return customFieldsInQuery, nil
}

// Function to get a single page of custom fields using startAt strategy as the maximum number of custom fields to be retrieved is capped at 50
func (j *Client) getCustomFieldsPage(ctx context.Context, startAt int) (*CustomFieldSearchResponse, error) {
	apiUrl, err := url.Parse(j.baseURL + jiraAPIBasePath + "/field/search")
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %v", err)
	}
	query := apiUrl.Query()
	query.Set("type", "custom")
	query.Set("startAt", strconv.Itoa(startAt))
	apiUrl.RawQuery = query.Encode()

	body, err := j.callJiraApi(ctx, apiUrl)
	if err != nil {
		return nil, err
	}
	var result CustomFieldSearchResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("cannot map response to custom field struct: %w", err)
	}
	return &result, nil
}

// Client is the implementation of Jira API queries. It holds the client state and orchestrates calls into the jirahttp package.
type Client struct {
	baseURL    string
	username   string
	apiToken   string
	maxResults int
	retryOpts  RetryOptions
	httpClient *http.Client
	log        *service.Logger
}

// NewClient is the constructor ofr a Client object
func NewClient(log *service.Logger, baseUrl, username, apiToken string, maxResults, maxRetries int, metrics *service.Metrics, httpClient *http.Client, headerPolicy *AuthHeaderPolicy) (*Client, error) {
	return &Client{
		log:        log,
		baseURL:    baseUrl,
		username:   username,
		apiToken:   apiToken,
		maxResults: maxResults,
		retryOpts: RetryOptions{
			MaxRetries:       maxRetries,
			AuthHeaderPolicy: headerPolicy,
		},
		httpClient: http_metrics.NewInstrumentedClient(
			metrics, "jira_http",
			httpClient),
	}, nil
}
