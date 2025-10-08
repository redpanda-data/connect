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

// Package jirahttp implements a low-level Jira API client and related
// helpers used internally by the jira processor. It provides:
//
//   - Client for making authenticated requests with retry and error handling
//   - Data transfer objects (DTOs) for Jira responses and queries
//   - Functions for building queries, normalizing fields, and filtering data
//   - Transformers for converting raw Jira API objects into typed responses
//
// Unlike the jira package, which contains only the Benthos processor glue
// and registration, jirahttp groups all Jira-specific logic in one place.
package jirahttp

import (
	"net/http"

	"github.com/redpanda-data/connect/v4/internal/impl/jira/helpers/http_helper"
	"github.com/redpanda-data/connect/v4/internal/impl/jira/helpers/http_metrics"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// JiraHttp is the implementation of Jira API queries. It holds the client state and orchestrates calls into the jirahttp package.
type JiraHttp struct {
	baseURL    string
	username   string
	apiToken   string
	maxResults int
	retryOpts  http_helper.RetryOptions
	httpClient *http.Client
	log        *service.Logger
}

// NewJiraHttp is the constructor ofr a JiraHttp object
func NewJiraHttp(log *service.Logger, baseUrl, username, apiToken string, maxResults, maxRetries int, metrics *service.Metrics, httpClient *http.Client) (*JiraHttp, error) {
	return &JiraHttp{
		log:        log,
		baseURL:    baseUrl,
		username:   username,
		apiToken:   apiToken,
		maxResults: maxResults,
		retryOpts: http_helper.RetryOptions{
			MaxRetries: maxRetries,
		},
		httpClient: http_metrics.NewInstrumentedClient(
			metrics, "jira_http",
			httpClient),
	}, nil
}
