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
	"context"
	"github.com/redpanda-data/connect/v4/internal/impl/jira/helpers/http_helper"
	"net/http"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// JiraProc is the Benthos processor implementation for Jira queries.
// It holds the client state and orchestrates calls into the jirahttp package.
type JiraProc struct {
	BaseURL    string
	Username   string
	ApiToken   string
	MaxResults int
	RetryOpts  http_helper.RetryOptions
	HttpClient *http.Client
	Log        *service.Logger
}

func (j *JiraProc) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	inputMsg, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}
	j.Log.Debugf("Fetching from Jira.. Input: %s", string(inputMsg))

	inputQuery, err := j.extractQueryFromMessage(msg)
	if err != nil {
		return nil, err
	}

	resource, customFields, params, err := j.prepareJiraQuery(ctx, inputQuery)
	if err != nil {
		return nil, err
	}

	return searchResource(ctx, j, resource, inputQuery, customFields, params)
}

func (*JiraProc) Close(context.Context) error { return nil }
