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

// Package jira provides a Benthos jiraProcessor that integrates with the Jira API
// to fetch data based on input messages. It allows querying Jira resources
// such as issues, projects, users, roles, transitions, and more.
//
// The jiraProcessor is configured with Jira connection details (base URL, user
// credentials, API token) along with query and pagination options. Each input
// message is parsed into a Jira query, which is then executed against the Jira
// Search API or related resource APIs.
//
// The jiraProcessor handles pagination, retries, and optional field expansion in
// order to make working with Jira’s API more convenient inside message-oriented
// workflows.
package jira

import (
	"context"
	"errors"
	"net/http"
	"net/url"

	"github.com/redpanda-data/connect/v4/internal/impl/jira/jirahttp"
	"github.com/redpanda-data/connect/v4/internal/license"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// jiraProcessor is the Benthos jiraProcessor implementation for Jira queries.
// It holds the client state and orchestrates calls into the jirahttp package.
type jiraProcessor struct {
	log    *service.Logger
	client *jirahttp.Client
}

// newJiraProcessorConfigSpec creates a new Configuration specification for the Jira processor
func newJiraProcessorConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Version("4.68.0").
		Summary("Queries Jira resources and returns structured data").
		Description(`Executes Jira API queries based on input messages and returns structured results. The processor handles pagination, retries, and field expansion automatically.

Supports querying the following Jira resources:
- Issues (JQL queries)
- Issue transitions
- Users
- Roles
- Project versions
- Project categories
- Project types
- Projects

The processor authenticates using basic authentication with username and API token. Input messages should contain valid Jira queries in JSON format.`).
		Example(
			"Minimal configuration",
			"Basic Jira processor setup with required fields only",
			`
pipeline:
  processors:
    - jira:
        base_url: "https://your-domain.atlassian.net"
        username: "${JIRA_USERNAME}"
        api_token: "${JIRA_API_TOKEN}"
`).
		Example(
			"Full configuration with tuning",
			"Complete configuration with pagination, timeout, and retry settings",
			`
pipeline:
  processors:
    - jira:
        base_url: "https://your-domain.atlassian.net"
        username: "${JIRA_USERNAME}"
        api_token: "${JIRA_API_TOKEN}"
        max_results_per_page: 200
        request_timeout: "30s"
        max_retries: 50
`).
		Field(service.NewStringField("base_url").
			Description("Jira instance base URL (e.g., https://your-domain.atlassian.net)")).
		Field(service.NewStringField("username").
			Description("Jira instance account username/email")).
		Field(service.NewStringField("api_token").
			Description("Jira API token for the specified account").
			Secret()).
		Field(service.NewIntField("max_results_per_page").
			Description("Maximum number of results to return per page when calling JIRA API").
			Default(50)).
		Field(service.NewDurationField("request_timeout").
			Description("HTTP request timeout").
			Default("30s")).
		Field(service.NewIntField("max_retries").
			Description("Maximum number of retries in case of 429 HTTP Status Code").
			Default(10))
}

// newJiraProcessor initializes and returns a jiraProcessor instance based
// on the provided Benthos configuration and resource manager. It validates
// the configuration values, sets up the Jira HTTP client, and ensures that
// an enterprise license is active before creating the processor.
func newJiraProcessor(conf *service.ParsedConfig, mgr *service.Resources) (*jiraProcessor, error) {
	if err := license.CheckRunningEnterprise(mgr); err != nil {
		return nil, err
	}

	baseURL, err := conf.FieldString("base_url")
	if err != nil {
		return nil, err
	}

	if _, err := url.ParseRequestURI(baseURL); err != nil {
		return nil, errors.New("base_url is not a valid URL")
	}

	username, err := conf.FieldString("username")
	if err != nil {
		return nil, err
	}

	apiToken, err := conf.FieldString("api_token")
	if err != nil {
		return nil, err
	}

	timeout, err := conf.FieldDuration("request_timeout")
	if err != nil {
		return nil, err
	}

	maxResults, err := conf.FieldInt("max_results_per_page")
	if err != nil {
		return nil, err
	}

	maxRetries, err := conf.FieldInt("max_retries")
	if err != nil {
		return nil, err
	}

	httpClient := &http.Client{Timeout: timeout}

	headerPolicy := &jirahttp.AuthHeaderPolicy{
		HeaderName: "X-Seraph-LoginReason",
		IsProblem: func(reason string) bool {
			return reason != "" && reason != "OK" && reason != "AUTHENTICATED_TRUE"
		},
	}

	jiraHttp, err := jirahttp.NewClient(mgr.Logger(), baseURL, username, apiToken, maxResults, maxRetries, mgr.Metrics(), httpClient, headerPolicy)
	if err != nil {
		return nil, err
	}

	return &jiraProcessor{
		client: jiraHttp,
		log:    mgr.Logger(),
	}, nil
}

func (j *jiraProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	inputMsg, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}
	j.log.Debugf("Fetching from Jira.. Input: %s", string(inputMsg))

	inputQuery, err := j.client.ExtractQueryFromMessage(msg)
	if err != nil {
		return nil, err
	}

	resource, customFields, params, err := j.client.PrepareJiraQuery(ctx, inputQuery)
	if err != nil {
		return nil, err
	}

	return j.searchResource(ctx, resource, inputQuery, customFields, params)
}

// Close shuts down the Jira processor.
func (*jiraProcessor) Close(context.Context) error { return nil }

// init registers the Jira processor with Benthos, wiring its configuration spec and constructor.
func init() {
	if err := service.RegisterProcessor(
		"jira", newJiraProcessorConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newJiraProcessor(conf, mgr)
		},
	); err != nil {
		panic(err)
	}
}
