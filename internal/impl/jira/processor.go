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

package jira

import (
	"context"
	"errors"
	"net/http"
	"net/url"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/jira/helpers/http_helper"
	"github.com/redpanda-data/connect/v4/internal/impl/jira/helpers/http_metrics"
	"github.com/redpanda-data/connect/v4/internal/license"
)

// Configuration specification for the Jira processor
var jiraProcessorConfigSpec = service.NewConfigSpec().
	Summary("Fetches data from Jira based on input messages").
	Description(`This processor takes input messages containing Jira queries and returns Jira data.

Supports the following Jira resources:
- issues
- issue transitions
- users
- roles
- project versions
- project categories
- project types
- projects

Configuration examples:

` + "```yaml" + `
# Minimal configuration
pipeline:
  processors:
    - jira:
        base_url: "https://your-domain.atlassian.net"
        username: "${JIRA_USERNAME}"
        api_token: "${JIRA_API_TOKEN}"

# Full configuration
pipeline:
  processors:
    - jira:
        base_url: "https://your-domain.atlassian.net"
        username: "${JIRA_USERNAME}"
        api_token: "${JIRA_API_TOKEN}"
        max_results_per_page: 200
        request_timeout: "30s"
        max_retries: 50
` + "```").
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

func newJiraProcessor(conf *service.ParsedConfig, mgr *service.Resources) (*jiraProc, error) {
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

	return &jiraProc{
		baseURL:    baseURL,
		username:   username,
		apiToken:   apiToken,
		maxResults: maxResults,
		retryOpts: http_helper.RetryOptions{
			MaxRetries: maxRetries,
		},
		httpClient: http_metrics.NewInstrumentedClient(
			mgr.Metrics(), "jira_http",
			httpClient),
		log: mgr.Logger(),
	}, nil
}

func init() {
	if err := service.RegisterProcessor(
		"jira", jiraProcessorConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newJiraProcessor(conf, mgr)
		},
	); err != nil {
		panic(err)
	}
}

func (j *jiraProc) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	inputMsg, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}
	j.log.Debugf("Fetching from Jira.. Input: %s", string(inputMsg))

	inputQuery, err := j.extractQueryFromMessage(msg)
	if err != nil {
		return nil, err
	}

	resource, customFields, params, err := j.prepareJiraQuery(ctx, inputQuery)
	if err != nil {
		return nil, err
	}

	return SearchResource(ctx, j, resource, inputQuery, customFields, params)
}

func (*jiraProc) Close(context.Context) error { return nil }
