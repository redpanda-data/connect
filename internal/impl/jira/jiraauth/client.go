// Copyright 2026 Redpanda Data, Inc.
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

// Package jiraauth provides shared construction of a configured *jirahttp.Client.
// Both the Jira processor and Jira input parse their own config shapes and pass
// resolved values into BuildClient, which wires basic auth and the standard
// Jira retry policy through the httpclient layer.
package jiraauth

import (
	"errors"
	"fmt"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/httpclient"
	"github.com/redpanda-data/connect/v4/internal/impl/jira/jirahttp"
)

// BuildClient constructs a *jirahttp.Client using basic auth (username or
// email + API token) and the standard Jira retry policy: retry on 429 and
// 5xx, hard-fail on 401/403.
//
// httpCfg is mutated in place: AuthSigner, Retry, and MetricPrefix are set.
// Callers should pass a config returned from httpclient.NewConfigFromParsed.
func BuildClient(mgr *service.Resources, httpCfg *httpclient.Config, username, apiToken string, maxResults int) (*jirahttp.Client, error) {
	if httpCfg == nil {
		return nil, errors.New("http config must not be nil")
	}
	if httpCfg.BaseURL == "" {
		return nil, errors.New("base_url must not be empty")
	}
	if username == "" {
		return nil, errors.New("username must not be empty")
	}
	if apiToken == "" {
		return nil, errors.New("api_token must not be empty")
	}
	if maxResults <= 0 || maxResults > 5000 {
		return nil, errors.New("max_results must be between 1 and 5000")
	}

	httpCfg.AuthSigner = httpclient.BasicAuthSigner(username, apiToken)
	httpCfg.Retry = &httpclient.RetryConfig{
		MaxRetries:    3,
		RetryStatuses: []int{429, 502, 503, 504},
		DropStatuses:  []int{401, 403},
	}
	if httpCfg.MetricPrefix == "" {
		httpCfg.MetricPrefix = "jira_http"
	}

	httpClient, err := httpclient.NewClient(*httpCfg, mgr)
	if err != nil {
		return nil, fmt.Errorf("building http client: %w", err)
	}

	headerPolicy := &jirahttp.AuthHeaderPolicy{
		HeaderName: "X-Seraph-LoginReason",
		IsProblem: func(reason string) bool {
			return reason != "" && reason != "OK" && reason != "AUTHENTICATED_TRUE"
		},
	}

	return jirahttp.NewClient(mgr.Logger(), httpCfg.BaseURL, maxResults, httpClient, headerPolicy), nil
}
