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

package jiraauth_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/httpclient"
	"github.com/redpanda-data/connect/v4/internal/impl/jira/jiraauth"
)

func TestBuildClient_ValidatesRequiredFields(t *testing.T) {
	mgr := service.MockResources()
	httpCfg := &httpclient.Config{BaseURL: "https://example.atlassian.net"}

	_, err := jiraauth.BuildClient(mgr, httpCfg, "", "tok", 50)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "username")

	_, err = jiraauth.BuildClient(mgr, httpCfg, "u@x", "", 50)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "api_token")

	_, err = jiraauth.BuildClient(mgr, httpCfg, "u@x", "tok", 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max_results")

	_, err = jiraauth.BuildClient(mgr, httpCfg, "u@x", "tok", 6000)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max_results")
}

func TestBuildClient_ReturnsClientWithRetryConfigured(t *testing.T) {
	mgr := service.MockResources()
	httpCfg := &httpclient.Config{BaseURL: "https://example.atlassian.net"}

	c, err := jiraauth.BuildClient(mgr, httpCfg, "user@example.com", "secret-token", 50)
	require.NoError(t, err)
	require.NotNil(t, c)

	require.NotNil(t, httpCfg.Retry)
	assert.Equal(t, 3, httpCfg.Retry.MaxRetries)
	assert.ElementsMatch(t, []int{429, 502, 503, 504}, httpCfg.Retry.RetryStatuses)
	assert.ElementsMatch(t, []int{401, 403}, httpCfg.Retry.DropStatuses)
	assert.Equal(t, "jira_http", httpCfg.MetricPrefix)
}
