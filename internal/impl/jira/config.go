package jira

import (
	"errors"
	"net/http"
	"net/url"

	"github.com/redpanda-data/connect/v4/internal/impl/jira/helpers/http_helper"
	"github.com/redpanda-data/connect/v4/internal/impl/jira/helpers/http_metrics"

	"github.com/redpanda-data/benthos/v4/public/service"
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
