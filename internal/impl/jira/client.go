package jira

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/redpanda-data/connect/v4/internal/impl/jira/helpers/http_helper"
	"github.com/redpanda-data/connect/v4/internal/impl/jira/helpers/jira_helper"
)

/*
This is the general function that calls Jira API on a specific URL using the URL object.
It applies standard header parameters to all calls, Authorization, User-Agent and Accept.
It uses the helper functions to check against possible response codes and handling the retry-after mechanism
*/
func (j *jiraProc) callJiraApi(ctx context.Context, u *url.URL) ([]byte, error) {
	j.debug("API call: %s", u.String())

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}
	req.SetBasicAuth(j.username, j.apiToken)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "Redpanda-Connect")

	resp, err := http_helper.DoRequestWithRetries(ctx, j.httpClient, req, j.retryOpts)
	if err != nil {
		return nil, fmt.Errorf("request failed: %v", err)
	}
	defer resp.Body.Close()

	body, jiraErr := jira_helper.CheckJiraAuth(resp)
	if jiraErr != nil {
		return nil, fmt.Errorf("error calling JIRA API: %v - %s", jiraErr, string(body))
	}
	return body, nil
}

/*
Function to get all Custom Fields from Jira API and placing them into a map
Then iterate over the map and the fields from a Fields input message to check if any of the fields are custom

Note that this supports custom fields that are nested, like if "Sprint.name" is present into the Fields input message -> this will be translated to "custom_field_10022.name"
Returns only the custom fields present in the Fields input message as a map[fieldName]=customFieldName
*/
func (j *jiraProc) getAllCustomFields(ctx context.Context, fieldsToSearch []string) (map[string]string, error) {
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
func (j *jiraProc) getCustomFieldsPage(ctx context.Context, startAt int) (*CustomFieldSearchResponse, error) {
	apiUrl, err := url.Parse(j.baseURL + JiraAPIBasePath + "/field/search")
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
