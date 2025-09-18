package jira

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"

	"github.com/redpanda-data/benthos/v4/public/service"
)

/*
Function to get a single page of issues using startAt offset strategy
The maxResults can be overridden by the processor parameters (up to 5000 - default 50)
*/
func (j *jiraProc) searchUsersPage(ctx context.Context, queryParams map[string]string, startAt int) ([]interface{}, error) {
	apiUrl, err := url.Parse(j.baseURL + JiraAPIBasePath + "/users/search")
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %v", err)
	}

	query := apiUrl.Query()
	for key, value := range queryParams {
		query.Set(key, value)
	}
	query.Set("maxResults", strconv.Itoa(j.maxResults))
	if startAt != 0 {
		query.Set("startAt", strconv.Itoa(startAt))
	}
	apiUrl.RawQuery = query.Encode()

	body, err := j.callJiraApi(ctx, apiUrl)
	if err != nil {
		return nil, err
	}

	var results []interface{}
	if err := json.Unmarshal(body, &results); err != nil {
		return nil, fmt.Errorf("cannot map response to struct: %w", err)
	}

	return results, nil
}

/*
searchAllUsers retrieves all Jira users by performing paginated API calls until
no more results are returned.

Parameters:
- ctx: context.Context → request context for cancellation and timeouts
- queryParams: map[string]string → query parameters for the Jira API request

Returns:
- []interface{} → list of all retrieved users
- error → error if a paginated request fails
*/
func (j *jiraProc) searchAllUsers(ctx context.Context, queryParams map[string]string) ([]interface{}, error) {
	var allUsers []interface{}

	startAt := 0
	for {
		users, err := j.searchUsersPage(ctx, queryParams, startAt)
		if err != nil {
			return nil, err
		}

		if len(users) == 0 {
			break
		}

		allUsers = append(allUsers, users...)

		startAt = startAt + len(users)
	}

	return allUsers, nil
}

/*
searchUsersResource queries Jira for users based on the provided parameters and
returns them as a batch of service messages.

Parameters:
- ctx: context.Context → request context for cancellation and timeouts
- inputQuery: *JsonInputQuery → user input specifying requested fields
- customFields: map[string]string → mapping of display names to custom field keys
- params: map[string]string → query parameters for the Jira API request

Returns:
- service.MessageBatch → batch of messages containing transformed users
- error → error if the API call, response parsing, or field processing fails
*/
func (j *jiraProc) searchUsersResource(
	ctx context.Context,
	inputQuery *JsonInputQuery,
	customFields map[string]string,
	params map[string]string,
) (service.MessageBatch, error) {
	var batch service.MessageBatch

	users, err := j.searchAllUsers(ctx, params)
	if err != nil {
		return nil, err
	}
	if len(users) == 0 {
		return batch, nil
	}

	normalizeInputFields(inputQuery, customFields)

	tree, err := j.buildSelectorTree(inputQuery.Fields, customFields)
	if err != nil {
		return nil, err
	}

	customFieldsReversed := reverseCustomFields(customFields)

	for _, user := range users {
		response := TransformUser(user)

		if len(tree) > 0 {
			filtered, err := j.filter(response.Fields, tree, customFieldsReversed)
			if err != nil {
				return nil, err
			}
			response.Fields = filtered
		}

		bytes, err := json.Marshal(response)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal user: %w", err)
		}

		message := service.NewMessage(bytes)
		message.MetaSet("jira_user_id", response.ID)
		batch = append(batch, message)
	}

	return batch, nil
}
