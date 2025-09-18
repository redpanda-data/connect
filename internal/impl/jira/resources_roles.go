package jira

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/redpanda-data/benthos/v4/public/service"
)

/*
searchRolesResource retrieves all Jira roles and returns them as a batch
of service messages after optional field filtering.
*/
func (j *jiraProc) searchRolesResource(
	ctx context.Context,
	inputQuery *JsonInputQuery,
	customFields map[string]string,
) (service.MessageBatch, error) {
	var batch service.MessageBatch

	roles, err := j.searchRoles(ctx)
	if err != nil {
		return nil, err
	}
	if len(roles) == 0 {
		return batch, nil
	}

	normalizeInputFields(inputQuery, customFields)

	tree, err := j.buildSelectorTree(inputQuery.Fields, customFields)
	if err != nil {
		return nil, err
	}

	customFieldsReversed := reverseCustomFields(customFields)

	for _, role := range roles {
		resp := TransformRole(role)

		if len(tree) > 0 {
			filtered, err := j.filter(resp.Fields, tree, customFieldsReversed)
			if err != nil {
				return nil, err
			}
			resp.Fields = filtered
		}

		bytes, err := json.Marshal(resp)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal role: %w", err)
		}

		message := service.NewMessage(bytes)
		message.MetaSet("jira_role_id", resp.ID)
		batch = append(batch, message)
	}

	return batch, nil
}

/*
searchRoles fetches all Jira roles from the API and returns them as a list.
*/
func (j *jiraProc) searchRoles(ctx context.Context) ([]interface{}, error) {
	apiUrl, err := url.Parse(j.baseURL + JiraAPIBasePath + "/role")
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %v", err)
	}

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
