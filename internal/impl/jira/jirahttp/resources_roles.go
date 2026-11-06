// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// resources_roles.go implements Jira resource handlers for roles.
// It fetches Jira roles from the API and transforms them into service messages with optional field filtering.

package jirahttp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// SearchRolesResource retrieves all Jira roles and returns them as a batch
// of service messages after optional field filtering.
func (j *Client) SearchRolesResource(
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

	tree, err := selectorTreeFrom(j.log, inputQuery.Fields, customFields)
	if err != nil {
		return nil, err
	}

	customFieldsReversed := reverseCustomFields(customFields)

	for _, role := range roles {
		resp := transformRole(role)

		if len(tree) > 0 {
			filtered, err := j.filter(resp.Fields, tree, customFieldsReversed)
			if err != nil {
				return nil, err
			}
			resp.Fields = filtered
		}

		bytes, err := json.Marshal(resp)
		if err != nil {
			return nil, fmt.Errorf("marshalling role: %w", err)
		}

		message := service.NewMessage(bytes)
		message.MetaSet("jira_role_id", resp.ID)
		batch = append(batch, message)
	}

	return batch, nil
}

// searchRoles fetches all Jira roles from the API and returns them as a list.
func (j *Client) searchRoles(ctx context.Context) ([]any, error) {
	apiUrl, err := url.Parse(j.baseURL + jiraAPIBasePath + "/role")
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %v", err)
	}

	body, err := j.callJiraApi(ctx, apiUrl)
	if err != nil {
		return nil, err
	}

	var results []any
	if err := json.Unmarshal(body, &results); err != nil {
		return nil, fmt.Errorf("cannot map response to struct: %w", err)
	}

	return results, nil
}
