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

// query.go contains helpers for parsing input messages into query structures and preparing Jira Search API parameters.
// These helpers are used by the Jira processor to translate user-facing query input into valid request parameters.

package jirahttp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// expandableFieldsSet is a set of special fields that are not retrieved from the Jira API
// when using *all on fields param. Special fields are retrieved by placing them in the "expand" key
// in query params when making the call to Jira API.
var expandableFieldsSet = map[string]struct{}{
	"renderedFields":           {},
	"names":                    {},
	"schema":                   {},
	"operations":               {},
	"editmeta":                 {},
	"changelog":                {},
	"versionedRepresentations": {},
	"transitions.fields":       {},
}

// extractExpandableFields is a method to extract special fields directly from the Fields []string input message
// This is designed so that the input message won't need the "expand" property, which will make everything more readable
func extractExpandableFields(fields []string) []string {
	var result []string
	for _, f := range fields {
		topLevel := f
		if idx := strings.Index(f, "."); idx != -1 {
			topLevel = f[:idx]
		}
		if _, ok := expandableFieldsSet[topLevel]; ok {
			result = append(result, f)
		}
	}
	return result
}

// extractQueryFromMessage method receives the input message from the processor
// and parses it into a jsonInputQuery object
func (j *JiraProc) extractQueryFromMessage(msg *service.Message) (*jsonInputQuery, error) {
	var queryData *jsonInputQuery
	msgBytes, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(msgBytes, &queryData); err != nil {
		return nil, fmt.Errorf("cannot parse input JSON: %s", string(msgBytes))
	}
	j.Log.Debugf("Input queryData: %v", queryData)
	return queryData, nil
}

// prepareJiraQuery is used to form the JQL used in Jira Search API as this is the only possible method to retrieve issues
//
// If nested fields are present in the Fields array, we take only the first part of the string, until the dot(.) as Jira API does not support nested fields filtering
// If no fields are present in the Fields array, we get all possible fields from Jira using *all
//
// This method also creates the custom field map as we don't know if the fields present into the Fields parameter are custom or not
// This is to facilitate the input message to have a cleaner look, for example,
// Instead of 'fields: ["summary","custom_field_10100"]' to have 'fields: ["summary", "Story Points"]'
// This will check the fields against custom fields retrieved by the Custom Field Jira API
//
// This method also returns all the query params used for the issue Search API
func (j *JiraProc) prepareJiraQuery(ctx context.Context, q *jsonInputQuery) (resourceType, map[string]string, map[string]string, error) {
	params := make(map[string]string)
	resource := ResourceIssue

	if q.Resource != "" {
		r, err := parseResource(q.Resource)
		if err != nil {
			return resource, nil, nil, err
		}
		resource = r
	}

	if resource == ResourceIssue {
		// JQL overrides the project param
		if q.JQL != "" {
			params["jql"] = q.JQL
		} else if q.Project != "" {
			params["jql"] = "project = " + q.Project
		} else {
			return ResourceProject, nil, nil, nil
		}
	}

	if q.Updated != "" {
		op, val, err := parseOperatorField(q.Updated)
		if err != nil {
			return resource, nil, nil, err
		}
		params["jql"] += " and updated " + op + " \"" + val + "\""
	}
	if q.Created != "" {
		op, val, err := parseOperatorField(q.Created)
		if err != nil {
			return resource, nil, nil, err
		}
		params["jql"] += " and created " + op + " \"" + val + "\""
	}

	customFields, err := j.getAllCustomFields(ctx, q.Fields)
	if err != nil {
		return resource, nil, nil, err
	}

	if len(q.Fields) > 0 {
		processed := make([]string, 0, len(q.Fields))
		for _, f := range q.Fields {
			// JIRA API doesn't support nested fields filtering --> status.name,
			// so we send the status in the query param and filter for status.name in the response manually
			// also make sure to not include custom fields by their real name and use their custom_field_xxxxx name

			if dot := strings.Index(f, "."); dot != -1 {
				if _, exists := customFields[f[:dot]]; !exists {
					processed = append(processed, f[:dot])
				}
			} else {
				if _, exists := customFields[f]; !exists {
					processed = append(processed, f)
				}
			}
		}
		for _, value := range customFields {
			// Add custom fields in the field array based on their custom field name: custom_field_xxxxx
			processed = append(processed, value)
		}
		params["fields"] = strings.Join(processed, ",")

		if expanded := extractExpandableFields(q.Fields); len(expanded) > 0 {
			params["expand"] = strings.Join(expanded, ",")
		}
	} else {
		params["fields"] = "*all"
	}

	j.Log.Debugf("JQL result: %s", params["jql"])
	j.Log.Debugf("Fields selected: %s", params["fields"])
	j.Log.Debugf("Expand fields: %s", params["expand"])

	return resource, customFields, params, nil
}

// parseOperatorField parses an input string of the form "<1d", "<= 1d", "> 2010/12/31 14:00", ">-2w", etc.
// it returns the operator (one of =, !=, >, >=, <, <=) and the rest of the string (trimmed).
func parseOperatorField(input string) (string, string, error) {
	input = strings.TrimSpace(input)
	operators := []string{"!=", ">=", "<=", "=", ">", "<"}
	for _, op := range operators {
		if strings.HasPrefix(input, op) {
			value := strings.TrimSpace(input[len(op):])
			return op, value, nil
		}
	}
	return "", "", fmt.Errorf("invalid filter string: %s", input)
}
