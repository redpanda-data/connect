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

// transform.go provides helper functions to convert raw Jira API objects into
// strongly typed response structs (issues, users, projects, roles, categories, versions, and transitions).

package jirahttp

import (
	"fmt"
)

// transformIssue takes a JiraIssue and returns a JiraIssueResponse with the changelog moved into the fields.
func transformIssue(orig Issue) IssueResponse {
	var r IssueResponse
	r.ID = orig.ID
	r.Key = orig.Key

	var fields map[string]any
	switch origFields := orig.Fields.(type) {
	case nil:
		fields = map[string]any{}
	case map[string]any:
		fields = make(map[string]any, len(origFields))
		for k, v := range origFields {
			fields[k] = v
		}
	default:
		fmt.Printf("Warning: issue.Fields type %T not map/nil (id=%s)\n", orig.Fields, orig.ID)
		fields = map[string]any{}
	}
	fields["changelog"] = orig.Changelog
	r.Fields = fields
	return r
}

// transformIssueTransition converts a raw Issue transition object into a
// issueTransitionResponse, safely handling unexpected types and extracting the ID.
func transformIssueTransition(orig any) issueTransitionResponse {
	var r issueTransitionResponse

	var fields map[string]any

	switch origFields := orig.(type) {
	case nil:
		fields = map[string]any{}
	case map[string]any:
		fields = make(map[string]any, len(origFields))
		for k, v := range origFields {
			fields[k] = v
		}
	default:
		fmt.Printf("Warning: issueTransition type %T not map/nil\n", orig)
		fields = map[string]any{}
	}

	r.Fields = fields

	if id, ok := fields["id"].(string); ok {
		r.ID = id
	} else {
		fmt.Println("Could not get issue transition id")
	}

	return r
}

// transformProject converts a raw project object into a ProjectResponse,
// copying its fields and extracting the ID and key.
func transformProject(orig any) ProjectResponse {
	var r ProjectResponse
	fields := map[string]any{}

	if m, ok := orig.(map[string]any); ok && m != nil {
		for k, v := range m {
			fields[k] = v
		}
	} else if orig != nil {
		fmt.Printf("Warning: project not map[string]any (type=%T)\n", orig)
	}

	r.Fields = fields

	if id, ok := fields["id"].(string); ok {
		r.ID = id
	} else {
		fmt.Println("Could not get project id")
	}
	if key, ok := fields["key"].(string); ok {
		r.Key = key
	} else {
		fmt.Println("Could not get project key")
	}

	return r
}

// transformUser converts a raw user object into a userResponse,copying its fields and extracting the account ID.
func transformUser(orig any) userResponse {
	var response userResponse
	var fields map[string]any

	switch msg := orig.(type) {
	case nil:
		fields = map[string]any{}
	case map[string]any:
		fields = make(map[string]any, len(msg))
		for k, v := range msg {
			fields[k] = v
		}
	default:
		fmt.Printf("Warning: user type %T not map/nil\n", orig)
		fields = map[string]any{}
	}

	response.Fields = fields

	if id, ok := fields["accountId"].(string); ok {
		response.ID = id
	} else {
		fmt.Println("Could not get user id")
	}

	return response
}

// transformProjectType converts a raw project type object into a projectTypeResponse,
// copying its fields and extracting the key and formatted key.
func transformProjectType(orig any) projectTypeResponse {
	var response projectTypeResponse
	fields := map[string]any{}

	if message, ok := orig.(map[string]any); ok && message != nil {
		for key, value := range message {
			fields[key] = value
		}
	} else if orig != nil {
		fmt.Printf("Warning: projectType not map[string]any (type=%T)\n", orig)
	}

	response.Fields = fields

	if key, ok := fields["key"].(string); ok {
		response.Key = key
	} else {
		fmt.Println("Could not get projectType key")
	}
	if formatedKey, ok := fields["formattedKey"].(string); ok {
		response.FormattedKey = formatedKey
	} else {
		fmt.Println("Could not get projectType formattedKey")
	}

	return response
}

// transformProjectCategory converts a raw project category object into a
// projectCategoryResponse, copying its fields and extracting the ID.
func transformProjectCategory(orig any) projectCategoryResponse {
	var projectCatRes projectCategoryResponse
	fields := map[string]any{}

	if msg, ok := orig.(map[string]any); ok && msg != nil {
		for key, value := range msg {
			fields[key] = value
		}
	} else if orig != nil {
		fmt.Printf("Warning: projectCategory not map[string]any (type=%T)\n", orig)
	}

	projectCatRes.Fields = fields

	if id, ok := fields["id"].(string); ok {
		projectCatRes.ID = id
	} else {
		fmt.Println("Could not get project category id")
	}

	return projectCatRes
}

// transformRole converts a raw role object into a roleResponse, copying its fields and extracting the ID.
func transformRole(orig any) roleResponse {
	var roleResponse roleResponse
	var fields map[string]any

	switch msg := orig.(type) {
	case nil:
		fields = map[string]any{}
	case map[string]any:
		fields = make(map[string]any, len(msg))
		for key, value := range msg {
			fields[key] = value
		}
	default:
		fmt.Printf("Warning: role type %T not map/nil\n", orig)
		fields = map[string]any{}
	}

	roleResponse.Fields = fields

	if id, ok := fields["id"].(string); ok {
		roleResponse.ID = id
	} else {
		fmt.Println("Could not get role id")
	}

	return roleResponse
}

// transformProjectVersion converts a raw project version object into a
// projectVersionResponse, copying its fields and extracting the ID.
func transformProjectVersion(orig any) projectVersionResponse {
	var versionRes projectVersionResponse
	var fields map[string]any

	switch msg := orig.(type) {
	case nil:
		fields = map[string]any{}
	case map[string]any:
		fields = make(map[string]any, len(msg))
		for key, value := range msg {
			fields[key] = value
		}
	default:
		fmt.Printf("Warning: project version type %T not map/nil\n", orig)
		fields = map[string]any{}
	}

	versionRes.Fields = fields

	if id, ok := fields["id"].(string); ok {
		versionRes.ID = id
	} else {
		fmt.Println("Could not get project version id")
	}

	return versionRes
}
