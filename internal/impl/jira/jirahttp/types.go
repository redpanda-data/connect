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

// types.go defines core data structures, response models, and enums for the Jira processor.
// It includes input query types, API response DTOs, output message formats, and resource type constants.

package jirahttp

import "errors"

/*** Input / DTOs ***/

// JsonInputQuery represents the input message that is received and processed by the processor
// The JQL parameter has precedence over the project, Updated and Created fields
// None of the fields are mandatory
type JsonInputQuery struct {
	Resource string   `json:"resource"`
	Project  string   `json:"project"`
	Issue    string   `json:"issue"`
	Fields   []string `json:"fields"`
	JQL      string   `json:"jql"`
	Updated  string   `json:"updated"`
	Created  string   `json:"created"`
}

// Issue represents a single Jira Issue/task retrieved by the Jira API.
// Changelog is a special field retrieved by using "expand" in query params when making the call to Jira API.
// Changelog will not be exposed as it comes from the API, instead it will be merged into the Fields any
// to make use of the custom filtering
type Issue struct {
	ID        string `json:"id"`
	Key       string `json:"key"`
	Fields    any    `json:"fields"`
	Changelog any    `json:"changelog"`
}

// IssueResponse represents a single Jira Issue/task from this processor output
// All the fields from Fields any will be filtered accordingly using the Fields from JSON input message
type IssueResponse struct {
	ID     string `json:"id"`
	Key    string `json:"key"`
	Fields any    `json:"fields"`
}

// issueTransitionResponse represents a single Jira Issue transition from this processor output
// All the fields from Fields any will be filtered accordingly using the Fields from JSON input message
type issueTransitionResponse struct {
	ID     string `json:"id"`
	Fields any    `json:"fields"`
}

// issueTransitionsSearchResponse represents the response from Jira Issue transitions search API
type issueTransitionsSearchResponse struct {
	Transitions []any `json:"transitions"`
}

// ProjectResponse represents a single Jira project from this processor output
type ProjectResponse struct {
	ID     string `json:"id"`
	Key    string `json:"key"`
	Fields any    `json:"fields"`
}

// ProjectSearchResponse represents the response from Jira project search API
type ProjectSearchResponse struct {
	Projects []any  `json:"values"`
	IsLast   bool   `json:"isLast"`
	NextPage string `json:"nextPage"`
}

// projectTypeResponse represents a single Jira project type from this processor output
type projectTypeResponse struct {
	Key          string `json:"key"`
	FormattedKey string `json:"formattedKey"`
	Fields       any    `json:"fields"`
}

// projectCategoryResponse represents a single Jira project category from this processor output
type projectCategoryResponse struct {
	ID     string `json:"id"`
	Fields any    `json:"fields"`
}

// CustomField is a Jira object that maps custom fields that are coming from different plugins to a custom name
// Example: Field "Story Points" is represented in the message as "custom_field_10100" as it is not an official Jira field
type CustomField struct {
	FieldID   string `json:"id"`
	FieldName string `json:"name"`
}

// CustomFieldSearchResponse represents the response from the custom fields Jira search API
// The Custom Field Search API is using pagination and is limited to 50 results/page max
// We are using JiraCustomFieldSearchResponse in this context to get the whole array of []customField object directly from Jira
type CustomFieldSearchResponse struct {
	Fields     []CustomField `json:"values"`
	IsLast     bool          `json:"isLast"`
	StartAt    int           `json:"startAt"`
	MaxResults int           `json:"maxResults"`
	Total      int           `json:"total"`
}

// SearchJQLResponse represents the response from Jira JQL search API
// This is the only possible way at this moment to retrieve issues/tasks from Jira
// The pagination method of the JQL Search API is using a nextPageToken that can be used to retrieve next pages of issues
type SearchJQLResponse struct {
	Issues        []Issue `json:"issues"`
	IsLast        bool    `json:"isLast"`
	NextPageToken string  `json:"nextPageToken"`
}

// userResponse represents a Jira user from this processor output
type userResponse struct {
	ID     string `json:"accountId"`
	Fields any    `json:"fields"`
}

// roleResponse represents a single Jira role from this processor output
type roleResponse struct {
	ID     string `json:"id"`
	Fields any    `json:"fields"`
}

// projectVersionResponse represents a single Jira project version from this processor output
type projectVersionResponse struct {
	ID     string `json:"id"`
	Fields any    `json:"fields"`
}

/*** Resource enum ***/

// ResourceType is an enum that holds the resource types that we can query for
type ResourceType string

// list of ResourceType values
const (
	ResourceIssue           ResourceType = "issue"
	ResourceIssueTransition ResourceType = "issue_transition"
	ResourceRole            ResourceType = "role"
	ResourceUser            ResourceType = "user"
	ResourceProject         ResourceType = "project"
	ResourceProjectCategory ResourceType = "project_category"
	ResourceProjectType     ResourceType = "project_type"
	ResourceProjectVersion  ResourceType = "project_version"
)

// parseResource safely converts a string into ResourceType or returns an error
func parseResource(s string) (ResourceType, error) {
	switch ResourceType(s) {
	case ResourceIssue, ResourceIssueTransition, ResourceRole,
		ResourceUser, ResourceProjectVersion, ResourceProject,
		ResourceProjectCategory, ResourceProjectType:
		return ResourceType(s), nil
	}
	return "", errors.New("invalid resource type: " + s)
}
