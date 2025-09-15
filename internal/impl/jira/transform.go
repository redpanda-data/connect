package jira

import "fmt"

/*
TransformIssue takes a JiraIssue and returns a JiraIssueResponse
with the changelog moved into the fields.
*/
func TransformIssue(orig Issue) IssueResponse {
	var r IssueResponse
	r.ID = orig.ID
	r.Key = orig.Key

	var fields map[string]interface{}
	switch origFields := orig.Fields.(type) {
	case nil:
		fields = map[string]interface{}{}
	case map[string]interface{}:
		fields = make(map[string]interface{}, len(origFields))
		for k, v := range origFields {
			fields[k] = v
		}
	default:
		fmt.Printf("Warning: issue.Fields type %T not map/nil (id=%s)\n", orig.Fields, orig.ID)
		fields = map[string]interface{}{}
	}
	fields["changelog"] = orig.Changelog
	r.Fields = fields
	return r
}

/*
TransformIssueTransition converts a raw issue transition object into a
IssueTransitionResponse, safely handling unexpected types and extracting the ID.
*/
func TransformIssueTransition(orig interface{}) IssueTransitionResponse {
	var r IssueTransitionResponse

	var fields map[string]interface{}

	switch origFields := orig.(type) {
	case nil:
		fields = map[string]interface{}{}
	case map[string]interface{}:
		fields = make(map[string]interface{}, len(origFields))
		for k, v := range origFields {
			fields[k] = v
		}
	default:
		fmt.Printf("Warning: issueTransition type %T not map/nil\n", orig)
		fields = map[string]interface{}{}
	}

	r.Fields = fields

	if id, ok := fields["id"].(string); ok {
		r.ID = id
	} else {
		fmt.Println("Could not get issue transition id")
	}

	return r
}

/*
TransformProject converts a raw project object into a ProjectResponse,
copying its fields and extracting the ID and key.
*/
func TransformProject(orig interface{}) ProjectResponse {
	var r ProjectResponse
	fields := map[string]interface{}{}

	if m, ok := orig.(map[string]interface{}); ok && m != nil {
		for k, v := range m {
			fields[k] = v
		}
	} else if orig != nil {
		fmt.Printf("Warning: project not map[string]interface{} (type=%T)\n", orig)
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

/*
TransformUser converts a raw user object into a UserResponse,
copying its fields and extracting the account ID.
*/
func TransformUser(orig interface{}) UserResponse {
	var response UserResponse
	var fields map[string]interface{}

	switch msg := orig.(type) {
	case nil:
		fields = map[string]interface{}{}
	case map[string]interface{}:
		fields = make(map[string]interface{}, len(msg))
		for k, v := range msg {
			fields[k] = v
		}
	default:
		fmt.Printf("Warning: user type %T not map/nil\n", orig)
		fields = map[string]interface{}{}
	}

	response.Fields = fields

	if id, ok := fields["accountId"].(string); ok {
		response.ID = id
	} else {
		fmt.Println("Could not get user id")
	}

	return response
}

/*
TransformProjectType converts a raw project type object into a ProjectTypeResponse,
copying its fields and extracting the key and formatted key.
*/
func TransformProjectType(orig interface{}) ProjectTypeResponse {
	var response ProjectTypeResponse
	fields := map[string]interface{}{}

	if message, ok := orig.(map[string]interface{}); ok && message != nil {
		for key, value := range message {
			fields[key] = value
		}
	} else if orig != nil {
		fmt.Printf("Warning: projectType not map[string]interface{} (type=%T)\n", orig)
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

/*
TransformProjectCategory converts a raw project category object into a
ProjectCategoryResponse, copying its fields and extracting the ID.
*/
func TransformProjectCategory(orig interface{}) ProjectCategoryResponse {
	var projectCatRes ProjectCategoryResponse
	fields := map[string]interface{}{}

	if msg, ok := orig.(map[string]interface{}); ok && msg != nil {
		for key, value := range msg {
			fields[key] = value
		}
	} else if orig != nil {
		fmt.Printf("Warning: projectCategory not map[string]interface{} (type=%T)\n", orig)
	}

	projectCatRes.Fields = fields

	if id, ok := fields["id"].(string); ok {
		projectCatRes.ID = id
	} else {
		fmt.Println("Could not get project category id")
	}

	return projectCatRes
}

/*
TransformRole converts a raw role object into a RoleResponse,
copying its fields and extracting the ID.
*/
func TransformRole(orig interface{}) RoleResponse {
	var roleResponse RoleResponse
	var fields map[string]interface{}

	switch msg := orig.(type) {
	case nil:
		fields = map[string]interface{}{}
	case map[string]interface{}:
		fields = make(map[string]interface{}, len(msg))
		for key, value := range msg {
			fields[key] = value
		}
	default:
		fmt.Printf("Warning: role type %T not map/nil\n", orig)
		fields = map[string]interface{}{}
	}

	roleResponse.Fields = fields

	if id, ok := fields["id"].(string); ok {
		roleResponse.ID = id
	} else {
		fmt.Println("Could not get role id")
	}

	return roleResponse
}

/*
TransformProjectVersion converts a raw project version object into a
ProjectVersionResponse, copying its fields and extracting the ID.
*/
func TransformProjectVersion(orig interface{}) ProjectVersionResponse {
	var versionRes ProjectVersionResponse
	var fields map[string]interface{}

	switch msg := orig.(type) {
	case nil:
		fields = map[string]interface{}{}
	case map[string]interface{}:
		fields = make(map[string]interface{}, len(msg))
		for key, value := range msg {
			fields[key] = value
		}
	default:
		fmt.Printf("Warning: project version type %T not map/nil\n", orig)
		fields = map[string]interface{}{}
	}

	versionRes.Fields = fields

	if id, ok := fields["id"].(string); ok {
		versionRes.ID = id
	} else {
		fmt.Println("Could not get project version id")
	}

	return versionRes
}
