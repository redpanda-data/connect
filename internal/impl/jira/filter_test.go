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

package jira

import (
	"reflect"
	"testing"
)

func TestBuildSelectorTree(t *testing.T) {
	j := &jiraProc{}
	fields := []string{"summary", "assignee.displayName", "status.name", "parent.fields.status.name", "Story Points", "Sprint.name"}
	custom := map[string]string{
		"Story Points": "custom_field_10100",
		"Sprint":       "custom_field_10022",
	}

	tree, err := j.buildSelectorTree(fields, custom)
	if err != nil {
		t.Fatalf("buildSelectorTree error: %v", err)
	}

	// spot checks
	if _, ok := tree["summary"]; !ok {
		t.Fatalf("expected summary in tree")
	}
	if _, ok := tree["assignee"]["displayName"]; !ok {
		t.Fatalf("expected assignee.displayName in tree")
	}
	if _, ok := tree["status"]["name"]; !ok {
		t.Fatalf("expected status.name in tree")
	}
	if _, ok := tree["parent"]["fields"]["status"]["name"]; !ok {
		t.Fatalf("expected parent.fields.status.name in tree")
	}
	if _, ok := tree["custom_field_10100"]; !ok {
		t.Fatalf("expected mapped custom field Story Points -> custom_field_10100")
	}
	if _, ok := tree["custom_field_10022"]; !ok {
		t.Fatalf("expected mapped custom field Sprint -> custom_field_10022")
	}
}

func TestNormalizeAndReverseCustomFields(t *testing.T) {
	custom := map[string]string{
		"Story Points": "custom_field_10100",
		"Sprint":       "custom_field_10022",
	}
	q := &JsonInputQuery{
		Fields: []string{"summary", "Story Points", "Sprint.name"},
	}
	normalizeInputFields(q, custom)
	want := []string{"summary", "custom_field_10100", "custom_field_10022.name"}
	if !reflect.DeepEqual(q.Fields, want) {
		t.Fatalf("normalizeInputFields got %v want %v", q.Fields, want)
	}

	rev := reverseCustomFields(custom)
	if got := rev["custom_field_10100"]; got != "Story Points" {
		t.Fatalf("reverseCustomFields wrong reverse for 10100: %v", got)
	}
}

func TestFilter_MapAndArray(t *testing.T) {
	j := &jiraProc{}
	// data represents a simplified issue.Fields payload
	data := map[string]any{
		"summary": "Fix bug",
		"assignee": map[string]any{
			"displayName": "Alice",
			"id":          "user-1",
		},
		"labels":             []any{"bug", "p1"},
		"custom_field_10100": 8, // Story Points
	}
	customRev := map[string]string{
		"custom_field_10100": "Story Points",
	}

	// selectors pick summary, assignee.displayName, labels, Story Points
	selectors := selectorTree{
		"summary":            {},
		"assignee":           {"displayName": {}},
		"labels":             {},
		"custom_field_10100": {},
	}

	out, err := j.filter(data, selectors, customRev)
	if err != nil {
		t.Fatalf("filter error: %v", err)
	}
	got := out.(map[string]any)

	if got["summary"] != "Fix bug" {
		t.Fatalf("missing summary")
	}
	if got["assignee"].(map[string]any)["displayName"] != "Alice" {
		t.Fatalf("missing assignee.displayName")
	}
	if _, ok := got["labels"]; !ok {
		t.Fatalf("missing labels")
	}
	// verify custom field key got remapped to real name
	if _, ok := got["Story Points"]; !ok {
		t.Fatalf("expected custom field key to be remapped to 'Story Points'")
	}
}
