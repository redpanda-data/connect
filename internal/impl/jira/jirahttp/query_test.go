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

package jirahttp

import (
	"encoding/json"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestExtractExpandableFields(t *testing.T) {
	in := []string{
		"summary",
		"changelog.histories.items",
		"status.name",
		"renderedFields.description",
		"schema",
		"assignee.displayName",
	}
	got := extractExpandableFields(in)

	// Expect only the ones rooted at expandable top-level keys
	wantSet := map[string]struct{}{
		"changelog.histories.items":  {},
		"renderedFields.description": {},
		"schema":                     {},
	}
	if len(got) != len(wantSet) {
		t.Fatalf("expandable mismatch, got %v", got)
	}
	for _, v := range got {
		if _, ok := wantSet[v]; !ok {
			t.Fatalf("unexpected expandable field: %s", v)
		}
	}
}

func TestParseOperatorField(t *testing.T) {
	type tc struct {
		in      string
		op, val string
		ok      bool
	}
	cases := []tc{
		{">= 2024-01-01", ">=", "2024-01-01", true},
		{"<= 1d", "<=", "1d", true},
		{"> -2w", ">", "-2w", true},
		{"<1h", "<", "1h", true},
		{"= 2025/12/31 14:00", "=", "2025/12/31 14:00", true},
		{"!= foo", "!=", "foo", true},
		{"no-op 1d", "", "", false},
	}
	for _, c := range cases {
		op, val, err := parseOperatorField(c.in)
		if c.ok && err != nil {
			t.Fatalf("parseOperatorField(%q) unexpected err: %v", c.in, err)
		}
		if !c.ok && err == nil {
			t.Fatalf("parseOperatorField(%q) expected error", c.in)
		}
		if op != c.op || val != c.val {
			t.Fatalf("parseOperatorField(%q) got (%q,%q) want (%q,%q)", c.in, op, val, c.op, c.val)
		}
	}
}

func TestExtractQueryFromMessage(t *testing.T) {
	j := &JiraProc{}
	input := jsonInputQuery{
		Resource: "issue",
		Project:  "DEMO",
		Fields:   []string{"summary", "status.name"},
		JQL:      "",
		Updated:  "> -1d",
		Created:  "< 2025-01-01",
	}
	raw, _ := json.Marshal(input)
	msg := service.NewMessage(raw)

	got, err := j.extractQueryFromMessage(msg)
	if err != nil {
		t.Fatalf("extractQueryFromMessage error: %v", err)
	}

	if got.Project != "DEMO" || got.Resource != "issue" {
		t.Fatalf("unexpected parse result: %+v", got)
	}
}

func TestExtractQueryFromMessage_InvalidJSON(t *testing.T) {
	j := &JiraProc{}
	msg := service.NewMessage([]byte("{not-json}"))
	if _, err := j.extractQueryFromMessage(msg); err == nil {
		t.Fatalf("expected error for invalid json")
	}
}
