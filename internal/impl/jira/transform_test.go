package jira

import (
	"reflect"
	"testing"
)

func TestTransformIssue(t *testing.T) {
	orig := Issue{
		ID:  "10001",
		Key: "DEMO-1",
		Fields: map[string]interface{}{
			"summary": "Hello",
		},
		Changelog: map[string]interface{}{"total": 2},
	}
	out := TransformIssue(orig)
	if out.ID != "10001" || out.Key != "DEMO-1" {
		t.Fatalf("id/key mismatch")
	}
	fields := out.Fields.(map[string]interface{})
	if fields["summary"] != "Hello" {
		t.Fatalf("missing summary")
	}
	if _, ok := fields["changelog"]; !ok {
		t.Fatalf("expected changelog injected into fields")
	}
}

func TestTransformProject(t *testing.T) {
	in := map[string]interface{}{"id": "P1", "key": "DEMO", "name": "Demo Project"}
	out := TransformProject(in)
	if out.ID != "P1" || out.Key != "DEMO" {
		t.Fatalf("id/key mismatch")
	}
	if !reflect.DeepEqual(out.Fields.(map[string]interface{})["name"], "Demo Project") {
		t.Fatalf("missing field copy")
	}
}

func TestTransformProjectType(t *testing.T) {
	in := map[string]interface{}{"key": "business", "formattedKey": "Business"}
	out := TransformProjectType(in)
	if out.Key != "business" || out.FormattedKey != "Business" {
		t.Fatalf("key/formattedKey mismatch")
	}
}

func TestTransformProjectCategory(t *testing.T) {
	in := map[string]interface{}{"id": "10010", "name": "Internal"}
	out := TransformProjectCategory(in)
	if out.ID != "10010" {
		t.Fatalf("id mismatch")
	}
}
