// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package jirahttp

import (
	"reflect"
	"testing"
)

func TestTransformIssue(t *testing.T) {
	orig := Issue{
		ID:  "10001",
		Key: "DEMO-1",
		Fields: map[string]any{
			"summary": "Hello",
		},
		Changelog: map[string]any{"total": 2},
	}
	out := transformIssue(orig)
	if out.ID != "10001" || out.Key != "DEMO-1" {
		t.Fatalf("id/key mismatch")
	}
	fields := out.Fields.(map[string]any)
	if fields["summary"] != "Hello" {
		t.Fatalf("missing summary")
	}
	if _, ok := fields["changelog"]; !ok {
		t.Fatalf("expected changelog injected into fields")
	}
}

func TestTransformProject(t *testing.T) {
	in := map[string]any{"id": "P1", "key": "DEMO", "name": "Demo project"}
	out := transformProject(in)
	if out.ID != "P1" || out.Key != "DEMO" {
		t.Fatalf("id/key mismatch")
	}
	if !reflect.DeepEqual(out.Fields.(map[string]any)["name"], "Demo project") {
		t.Fatalf("missing field copy")
	}
}

func TestTransformProjectType(t *testing.T) {
	in := map[string]any{"key": "business", "formattedKey": "Business"}
	out := transformProjectType(in)
	if out.Key != "business" || out.FormattedKey != "Business" {
		t.Fatalf("key/formattedKey mismatch")
	}
}

func TestTransformProjectCategory(t *testing.T) {
	in := map[string]any{"id": "10010", "name": "Internal"}
	out := transformProjectCategory(in)
	if out.ID != "10010" {
		t.Fatalf("id mismatch")
	}
}
