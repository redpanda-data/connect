// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package jirahttp

import (
	"testing"
)

func TestParseResource(t *testing.T) {
	cases := []struct {
		in      string
		wantErr bool
	}{
		{"issue", false},
		{"issue_transition", false},
		{"role", false},
		{"user", false},
		{"project_version", false},
		{"project", false},
		{"project_category", false},
		{"project_type", false},
		{"", true},
		{"unknown", true},
	}

	for _, c := range cases {
		_, err := parseResource(c.in)
		if (err != nil) != c.wantErr {
			t.Fatalf("parseResource(%q) error=%v wantErr=%v", c.in, err, c.wantErr)
		}
	}
}
