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
