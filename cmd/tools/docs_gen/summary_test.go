// Copyright 2026 Redpanda Data, Inc.
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

package main

import (
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/public/schema"
)

// summaryExceptions are the components whose Summary breaks summaryProblem
// today. The benthos ones can't be fixed in this repository. The test fails
// when an entry no longer breaks the rule, so the list only shrinks: fixing a
// Summary means removing its entry.
var summaryExceptions = map[string]string{}

// summaryProblem returns why a Summary can't become a clean :description:
// attribute and catalog summary, or "" when it can. A Summary must be one
// line of text with no block content, such as a code fence or a list.
func summaryProblem(summary string) string {
	s := strings.TrimFunc(summary, jsIsSpace)
	switch {
	case s == "":
		return "is empty"
	case summaryHasBlocks(s):
		return "has block content, such as a code fence, a list, or a second paragraph"
	case strings.Contains(s, "\n"):
		return "spans more than one line"
	}
	return ""
}

func TestSummaryProblem(t *testing.T) {
	assert.Empty(t, summaryProblem("Reads messages from a queue."))
	assert.NotEmpty(t, summaryProblem(""))
	assert.NotEmpty(t, summaryProblem("Reads messages\nfrom a queue."))
	assert.NotEmpty(t, summaryProblem("Reads rows like:\n```json\n{}\n```"))
}

// TestComponentSummariesAreOneLine checks every documented component's
// Summary, which becomes the page :description: and the catalog summary.
func TestComponentSummariesAreOneLine(t *testing.T) {
	full, err := marshalSchema(schema.Standard("", "").MarshalJSONV0())
	require.NoError(t, err)

	failing := map[string]string{}
	for _, g := range full.Groups {
		for _, c := range g.Components {
			if c.Name == "" {
				continue
			}
			if p := summaryProblem(c.Summary); p != "" {
				failing[componentKey(g.Key, c.Name)] = p
			}
		}
	}
	var keys []string
	for k := range failing {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if _, ok := summaryExceptions[k]; ok {
			t.Logf("skipping %v: its Summary %v, and it is %v", k, failing[k], summaryExceptions[k])
			continue
		}
		t.Errorf("%v: the Summary %v. Make it one sentence and move the rest into the Description.", k, failing[k])
	}
	for k, why := range summaryExceptions {
		if _, ok := failing[k]; !ok {
			t.Errorf("%v (%v) no longer breaks the Summary rule, so remove it from summaryExceptions", k, why)
		}
	}
}
