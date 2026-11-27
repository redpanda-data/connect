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
	"strings"
	"testing"

	"github.com/redpanda-data/connect/v4/public/schema"
)

// firstHeading returns the first heading line outside verbatim blocks and
// its marker length (2 for "==" or "##", 3 for "===" or "###"). The depth
// is 0 when the text has no heading.
func firstHeading(text string) (string, int) {
	for _, l := range annotateLines(text) {
		if !l.verbatim && headingLine.MatchString(l.text) {
			return l.text, len(l.text) - len(strings.TrimLeft(l.text, l.text[:1]))
		}
	}
	return "", 0
}

func TestFirstHeading(t *testing.T) {
	for _, tc := range []struct {
		name string
		text string
		want int
	}{
		{"none", "Just a paragraph.", 0},
		{"asciidoc level 1", "Intro.\n\n== Metadata\n\n=== Sub", 2},
		{"markdown level 1", "Intro.\n\n## Metadata", 2},
		{"starts too deep", "Intro.\n\n### Shaping data", 3},
		{"heading in a listing is ignored", "----\n### not a heading\n----\n\n== Real", 2},
		{"heading in a fence is ignored", "```yaml\n## comment\n```\n\n=== Too deep", 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, got := firstHeading(tc.text); got != tc.want {
				t.Errorf("firstHeading depth = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestDescriptionHeadingLevels fails when a component description's first
// heading is deeper than a level 1 section ("==" or "##"). The description
// renders under the page title, so a first heading at "===" or "###" has no
// parent section and Asciidoctor reports "section title out of sequence" on
// the published page. It's a plain unit test so it also runs on fork PRs,
// which skip the docs-check job.
func TestDescriptionHeadingLevels(t *testing.T) {
	raw, err := schema.Standard("", "").MarshalJSONV0()
	if err != nil {
		t.Fatal(err)
	}
	full, err := parseFullSchema(raw)
	if err != nil {
		t.Fatal(err)
	}

	for _, g := range full.Groups {
		for _, c := range g.Components {
			if line, d := firstHeading(c.Description); d > 2 {
				t.Errorf("%s/%s: the description's first heading %q is below level 1, so it has no parent section; use == (or ##) for its top-level headings",
					pageTypeDir(g.Key), c.Name, line)
			}
		}
	}
}
