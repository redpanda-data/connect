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
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/public/schema"
)

// Doc strings reach readers through the docs site as AsciiDoc and through
// each distribution's `list --format json-full --docs-format markdown` as
// Markdown, which the Redpanda Cloud UI shows. These tests run every spec in
// every distribution through both checks so a mistake fails here, in the PR
// that introduces it, rather than showing up as raw markup or missing text.

var markdownLeftovers = map[string]*regexp.Regexp{
	"xref":        regexp.MustCompile(`xref:`),
	"anchor":      regexp.MustCompile(`<<[^\s<]|[^\s>]>>`),
	"conditional": regexp.MustCompile(`(?m)^(ifn?def|endif)::`),
	"delimiter":   regexp.MustCompile(`(?m)^(====|----|\|===|\[tabs\])$`),
	"link macro":  regexp.MustCompile(`https?://[^\s)]+\[`),
	"raw target":  regexp.MustCompile(`\.adoc\]\(`),
	"bold":        regexp.MustCompile(`(^|\s)\*[^*\s][^*\n]*[^*\s]\*(\s|$)`),
}

// Code carried as data, not prose.
var docCodeKeys = map[string]bool{"config": true, "mapping": true, "results": true, "default": true}

func walkDocStrings(v any, path, key string, fn func(path, key, s string)) {
	if docCodeKeys[key] {
		return
	}
	switch x := v.(type) {
	case string:
		fn(path, key, x)
	case []any:
		for i, e := range x {
			walkDocStrings(e, path+"/"+strconv.Itoa(i), key, fn)
		}
	case map[string]any:
		name, _ := x["name"].(string)
		if name != "" {
			path += "/" + name
		}
		for k, e := range x {
			walkDocStrings(e, path+"/"+k, k, fn)
		}
	}
}

func distributions() map[string]*service.ConfigSchema {
	return map[string]*service.ConfigSchema{
		"standard": schema.Standard("0.0.0", "now"),
		"cloud":    schema.Cloud("0.0.0", "now"),
		"cloud-ai": schema.CloudAI("0.0.0", "now"),
	}
}

func TestDocStringConditionalsAreWellFormed(t *testing.T) {
	raw, err := schema.Standard("0.0.0", "now").MarshalJSONV0()
	require.NoError(t, err)
	var doc any
	require.NoError(t, json.Unmarshal(raw, &doc))

	walkDocStrings(doc, "", "", func(path, key, s string) {
		for _, p := range service.LintDocsConditionals(s) {
			assert.Fail(t, "malformed conditional", "%v: %v", path, p)
		}
		if key == "summary" && (strings.Contains(s, "ifdef::") || strings.Contains(s, "ifndef::")) {
			assert.Fail(t, "conditional in a summary", "%v: summaries render as one line in catalogs and page metadata", path)
		}
	})
}

func TestDocStringsRenderAsMarkdownInEveryDistribution(t *testing.T) {
	for name, s := range distributions() {
		t.Run(name, func(t *testing.T) {
			url := schema.DocumentationURL
			if name != "standard" {
				url = schema.CloudDocumentationURL
				require.Equal(t, []string{service.DocsAttributeCloud}, s.DocsAttributes(), "Cloud distributions declare env-cloud so their readers see the Cloud text")
			}
			out, err := s.MarshalJSONV0Markdown(service.DocsMarkdownOptions{DocumentationURL: url, ComponentURLs: schema.DocsComponentURLs})
			require.NoError(t, err)
			var doc map[string]any
			require.NoError(t, json.Unmarshal(out, &doc))
			assert.Equal(t, "markdown", doc["docs_format"])

			walkDocStrings(doc, "", "", func(path, _ string, s string) {
				for kind, rx := range markdownLeftovers {
					assert.False(t, rx.MatchString(s), "%v: leftover %v in %q", path, kind, s)
				}
			})
		})
	}
}
