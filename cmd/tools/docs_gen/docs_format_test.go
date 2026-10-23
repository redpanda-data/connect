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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/docslinks"
	"github.com/redpanda-data/connect/v4/public/schema"
)

// Doc strings reach readers through the docs site as AsciiDoc and through
// each distribution's `list --format json-full --docs-format markdown` as
// Markdown, which the Redpanda Cloud UI shows. These tests run every spec in
// every distribution through both checks so a mistake fails here, in the PR
// that introduces it, rather than showing up as raw markup or missing text.

var markdownLeftovers = map[string]*regexp.Regexp{
	"xref":        regexp.MustCompile(`xref:`),
	"anchor":      regexp.MustCompile(`<<[a-z0-9_.-]+(,[^>]*)?>>`),
	"conditional": regexp.MustCompile(`(?m)^(ifn?def|endif)::`),
	"delimiter":   regexp.MustCompile(`(?m)^(====|----|\|===|\[tabs\])$`),
	"link macro":  regexp.MustCompile(`https?://[^\s)]+\[`),
	"raw target":  regexp.MustCompile(`\.adoc\]\(`),
	"bold":        regexp.MustCompile(`(^|[\s(])\*[^*\s]([^*\n]*[^*\s])?\*([\s.,;:!?)]|$)`),
}

// Code carried as data, not prose. A component's field tree also sits under a
// "config" key, as an object, so only a "config" string (an example's YAML
// snippet) is skipped: field descriptions must still be checked. A field's
// "examples" are values, not prose, so they are skipped too.
var docCodeKeys = map[string]bool{"mapping": true, "results": true, "default": true, "linter": true}

func walkDocStrings(v any, path, key string, fn func(path, key, s string)) {
	if docCodeKeys[key] {
		return
	}
	switch x := v.(type) {
	case string:
		if key == "config" {
			return
		}
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
		_, isField := x["kind"]
		for k, e := range x {
			if isField && k == "examples" {
				continue
			}
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

	walkDocStrings(doc, "", "", func(path, _ string, s string) {
		for _, p := range service.LintDocsConditionals(s) {
			assert.Fail(t, "malformed conditional", "%v: %v", path, p)
		}
	})
}

// docsSiteURL finds a link written as a docs.redpanda.com URL. The API
// reference under /api/ is not part of the Antora site, so it has no xref.
var docsSiteURL = regexp.MustCompile(`https?://docs\.redpanda\.com/\S*`)

func docsSiteURLs(s string) []string {
	var urls []string
	for _, u := range docsSiteURL.FindAllString(s, -1) {
		if !strings.Contains(u, "docs.redpanda.com/api/") {
			urls = append(urls, u)
		}
	}
	return urls
}

// A docs.redpanda.com URL in a doc string always points at one docs site, so
// a Cloud reader lands on the self-managed page and the link breaks when the
// page moves. An xref resolves to each distribution's own copy.
func TestDocStringsLinkDocsWithXrefs(t *testing.T) {
	for name, s := range distributions() {
		t.Run(name, func(t *testing.T) {
			raw, err := s.MarshalJSONV0()
			require.NoError(t, err)
			var doc any
			require.NoError(t, json.Unmarshal(raw, &doc))

			walkDocStrings(doc, "", "", func(path, _ string, s string) {
				for _, u := range docsSiteURLs(s) {
					assert.Fail(t, "docs.redpanda.com URL in a doc string", "%v: %v. Link to docs pages with an xref, such as xref:get-started:licensing.adoc[licensing docs] or xref:streaming:manage:x.adoc[text], so each distribution links to its own copy (see docs/README.md).", path, u)
				}
			})
		})
	}
}

func TestDocsSiteURLs(t *testing.T) {
	assert.Equal(t, []string{"https://docs.redpanda.com/redpanda-connect/get-started/licensing/[licensing"}, docsSiteURLs("requires a license (https://docs.redpanda.com/redpanda-connect/get-started/licensing/[licensing docs^])."))
	assert.Equal(t, []string{"https://docs.redpanda.com/current/reference/properties/cluster-properties/#kafka_batch_max_bytes."}, docsSiteURLs("see https://docs.redpanda.com/current/reference/properties/cluster-properties/#kafka_batch_max_bytes."))
	assert.Len(t, docsSiteURLs("http://docs.redpanda.com/connect/ and https://docs.redpanda.com/streaming/"), 2)
	assert.Empty(t, docsSiteURLs("see link:https://docs.redpanda.com/api/doc/cloud-controlplane/authentication[API authentication^]"), "the API reference is not an Antora page")
	assert.Empty(t, docsSiteURLs("see xref:get-started:licensing.adoc[licensing docs] or https://www.redpanda.com/upgrade[upgrade^]"))
}

// Each pattern must catch the markup it names and leave correct Markdown
// alone, or the test above either misses leftovers or fails on good text.
func TestMarkdownLeftoverPatterns(t *testing.T) {
	cases := []struct {
		kind  string
		match []string
		clean []string
	}{
		{
			kind:  "anchor",
			match: []string{"see <<batching>>", "see <<max_in_flight,max in flight>>.", "<<tls.enabled>>"},
			clean: []string{"a default of `1<<20` bytes", "`x << 2` and `y >> 2`", "`a<<b`, `c>>d`", "see [batching](#batching)"},
		},
		{
			kind:  "bold",
			match: []string{"Set this to *true*.", "the *a* flag", "*Important*: read this", "it is *not* safe", "(*beta*)", "*all*"},
			clean: []string{"Set this to **true**.", "matches `*.json` files", "a * b * c", "use a glob such as `*foo*`", "2*3*4", "**a**"},
		},
	}
	for _, c := range cases {
		rx := markdownLeftovers[c.kind]
		require.NotNil(t, rx, c.kind)
		for _, s := range c.match {
			assert.True(t, rx.MatchString(s), "%v should match %q", c.kind, s)
		}
		for _, s := range c.clean {
			assert.False(t, rx.MatchString(s), "%v should not match %q", c.kind, s)
		}
	}
}

// The docs each distribution's doc strings link to, written out rather than
// taken from docslinks so a wrong switch in MarkdownOptions fails here.
var wantDocsURL = map[string]string{
	"standard": "https://docs.redpanda.com/connect",
	"cloud":    "https://docs.redpanda.com/cloud-data-platform/develop/connect",
	"cloud-ai": "https://docs.redpanda.com/cloud-data-platform/develop/connect",
}

func TestDocStringsRenderAsMarkdownInEveryDistribution(t *testing.T) {
	for name, s := range distributions() {
		t.Run(name, func(t *testing.T) {
			if name != "standard" {
				require.Equal(t, []string{service.DocsAttributeCloud}, s.DocsAttributes(), "Cloud distributions declare env-cloud so their readers see the Cloud text")
			}
			opts := docslinks.MarkdownOptions(s)
			assert.Equal(t, wantDocsURL[name], opts.DocumentationURL, "the binaries pass these options through docslinks.CLIOpts")
			out, err := s.MarshalJSONV0Markdown(opts)
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

// The checks above only protect what walkDocStrings visits. A field's
// description sits inside the component's "config" object, so a probe with
// mistakes only in a field description must still be caught.
func TestWalkDocStringsVisitsFieldDescriptions(t *testing.T) {
	env := service.NewEmptyEnvironment()
	spec := service.NewConfigSpec().
		Summary("Probe.").
		Field(service.NewStringField("probe_field").
			Description("Starts here.\n\nifndef::env-cloud[]\nNever closed.").
			Example("<<not prose>>"))
	require.NoError(t, env.RegisterInput("zz_probe", spec,
		func(*service.ParsedConfig, *service.Resources) (service.Input, error) { return nil, nil }))

	raw, err := env.FullConfigSchema("0.0.0", "now").MarshalJSONV0()
	require.NoError(t, err)
	var doc any
	require.NoError(t, json.Unmarshal(raw, &doc))

	var lintHits, exampleHits []string
	walkDocStrings(doc, "", "", func(path, _ string, s string) {
		if len(service.LintDocsConditionals(s)) > 0 {
			lintHits = append(lintHits, path)
		}
		if strings.Contains(s, "<<not prose>>") {
			exampleHits = append(exampleHits, path)
		}
	})
	assert.Equal(t, []string{"/inputs/0/zz_probe/config/children/0/probe_field/description"}, lintHits, "the field description is checked")
	assert.Empty(t, exampleHits, "field examples are values, not prose")
}

// The Cloud binary compiles out the components Cloud does not ship, so a link
// to one of them must render as text, while a link to a component Cloud has,
// or to a docs page under components/ that is not a component, stays a link.
func TestDocsHasComponentKeepsPagesAndShippedComponents(t *testing.T) {
	cloud := schema.Cloud("0.0.0", "now")
	has := docslinks.HasComponent(cloud.Environment())
	assert.True(t, has("inputs", "generate"), "Cloud ships generate")
	assert.False(t, has("inputs", "file"), "Cloud does not ship the file input")
	assert.True(t, has("outputs", "bigquery_cdc_migration"), "a guide page under components/ is not a component")
	assert.True(t, has("inputs", "about"), "the about page is not a component")
	assert.True(t, docslinks.HasComponent(schema.Standard("0.0.0", "now").Environment())("inputs", "file"), "self-managed ships the file input")

	out, err := cloud.MarshalJSONV0Markdown(docslinks.MarkdownOptions(cloud))
	require.NoError(t, err)
	var doc struct {
		Outputs []struct {
			Name        string `json:"name"`
			Description string `json:"description"`
		} `json:"outputs"`
	}
	require.NoError(t, json.Unmarshal(out, &doc))
	var desc string
	for _, o := range doc.Outputs {
		if o.Name == "gcp_bigquery_write_api" {
			desc = o.Description
		}
	}
	require.NotEmpty(t, desc, "Cloud ships gcp_bigquery_write_api")
	assert.Contains(t, desc, "[CDC migration guide]("+wantDocsURL["cloud"]+"/components/outputs/bigquery_cdc_migration/)")
}
