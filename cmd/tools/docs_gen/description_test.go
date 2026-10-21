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
	"testing"

	"github.com/stretchr/testify/assert"
)

// These cases match the JavaScript generator's tests in
// docs-extensions-and-macros, so both generators render the same way.

func TestProtectCodeSpans(t *testing.T) {
	escape := func(s string) string { return protectCodeSpans(escapePlaceholderBraces(s)) }

	assert.Equal(t, "custom objects end with `+__c+`; Big Objects end with `+__b+`",
		escape("custom objects end with `__c`; Big Objects end with `__b`"))
	assert.Equal(t, "exclude `+'.git/**', '**/*.png'+`", escape("exclude `'.git/**', '**/*.png'`"))
	assert.Equal(t, "a `+__$x {name}+` span", escape("a `__$x {name}` span"))

	unchanged := "plain `snake_case` and `+__c+`\n\n----\nkey: `__c`\n----"
	assert.Equal(t, unchanged, escape(unchanged))
}

func TestNormalizeMetadata(t *testing.T) {
	assert.Contains(t,
		normalizeMetadata("== Metadata\n\n- lsn (The commit LSN. Not present on snapshot (`read`) messages.)"),
		"- `lsn` (The commit LSN. Not present on snapshot (`read`) messages.)")

	assert.Equal(t,
		"== Metadata\n\nIf HTTPS is enabled, the following fields are added as well:\n\n- `tls_version`\n- `tls_subject`",
		normalizeMetadata("== Metadata\n\nIf HTTPS is enabled, the following fields are added as well:\n- `tls_version`\n- `tls_subject`"))

	wrapped := "== Metadata\n\n- `a` (first line\n  continues)\n- `b`"
	assert.Equal(t, wrapped, normalizeMetadata(wrapped))
}

func TestMetaSummary(t *testing.T) {
	plain := "Reads from a queue.\nThe second line is a soft break."
	assert.Equal(t, plain, metaSummary(plain), "a paragraph with soft breaks is kept whole")

	changefeed := "Listens to a changefeed and creates a message for each row received. Each message is a json object looking like: \n```json\n{\"a\": 1}\n```"
	assert.Equal(t, "Listens to a changefeed and creates a message for each row received.", metaSummary(changefeed))
	assert.Equal(t, "First paragraph.", metaSummary("First paragraph.\n\nSecond paragraph."))
	assert.Equal(t, "Lists items:", metaSummary("Lists items:\n- one\n- two"))
	assert.Equal(t, "Reads rows.", metaSummary("Reads rows.\n\nifndef::env-cloud[]\nSelf-managed only.\nendif::[]"), "a conditional after the sentence is cut")
	assert.Equal(t, "Reads rows.", metaSummary("Reads rows.\nifndef::env-cloud[]\nSelf-managed only.\nendif::[]"), "a conditional glued to the sentence is cut")
	assert.Equal(t, "Reads rows.", metaSummary("Reads rows.\nifndef::env-cloud[Self-managed only.]"), "the single-line form is cut")
}

func TestCloudGuardXrefs(t *testing.T) {
	excluded := func(typeDir, name string) bool { return typeDir == "processors" && name == "grok" }

	got := cloudGuardXrefs("Faster than xref:components:processors/grok.adoc[`grok`].\nStill the same paragraph.\n\nSee xref:components:processors/mapping.adoc[mapping].", excluded)
	assert.Equal(t, "ifndef::env-cloud[]\n"+
		"Faster than xref:components:processors/grok.adoc[`grok`].\nStill the same paragraph.\n"+
		"endif::[]\nifdef::env-cloud[]\n"+
		"Faster than `grok`.\nStill the same paragraph.\n"+
		"endif::[]\n\nSee xref:components:processors/mapping.adoc[mapping].", got)

	assert.Contains(t, cloudGuardXrefs("Use xref:processors/grok.adoc[].", excluded), "Use `grok`.", "an xref with no text falls back to the component name")

	guarded := "ifndef::env-cloud[]\nUse xref:components:processors/grok.adoc[grok].\nendif::[]"
	assert.Equal(t, guarded, cloudGuardXrefs(guarded, excluded), "paragraphs already inside a conditional are left alone")

	code := "----\nxref:components:processors/grok.adoc[grok]\n----"
	assert.Equal(t, code, cloudGuardXrefs(code, excluded), "verbatim blocks are left alone")

	singleLine := "ifndef::env-cloud[Self-managed only.]\n\nSee xref:components:processors/grok.adoc[grok]."
	assert.Equal(t, "ifndef::env-cloud[Self-managed only.]\n\n"+
		"ifndef::env-cloud[]\nSee xref:components:processors/grok.adoc[grok].\nendif::[]\n"+
		"ifdef::env-cloud[]\nSee grok.\nendif::[]",
		cloudGuardXrefs(singleLine, excluded), "a single-line conditional has no endif, so later paragraphs are still guarded")

	cloudOnly := "ifdef::env-cloud[]\nUse xref:components:processors/grok.adoc[grok] instead.\nendif::[]\n\nAfter."
	assert.Equal(t, "ifdef::env-cloud[]\nUse grok instead.\nendif::[]\n\nAfter.",
		cloudGuardXrefs(cloudOnly, excluded), "inside an ifdef::env-cloud block the link is replaced in place")

	nested := "ifdef::env-cloud[]\nifndef::env-cloud[]\nUse xref:components:processors/grok.adoc[grok].\nendif::[]\nendif::[]\n\nSee xref:components:processors/grok.adoc[grok]."
	assert.Equal(t, "ifdef::env-cloud[]\nifndef::env-cloud[]\nUse xref:components:processors/grok.adoc[grok].\nendif::[]\nendif::[]\n\n"+
		"ifndef::env-cloud[]\nSee xref:components:processors/grok.adoc[grok].\nendif::[]\n"+
		"ifdef::env-cloud[]\nSee grok.\nendif::[]",
		cloudGuardXrefs(nested, excluded), "an ifndef::env-cloud block hides its content in Cloud even when nested, and the depth unwinds")
}

func TestRenderDescriptionPartial(t *testing.T) {
	c := componentSpec{
		Name:        "zmq4",
		Status:      "beta",
		Summary:     "Consumes messages from a ZeroMQ socket. Each message is like:\n```json\n{}\n```",
		Description: "Reads messages.",
		Version:     "4.1.0",
		Footnotes:   "== Patterns\n\nUse xref:components:processors/grok.adoc[grok].",
	}
	excluded := func(_, name string) bool { return name == "grok" }

	got := renderDescriptionPartial(c, "inputs", componentPlatform{CgoOnly: true}, excluded)
	assert.Contains(t, got, "// tag::meta[]\n:description: Consumes messages from a ZeroMQ socket.\n:status: beta\n:page-cgo-only: true\n// end::meta[]\n")
	assert.Contains(t, got, "// tag::body[]\n[CAUTION]\n====\nThis component is in beta.")
	assert.Contains(t, got, "ifndef::env-cloud[]\nIntroduced in version 4.1.0.\nendif::[]")
	assert.Contains(t, got, "// tag::footnotes[]\n== Patterns\n\nUse xref:components:processors/grok.adoc[grok].\n// end::footnotes[]\n")
	assert.NotContains(t, got, "ifdef::env-cloud[]", "components that Cloud doesn't include keep their links")

	cloud := renderDescriptionPartial(c, "inputs", componentPlatform{Cloud: true}, excluded)
	assert.NotContains(t, cloud, ":page-cgo-only:")
	assert.Contains(t, cloud, "ifdef::env-cloud[]\nUse grok.\nendif::[]", "Cloud components unlink components that Cloud doesn't include")

	c.Status, c.Footnotes = "deprecated", ""
	deprecated := renderDescriptionPartial(c, "inputs", componentPlatform{}, nil)
	assert.Contains(t, deprecated, "// tag::body[]\n[WARNING]\n====\nThis component is deprecated")
	assert.Contains(t, deprecated, "// tag::footnotes[]\n// end::footnotes[]\n", "the footnotes tag is always present")

	c.Status = "stable"
	stable := renderDescriptionPartial(c, "inputs", componentPlatform{}, nil)
	assert.Contains(t, stable, ":status: stable\n")
	assert.Contains(t, stable, "// tag::body[]\nConsumes messages", "stable components get no notice")

	for _, tag := range []string{"meta", "body", "footnotes"} {
		assert.Contains(t, emptyDescriptionPartial, "// tag::"+tag+"[]\n// end::"+tag+"[]\n")
	}
}

// The Metadata section moves into its own partial. A conditional around it
// must stay whole on one side of the cut, or the description and metadata
// partials each get half of it.
func TestMetadataSectionKeepsConditionalsBalanced(t *testing.T) {
	d := "Reads messages.\n\nifndef::env-cloud[]\n\n== Metadata\n\nThis input adds the following metadata fields:\n\n- foo\n\nendif::[]"
	body := renderDescriptionBody(d, "inputs", "probe")
	meta := extractMetadata(d)
	assert.NotContains(t, meta, "endif::[]", "the closing directive stays with its opener")
	assert.Contains(t, body, "ifndef::env-cloud[]\n\ninclude::connect:components:partial$metadata/inputs/probe.adoc[]\n\nendif::[]")

	// A conditional the section opens without closing is not split: the
	// section stays in the description.
	open := "Reads messages.\n\n== Metadata\n\nifndef::env-cloud[]\n\n- foo\n\n== Next\n\nendif::[]"
	assert.Empty(t, extractMetadata(open))
	assert.Equal(t, open, descriptionWithMetadataInclude(open, "inputs", "probe"))

	// A balanced conditional inside the section moves with it.
	inside := "Reads messages.\n\n== Metadata\n\n- foo\n\nifndef::env-cloud[]\n- bar\nendif::[]"
	assert.Contains(t, extractMetadata(inside), "ifndef::env-cloud[]\n- bar\nendif::[]")
}
