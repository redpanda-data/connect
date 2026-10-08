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
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"golang.org/x/text/collate"
	"golang.org/x/text/language"
)

var (
	exampleHeading5 = regexp.MustCompile(`(?m)^#####\s+(.+)$`)
	exampleHeading4 = regexp.MustCompile(`(?m)^####\s+(.+)$`)
	exampleHeading3 = regexp.MustCompile(`(?m)^###\s+(.+)$`)
)

var htmlEscaper = strings.NewReplacer(
	"&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;", "'", "&#x27;", "`", "&#x60;", "=", "&#x3D;",
)

// ensurePeriod adds a full stop to text that doesn't end in punctuation.
func ensurePeriod(text string) string {
	trimmed := strings.TrimFunc(text, jsIsSpace)
	if trimmed == "" || strings.HasSuffix(trimmed, ".") || strings.HasSuffix(trimmed, "!") || strings.HasSuffix(trimmed, "?") {
		return text
	}
	return trimmed + "."
}

func renderBloblangExample(ex bloblangExample) string {
	var leadIn string
	if summary := strings.TrimFunc(ex.Summary, jsIsSpace); summary != "" {
		summary = exampleHeading5.ReplaceAllString(summary, "=== ${1}")
		summary = exampleHeading4.ReplaceAllString(summary, "== ${1}")
		summary = exampleHeading3.ReplaceAllString(summary, "== ${1}")
		switch {
		case strings.HasSuffix(summary, "."), strings.HasSuffix(summary, "!"), strings.HasSuffix(summary, "?"):
			summary = summary[:len(summary)-1] + ":"
		case !strings.HasSuffix(summary, ":"):
			summary += ":"
		}
		leadIn = summary + "\n\n"
	}
	var code strings.Builder
	code.WriteString(strings.TrimFunc(ex.Mapping, jsIsSpace) + "\n")
	for _, r := range ex.Results {
		code.WriteString("\n# In:  " + r[0] + "\n# Out: " + r[1] + "\n")
	}
	return leadIn + "[,bloblang]\n----\n" + strings.TrimFunc(code.String(), jsIsSpace) + "\n----\n"
}

// withCategoryText returns a method spec that uses the text its categories
// give it, as the benthos docs did: the first category description replaces
// the method description. The examples are those of every category, followed
// by any method example that no category repeats, so none are lost when a
// method is in several categories.
func withCategoryText(spec bloblangSpec) bloblangSpec {
	var examples []bloblangExample
	seen := map[string]bool{}
	add := func(ex bloblangExample) {
		key := fmt.Sprintf("%q %q %q", ex.Summary, ex.Mapping, ex.Results)
		if !seen[key] {
			seen[key] = true
			examples = append(examples, ex)
		}
	}
	description := ""
	for _, c := range spec.Categories {
		if description == "" {
			description = strings.TrimSpace(c.Description)
		}
		for _, ex := range c.Examples {
			add(ex)
		}
	}
	for _, ex := range spec.Examples {
		add(ex)
	}
	if description != "" {
		spec.Description = description
	}
	spec.Examples = examples
	return spec
}

// categoryVariants returns, for a method whose categories describe it
// differently, one spec per category with that category's own description
// and examples, keyed by category. It returns nil when the categories share
// one text, so the method's single partial serves every category.
func categoryVariants(spec bloblangSpec) map[string]bloblangSpec {
	texts := map[string]bool{}
	for _, c := range spec.Categories {
		if c.Category == "" {
			continue
		}
		texts[fmt.Sprintf("%q %v", strings.TrimSpace(c.Description), c.Examples)] = true
	}
	if len(texts) < 2 {
		return nil
	}
	variants := map[string]bloblangSpec{}
	for _, c := range spec.Categories {
		if c.Category == "" {
			continue
		}
		v := spec
		if d := strings.TrimSpace(c.Description); d != "" {
			v.Description = d
		}
		if len(c.Examples) > 0 {
			v.Examples = c.Examples
		}
		variants[c.Category] = v
	}
	return variants
}

// categoryVariantName is the partial name of a method's variant for a
// category, such as length-string_manipulation. Method names have no hyphens,
// so a variant can't collide with a method.
func categoryVariantName(method, category string) string {
	slug := strings.Trim(nonSlug.ReplaceAllString(strings.ToLower(category), "_"), "_")
	return method + "-" + slug
}

var nonSlug = regexp.MustCompile(`[^a-z0-9]+`)

// renderBloblangSpec renders the reference partial for one function or
// method. kind is "function" or "method".
func renderBloblangSpec(spec bloblangSpec, kind string) string {
	var b strings.Builder
	b.WriteString(generatedBanner + "\n\n= " + htmlEscaper.Replace(spec.Name) + "\n")
	if notice := statusNotice(spec.Status, kind); notice != "" {
		b.WriteString("\n" + notice)
	}
	if spec.Description != "" {
		b.WriteString("\n" + ensurePeriod(spec.Description) + "\n")
	}
	b.WriteString("\n")
	if spec.Params != nil && len(spec.Params.Named) > 0 {
		b.WriteString("\n== Parameters\n\n[cols=\"1,1,3\"]\n|===\n| Name | Type | Description\n\n")
		for _, p := range spec.Params.Named {
			b.WriteString("| `" + htmlEscaper.Replace(p.Name) + "`")
			if p.optional() {
				b.WriteString(" (optional)")
			}
			desc := p.Description
			if d, ok := bloblangDefault(p.Default); ok {
				desc = strings.TrimFunc(ensurePeriod(desc)+" Default: "+d+".", jsIsSpace)
			}
			b.WriteString("\n| `" + htmlEscaper.Replace(p.Type) + "`\n| " + desc + "\n\n")
		}
		b.WriteString("|===\n\n")
	} else if spec.Params != nil && spec.Params.Variadic {
		b.WriteString("\n== Parameters\n\nThis " + kind + " accepts any number of arguments.\n\n")
	}
	b.WriteString("\n")
	if len(spec.Examples) > 0 {
		b.WriteString("== Examples\n\n")
		for _, ex := range spec.Examples {
			b.WriteString(renderBloblangExample(ex) + "\n")
		}
	}
	b.WriteString("\n")
	return b.String()
}

// statusNotice returns the admonition for a component, function, or method
// whose status warrants one, or "" for stable and hidden specs. kind names the
// thing in the notice, for example "component" or "method".
func statusNotice(status, kind string) string {
	switch status {
	case "deprecated":
		return "[WARNING]\n====\nThis " + kind + " is deprecated and will be removed in a future version.\n====\n"
	case "beta":
		return "[CAUTION]\n====\nThis " + kind + " is in beta. Its behavior might change in a future release.\n====\n"
	case "experimental":
		return "[CAUTION]\n====\nThis " + kind + " is experimental. It might change or be removed in a future release.\n====\n"
	}
	return ""
}

// bloblangDefault formats a parameter default as an inline code span. It
// reports false when there is no default, or when the default is an object or
// array, which is how benthos marshals defaults built in Go (random_int seed,
// for example, marshals as {"Value": 0}) rather than a value a user can type.
func bloblangDefault(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 {
		return "", false
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err != nil {
		return "", false
	}
	v := compact.String()
	if strings.HasPrefix(v, "{") || strings.HasPrefix(v, "[") {
		return "", false
	}
	if strings.ContainsAny(v, "+`") {
		return "`" + htmlEscaper.Replace(v) + "`", true
	}
	// A passthrough keeps quotes and whitespace, such as an indent of four
	// spaces, exactly as the schema declares them.
	return "`+" + v + "+`", true
}

// visibleBloblang drops the functions and methods that benthos hides from the
// docs, such as `var` and `nothing`, so neither their partials nor the list
// includes are written.
func visibleBloblang(specs []bloblangSpec) []bloblangSpec {
	var out []bloblangSpec
	for _, s := range specs {
		if s.Name != "" && s.Status != "hidden" {
			out = append(out, s)
		}
	}
	return out
}

// renderFunctionsList renders the includes that make up the Bloblang
// functions reference. Functions that the Redpanda Cloud build doesn't allow
// (inCloud is false) are wrapped so that Cloud docs leave them out.
func renderFunctionsList(specs []bloblangSpec, inCloud map[string]bool) string {
	var names []string
	for _, s := range visibleBloblang(specs) {
		names = append(names, s.Name)
	}
	sort.Strings(names)
	var b strings.Builder
	b.WriteString(generatedBanner + "\n")
	for _, n := range names {
		b.WriteString(selfManagedOnly("include::connect:components:partial$bloblang-functions/"+n+".adoc[leveloffset=+1]\n", inCloud[n]))
	}
	return b.String()
}

// selfManagedOnly returns block preceded by a blank line, wrapped in
// ifndef::env-cloud[] unless it is available in Redpanda Cloud.
func selfManagedOnly(block string, inCloud bool) string {
	if inCloud {
		return "\n" + block
	}
	return "\nifndef::env-cloud[]\n" + block + "endif::[]\n"
}

var defaultCollator = collate.New(language.Und)

// renderMethodsList renders the includes that make up the Bloblang methods
// reference, grouped by category. General comes first and Deprecated last.
// Methods that the Redpanda Cloud build doesn't allow are wrapped as in
// renderFunctionsList.
func renderMethodsList(specs []bloblangSpec, inCloud map[string]bool) string {
	type entry struct{ name, partial string }
	byCategory := map[string][]entry{}
	var categories []string
	for _, s := range visibleBloblang(specs) {
		variants := categoryVariants(s)
		for _, c := range s.Categories {
			if c.Category == "" {
				continue
			}
			if _, ok := byCategory[c.Category]; !ok {
				categories = append(categories, c.Category)
			}
			partial := s.Name
			if _, ok := variants[c.Category]; ok {
				partial = categoryVariantName(s.Name, c.Category)
			}
			byCategory[c.Category] = append(byCategory[c.Category], entry{s.Name, partial})
		}
	}
	rank := func(c string) int {
		switch c {
		case "General":
			return -1
		case "Deprecated":
			return 1
		}
		return 0
	}
	sort.SliceStable(categories, func(i, j int) bool {
		a, b := categories[i], categories[j]
		if rank(a) != rank(b) {
			return rank(a) < rank(b)
		}
		return defaultCollator.CompareString(a, b) < 0
	})
	var b strings.Builder
	b.WriteString(generatedBanner + "\n")
	for _, c := range categories {
		methods := byCategory[c]
		sort.Slice(methods, func(i, j int) bool { return methods[i].name < methods[j].name })
		var section strings.Builder
		section.WriteString("\n== " + toSentenceCase(c) + "\n")
		anyInCloud := false
		for _, m := range methods {
			anyInCloud = anyInCloud || inCloud[m.name]
			section.WriteString(selfManagedOnly("include::connect:components:partial$bloblang-methods/"+m.partial+".adoc[leveloffset=+2]\n", inCloud[m.name]))
		}
		if anyInCloud {
			b.WriteString(section.String())
		} else {
			// No method in this category is in Cloud, so hide the heading too.
			b.WriteString("\nifndef::env-cloud[]" + section.String() + "endif::[]\n")
		}
	}
	return b.String()
}

var sentenceCasePreserved = map[string]bool{
	"SQL": true, "JSON": true, "JWT": true, "XML": true, "HTML": true, "URL": true, "URI": true,
	"HTTP": true, "HTTPS": true, "TLS": true, "SSL": true, "AWS": true, "GCP": true, "API": true,
	"ID": true, "UUID": true, "CSV": true,
}

// toSentenceCase turns a category name such as "Object & Array Manipulation"
// into a sentence-case heading, keeping acronyms upper case.
func toSentenceCase(text string) string {
	words := regexp.MustCompile(`\s+`).Split(text, -1)
	for i, w := range words {
		switch {
		case strings.EqualFold(w, "geoip"):
			words[i] = "GeoIP"
		case sentenceCasePreserved[strings.ToUpper(w)]:
			words[i] = strings.ToUpper(w)
		case w == "&":
		case i == 0 && w != "":
			words[i] = strings.ToUpper(w[:1]) + strings.ToLower(w[1:])
		default:
			words[i] = strings.ToLower(w)
		}
	}
	return strings.Join(words, " ")
}
