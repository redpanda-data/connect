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

// renderBloblangSpec renders the reference partial for one function or
// method. kind is "function" or "method".
func renderBloblangSpec(spec bloblangSpec, kind string) string {
	var b strings.Builder
	b.WriteString(generatedBanner + "\n\n= " + htmlEscaper.Replace(spec.Name) + "\n")
	if spec.Status == "deprecated" {
		b.WriteString("\n[WARNING]\n====\nThis " + kind + " is deprecated and will be removed in a future version.\n====\n")
	}
	if spec.Description != "" {
		b.WriteString("\n" + ensurePeriod(spec.Description) + "\n")
	}
	b.WriteString("\n")
	if spec.Params != nil && len(spec.Params.Named) > 0 {
		b.WriteString("\n== Parameters\n\n[cols=\"1,1,3\"]\n|===\n| Name | Type | Description\n\n")
		for _, p := range spec.Params.Named {
			b.WriteString("| `" + htmlEscaper.Replace(p.Name) + "`")
			if p.IsOptional {
				b.WriteString(" (optional)")
			}
			b.WriteString("\n| `" + htmlEscaper.Replace(p.Type) + "`\n| " + p.Description + "\n\n")
		}
		b.WriteString("|===\n\n")
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

// renderFunctionsList renders the includes that make up the Bloblang
// functions reference, in name order.
func renderFunctionsList(specs []bloblangSpec) string {
	var names []string
	for _, s := range specs {
		if s.Name != "" {
			names = append(names, s.Name)
		}
	}
	sort.Strings(names)
	var b strings.Builder
	b.WriteString(generatedBanner + "\n")
	for _, n := range names {
		b.WriteString("\ninclude::connect:components:partial$bloblang-functions/" + n + ".adoc[leveloffset=+1]\n")
	}
	return b.String()
}

var defaultCollator = collate.New(language.Und)

// renderMethodsList renders the includes that make up the Bloblang methods
// reference, grouped by category. General comes first and Deprecated last.
func renderMethodsList(specs []bloblangSpec) string {
	byCategory := map[string][]string{}
	var categories []string
	for _, s := range specs {
		if s.Name == "" {
			continue
		}
		for _, c := range s.Categories {
			if c.Category == "" {
				continue
			}
			if _, ok := byCategory[c.Category]; !ok {
				categories = append(categories, c.Category)
			}
			byCategory[c.Category] = append(byCategory[c.Category], s.Name)
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
		sort.Strings(methods)
		b.WriteString("\n== " + toSentenceCase(c) + "\n")
		for _, m := range methods {
			b.WriteString("\ninclude::connect:components:partial$bloblang-methods/" + m + ".adoc[leveloffset=+2]\n")
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
