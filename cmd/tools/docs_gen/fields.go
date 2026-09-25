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
	"strings"
	"unicode"
)

const interpolationNotice = "This field supports xref:configuration:interpolation.adoc#bloblang-queries[interpolation functions]."

var (
	betaPrefix     = regexp.MustCompile(`(?i)^\s*BETA:\s*`)
	blankLineRuns  = regexp.MustCompile(`\n{2,}`)
	numericLooking = regexp.MustCompile(`^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$`)
	yamlBoolLike   = regexp.MustCompile(`(?i)^(true|false|null|yes|no|on|off)$`)
	yamlSpecial    = regexp.MustCompile("[:\\[\\]{},&>|%@`\"]")
	yamlSpecialNQ  = regexp.MustCompile("[:\\[\\]{},&>|%@`]")
)

// fieldName is the field's name with [] appended for array fields.
func fieldName(f fieldSpec) string {
	if f.Kind == "array" && !strings.HasSuffix(f.Name, "[]") {
		return f.Name + "[]"
	}
	return f.Name
}

func fieldDisplayType(f fieldSpec) string {
	switch {
	case strings.HasSuffix(f.Name, "[]"):
		return "array<object>"
	case f.Kind == "map":
		return "object"
	case f.Kind == "array" || f.Kind == "list" || f.Kind == "2darray":
		if f.Type == "" || f.Type == "unknown" || f.Type == "array" {
			return "array"
		}
		if f.Kind == "2darray" {
			return "array<array<" + f.Type + ">>"
		}
		return "array<" + f.Type + ">"
	}
	return f.Type
}

// renderFields renders the `=== field` sections for a list of fields and,
// recursively, their children.
func renderFields(fields []fieldSpec, prefix string) string {
	var out strings.Builder
	for _, f := range sortFieldsByName(fields) {
		if f.IsDeprecated || f.Name == "" {
			continue
		}
		path := fieldName(f)
		if prefix != "" {
			path = prefix + "." + path
		}

		var b strings.Builder
		b.WriteString("=== `" + path + "`\n\n")

		desc := protectCodeSpans(escapePlaceholderBraces(f.Description))
		if betaPrefix.MatchString(f.Description) {
			desc = "badge::[label=Beta, size=large, tooltip={page-beta-text}]\n\n" + betaPrefix.ReplaceAllString(desc, "")
		}
		if f.Interpolated {
			if !strings.Contains(strings.ToLower(desc), "interpolation functions") {
				trimmed := strings.TrimSpace(desc)
				if trimmed != "" {
					trimmed += "\n\n"
				}
				desc = trimmed + interpolationNotice
			}
		} else {
			desc = blankLineRuns.ReplaceAllString(strings.ReplaceAll(desc, interpolationNotice, ""), "\n\n")
		}
		if desc != "" {
			b.WriteString(desc + "\n\n")
		}
		if f.IsSecret {
			b.WriteString("include::connect:components:partial$secret_warning.adoc[]\n\n")
		}
		if f.Version != "" {
			b.WriteString("ifndef::env-cloud[]\nRequires version " + f.Version + " or later.\nendif::[]\n\n")
		}
		b.WriteString("*Type*: `" + fieldDisplayType(f) + "`\n\n")

		if f.hasDefault() {
			b.WriteString(renderFieldDefault(f.defaultValue()))
		}

		if len(f.AnnotatedOptions) > 0 {
			b.WriteString("[cols=\"1m,2a\"]\n|===\n|Option |Summary\n\n")
			for _, o := range f.AnnotatedOptions {
				b.WriteString("|" + o[0] + "\n|" + o[1] + "\n\n")
			}
			b.WriteString("|===\n\n")
		}
		if len(f.Options) > 0 {
			opts := make([]string, len(f.Options))
			for i, o := range f.Options {
				opts[i] = "`" + o + "`"
			}
			b.WriteString("*Options*: " + strings.Join(opts, ", ") + "\n\n")
		}
		if len(f.Examples) > 0 {
			b.WriteString(renderFieldExamples(f))
		}
		if len(f.Children) > 0 {
			b.WriteString(renderFields(f.Children, path))
		}
		out.WriteString(b.String())
	}
	return out.String()
}

func renderFieldDefault(v any) string {
	switch t := v.(type) {
	case []any:
		if len(t) == 0 {
			return "*Default*: `[]`\n\n"
		}
	case map[string]any:
		if len(t) == 0 {
			return "*Default*: `{}`\n\n"
		}
	case string:
		display := t
		if t == "" {
			display = `""`
		}
		return "*Default*: `" + display + "`\n\n"
	case nil:
	default:
		return "*Default*: `" + jsString(t) + "`\n\n"
	}
	return "*Default*:\n[source,yaml]\n----\n" + strings.TrimSpace(yamlStringify(v, yamlDoubleQuoted)) + "\n----\n\n"
}

func renderFieldExamples(f fieldSpec) string {
	var b strings.Builder
	b.WriteString("[source,yaml]\n----\n# Examples:\n")
	for i, raw := range f.Examples {
		ex := decodeValue(raw)
		if f.Kind == "array" {
			if arr, ok := ex.([]any); ok {
				hasObjects := false
				for _, item := range arr {
					switch item.(type) {
					case map[string]any, []any:
						hasObjects = true
					}
				}
				if hasObjects {
					b.WriteString(renderYAMLList(f.Name, arr))
				} else {
					b.WriteString(f.Name + ":\n")
					for _, item := range arr {
						b.WriteString("  - " + quoteListScalar(item) + "\n")
					}
				}
			} else {
				b.WriteString(f.Name + ": " + jsString(ex) + "\n")
			}
		} else {
			switch t := ex.(type) {
			case map[string]any, []any, nil:
				b.WriteString(f.Name + ":\n")
				b.WriteString(indentLines(strings.TrimSpace(yamlStringify(t, yamlPlain)), "  ") + "\n")
			case string:
				if strings.Contains(t, "\n") {
					b.WriteString(f.Name + ": |-\n")
					b.WriteString(indentLines(t, "  ") + "\n")
				} else {
					b.WriteString(f.Name + ": " + yamlScalar(t, "  ") + "\n")
				}
			default:
				b.WriteString(f.Name + ": " + jsString(t) + "\n")
			}
		}
		if i < len(f.Examples)-1 {
			b.WriteString("\n# ---\n\n")
		}
	}
	b.WriteString("----\n\n")
	return b.String()
}

// quoteListScalar renders one item of a list of scalars.
func quoteListScalar(item any) string {
	if s, ok := item.(string); ok {
		if strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`) {
			return s
		}
		if s == "" || s == "*" || yamlBoolLike.MatchString(s) || numericLooking.MatchString(s) ||
			yamlSpecial.MatchString(s) || strings.IndexFunc(s, jsIsSpace) >= 0 {
			return doubleQuote(s)
		}
		return s
	}
	s := jsString(item)
	if yamlSpecial.MatchString(s) || strings.IndexFunc(s, jsIsSpace) >= 0 {
		return doubleQuote(s)
	}
	return s
}

func doubleQuote(s string) string {
	return `"` + strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), `"`, `\"`) + `"`
}

// renderYAMLList renders a list whose items include objects.
func renderYAMLList(name string, items []any) string {
	var b strings.Builder
	b.WriteString(name + ":\n")
	for _, item := range items {
		switch t := item.(type) {
		case string, bool, json.Number:
			b.WriteString("  - " + quoteSpecialScalar(jsString(t)) + "\n")
		default:
			lines := strings.Split(strings.TrimSpace(yamlStringify(item, yamlPlain)), "\n")
			for i, l := range lines {
				if i == 0 {
					b.WriteString("  - " + l)
				} else {
					b.WriteString("\n    " + l)
				}
			}
			b.WriteString("\n")
		}
	}
	b.WriteString("\n")
	return b.String()
}

func quoteSpecialScalar(s string) string {
	if strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`) {
		return s
	}
	if s == "*" || yamlSpecialNQ.MatchString(s) {
		return `"` + s + `"`
	}
	return s
}

func indentLines(s, indent string) string {
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		lines[i] = indent + l
	}
	return strings.Join(lines, "\n")
}

// jsIsSpace matches the characters JavaScript's \s matches.
func jsIsSpace(r rune) bool {
	if r == 0xfeff {
		return true
	}
	if r == '\u0085' {
		return false
	}
	return unicode.IsSpace(r)
}

// The Common and Advanced config snippets.

var typesWithLabel = map[string]bool{"inputs": true, "outputs": true, "processors": true}

func buildConfigYAML(key, name string, fields []fieldSpec, includeAdvanced bool) string {
	lines := []string{key + ":"}
	if typesWithLabel[key] {
		lines = append(lines, `  label: ""`)
	}
	var render []fieldSpec
	for _, f := range fields {
		if f.IsDeprecated || (!includeAdvanced && f.IsAdvanced) {
			continue
		}
		render = append(render, f)
	}
	if len(render) == 0 {
		lines = append(lines, "  "+name+": {}")
	} else {
		lines = append(lines, "  "+name+":")
	}
	for _, f := range render {
		switch {
		case f.Kind == "array" && f.Type == "object" && f.Children != nil:
			lines = append(lines, configLeaf(f, 4))
		case f.Type == "object" && f.Children != nil:
			lines = append(lines, configObject(f, 4, includeAdvanced)...)
		default:
			lines = append(lines, configLeaf(f, 4))
		}
	}
	return strings.Join(lines, "\n")
}

// componentTypes are config types whose value is itself a component config,
// such as reject_errored, which wraps an output.
var componentTypes = map[string]bool{
	"input": true, "output": true, "processor": true, "cache": true, "rate_limit": true,
	"buffer": true, "metrics": true, "tracer": true, "scanner": true,
}

// buildValueConfigYAML renders the config snippet for a component whose config
// is not an object with fields: a scalar such as `resource: ""`, a list such
// as `fallback: []`, or an empty object such as `drop: {}`.
func buildValueConfigYAML(key, name string, conf fieldSpec) string {
	lines := []string{key + ":"}
	if typesWithLabel[key] {
		lines = append(lines, `  label: ""`)
	}
	switch {
	case conf.Kind == "array" || conf.Kind == "2darray":
		lines = append(lines, "  "+name+": []")
	case conf.Type == "object" || conf.Kind == "map" || componentTypes[conf.Type]:
		lines = append(lines, "  "+name+": {}")
	default:
		conf.Name = name
		lines = append(lines, configLeaf(conf, 2))
	}
	return strings.Join(lines, "\n")
}

func configObject(f fieldSpec, indent int, includeAdvanced bool) []string {
	lines := []string{strings.Repeat(" ", indent) + f.Name + ":"}
	for _, c := range f.Children {
		if c.IsDeprecated || (!includeAdvanced && c.IsAdvanced) {
			continue
		}
		switch {
		case c.Kind == "array" && c.Type == "object" && c.Children != nil:
			lines = append(lines, configLeaf(c, indent+2))
		case len(c.Children) > 0:
			lines = append(lines, configObject(c, indent+2, includeAdvanced)...)
		default:
			lines = append(lines, configLeaf(c, indent+2))
		}
	}
	return lines
}

func configLeaf(f fieldSpec, indent int) string {
	pad := strings.Repeat(" ", indent)
	comment := "# No default (required)"
	if f.IsOptional {
		comment = "# No default (optional)"
	}
	if !f.hasDefault() {
		if f.Kind == "array" {
			return pad + f.Name + ": [] " + comment
		}
		return pad + f.Name + `: "" ` + comment
	}
	switch t := f.defaultValue().(type) {
	case []any:
		if len(t) == 0 {
			return pad + f.Name + ": []"
		}
	case map[string]any:
		if len(t) == 0 {
			return pad + f.Name + ": {}"
		}
	case string:
		if t == "" {
			return pad + f.Name + `: ""`
		}
		return pad + f.Name + ": " + yamlScalar(t, strings.Repeat(" ", indent+2))
	case nil:
	default:
		return pad + f.Name + ": " + jsString(t)
	}
	body := strings.TrimSpace(yamlStringify(f.defaultValue(), yamlDoubleQuoted))
	return pad + f.Name + ":\n" + indentLines(body, strings.Repeat(" ", indent+2))
}

func renderComponentExamples(examples []componentExample) string {
	var b strings.Builder
	for _, ex := range examples {
		if ex.Title != "" {
			b.WriteString("=== " + strings.ReplaceAll(ex.Title, "=", `\=`) + "\n\n")
		}
		if ex.Summary != "" {
			b.WriteString(ex.Summary + "\n\n")
		}
		if ex.Config != "" {
			b.WriteString("[source,yaml]\n----\n" + strings.TrimSpace(ex.Config) + "\n----\n\n")
		}
	}
	return b.String()
}
