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
	"path/filepath"
	"regexp"
	"strconv"
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
	// interpolationPhrase finds a description that mentions interpolation
	// functions without linking them.
	interpolationPhrase = regexp.MustCompile(`(?i)interpolation functions`)
)

// fieldName is the field's name with [] appended for array fields, and [][]
// for arrays of arrays.
func fieldName(f fieldSpec) string {
	switch {
	case strings.HasSuffix(f.Name, "[]"):
		return f.Name
	case f.Kind == "array":
		return f.Name + "[]"
	case f.Kind == "2darray":
		return f.Name + "[][]"
	}
	return f.Name
}

func fieldDisplayType(f fieldSpec) string {
	switch {
	case strings.HasSuffix(f.Name, "[]"):
		return "array<object>"
	case f.Kind == "map":
		// A map whose values have a known type shows it, as arrays do.
		if f.Type == "" || f.Type == "unknown" || f.Type == "object" {
			return "object"
		}
		return "object<" + f.Type + ">"
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
	return renderFieldsSince(fields, prefix, "")
}

// renderFieldsSince renders fields whose nearest versioned ancestor (the
// component, or a parent field) was introduced in version since. A field's
// "Requires version" line is only shown when its version is later than that,
// since the ancestor's requirement already covers it. Shared field
// constructors carry the version history of the field in general, which can
// be older than the component that uses them.
func renderFieldsSince(fields []fieldSpec, prefix, since string) string {
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
			desc = withInterpolationNotice(desc)
		} else {
			desc = blankLineRuns.ReplaceAllString(strings.ReplaceAll(desc, interpolationNotice, ""), "\n\n")
		}
		if desc != "" {
			b.WriteString(desc + "\n\n")
		}
		if f.IsSecret {
			b.WriteString("include::connect:components:partial$secret_warning.adoc[]\n\n")
		}
		childSince := since
		if f.Version != "" && versionLater(f.Version, since) {
			b.WriteString("ifndef::env-cloud[]\nRequires version " + f.Version + " or later.\nendif::[]\n\n")
			childSince = f.Version
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
				// An empty option would render as two backticks, which
				// AsciiDoc prints literally.
				if o == "" {
					o = `""`
				}
				opts[i] = "`" + o + "`"
			}
			b.WriteString("*Options*: " + strings.Join(opts, ", ") + "\n\n")
		}
		if len(f.Examples) > 0 {
			b.WriteString(renderFieldExamples(f))
		}
		if len(f.Children) > 0 {
			b.WriteString(renderFieldsSince(f.Children, path, childSince))
		}
		out.WriteString(b.String())
	}
	return out.String()
}

// withInterpolationNotice links interpolation functions from the description
// of an interpolated field. A description that already names them gets the
// link on its first mention. Any other description gets the standard notice.
func withInterpolationNotice(desc string) string {
	if strings.Contains(desc, "xref:configuration:interpolation.adoc") {
		return desc
	}
	if loc := interpolationPhrase.FindStringIndex(desc); loc != nil {
		return desc[:loc[0]] + "xref:configuration:interpolation.adoc#bloblang-queries[" + desc[loc[0]:loc[1]] + "]" + desc[loc[1]:]
	}
	trimmed := strings.TrimSpace(desc)
	if trimmed != "" {
		trimmed += "\n\n"
	}
	return trimmed + interpolationNotice
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
		// An empty, whitespace-only, or control-character default would
		// render as an empty or broken code span, so it is shown quoted and
		// escaped, for example `"\n"`.
		if strings.TrimSpace(t) == "" || strings.IndexFunc(t, unicode.IsControl) >= 0 {
			display = jsonStringify(t)
		}
		return "*Default*: `" + display + "`\n\n"
	case nil:
		return "*Default*: `null`\n\n"
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
		if arr, ok := ex.([]any); ok && f.Kind == "array" {
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
			switch t := ex.(type) {
			case map[string]any, []any, nil:
				b.WriteString(f.Name + ":\n")
				b.WriteString(indentLines(strings.TrimSpace(yamlStringify(t, yamlPlain)), "  ") + "\n")
			case string:
				// yamlScalar picks the block chomping and indentation
				// indicators that keep trailing newlines and leading spaces.
				b.WriteString(f.Name + ": " + yamlScalar(t, "  ") + "\n")
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
		// A value that starts with a quote keeps it, as in the Postgres
		// example `"MyCaseSensitiveTable"`, so it is wrapped in the other
		// kind of quotes.
		if strings.HasPrefix(s, `"`) || strings.HasPrefix(s, "'") {
			return yamlQuotedString(s, "    ", false)
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

// doubleQuote renders s as a YAML double-quoted scalar. A JSON string is one,
// and jsonStringify also escapes control characters.
func doubleQuote(s string) string {
	return jsonStringify(s)
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
	if strings.HasPrefix(s, `"`) || strings.HasPrefix(s, "'") {
		return yamlQuotedString(s, "    ", false)
	}
	if s == "*" || yamlSpecialNQ.MatchString(s) {
		return doubleQuote(s)
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
//
// Benthos renders these snippets too (ConfigView.TemplateData). They are built
// here, together with yaml.go, to reproduce the formatting of the docs
// pipeline this generator replaces, so pages don't reflow during the
// migration. Once rp-connect-docs reads the generated docs, switch to the
// benthos snippets: https://github.com/redpanda-data/connect/issues/4924

// snippets writes the Common and Advanced config snippets for a component.
// The group key only decides the directory. Each snippet nests the config
// under the singular component type, as benthos genExampleConfigs does.
func (w *writer) snippets(key string, c componentSpec) {
	base := filepath.Join(key, c.Name+".yaml")
	common, advanced := componentSnippets(c)
	w.write(filepath.Join("examples/common", base), common)
	w.write(filepath.Join("examples/advanced", base), advanced)
}

// componentSnippets renders the Common and Advanced snippets for a component.
// Components whose config is a single value, a list, or an object with no
// fields (such as resource, fallback, and drop) have no Common or Advanced
// split, so both snippets are the same.
func componentSnippets(c componentSpec) (common, advanced string) {
	if c.Config.Children == nil {
		snippet := buildValueConfigYAML(c.Type, c.Name, c.Config)
		return snippet, snippet
	}
	return buildConfigYAML(c.Type, c.Name, c.Config, false), buildConfigYAML(c.Type, c.Name, c.Config, true)
}

// buildConfigYAML renders a snippet for a component whose config has fields,
// nested under root, the component type.
func buildConfigYAML(root, name string, conf fieldSpec, includeAdvanced bool) string {
	lines := []string{root + ":"}
	if typesWithLabel[root] {
		lines = append(lines, `  label: ""`)
	}
	lines = append(lines, configNamed(name, conf, 2, includeAdvanced)...)
	return strings.Join(lines, "\n") + "\n"
}

// buildValueConfigYAML renders the config snippet for a component whose config
// is not an object with fields: a scalar such as `resource: ""`, a list such
// as `fallback: []`, or an empty object such as `drop: {}`.
func buildValueConfigYAML(root, name string, conf fieldSpec) string {
	lines := []string{root + ":"}
	// A resource reference takes its label from the resource it points to,
	// and the linter rejects a label on it.
	if typesWithLabel[root] && name != "resource" {
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
	return strings.Join(lines, "\n") + "\n"
}

// configNamed renders `name:` and the fields of an object, map, or list of
// objects, at the given indent.
//
//   - An object lists its fields under the key.
//   - A map has one placeholder entry, `<name>:`, that lists the fields of
//     each value.
//   - A list of objects has one item that lists the fields.
//
// When every field is filtered out, the value is `{}` or `[]` so the key is
// never left as a bare `name:`, which YAML reads as null.
func configNamed(name string, f fieldSpec, indent int, includeAdvanced bool) []string {
	pad := strings.Repeat(" ", indent)
	switch f.Kind {
	case "array":
		children := configFields(f.Children, indent+4, includeAdvanced)
		if len(children) == 0 {
			return []string{pad + name + ": []"}
		}
		children[0] = pad + "  - " + children[0][indent+4:]
		return append([]string{pad + name + ":"}, children...)
	case "map":
		children := configFields(f.Children, indent+4, includeAdvanced)
		if len(children) == 0 {
			return []string{pad + name + ": {}"}
		}
		return append([]string{pad + name + ":", pad + "  <name>:"}, children...)
	}
	children := configFields(f.Children, indent+2, includeAdvanced)
	if len(children) == 0 {
		return []string{pad + name + ": {}"}
	}
	return append([]string{pad + name + ":"}, children...)
}

// configFields renders each field that the snippet shows, at the given
// indent. A field that is a list of objects shows its default, usually `[]`,
// rather than an example item.
func configFields(fields []fieldSpec, indent int, includeAdvanced bool) []string {
	var lines []string
	for _, f := range fields {
		if f.IsDeprecated || (!includeAdvanced && f.IsAdvanced) {
			continue
		}
		switch {
		case f.Kind == "array" || f.Kind == "2darray" || len(f.Children) == 0:
			lines = append(lines, configLeaf(f, indent))
		default:
			lines = append(lines, configNamed(f.Name, f, indent, includeAdvanced)...)
		}
	}
	return lines
}

// configPlaceholder is the value a snippet shows for a field with no default,
// chosen by type so the snippet still has the right shape.
func configPlaceholder(f fieldSpec) string {
	switch {
	case f.Kind == "array" || f.Kind == "2darray":
		return "[]"
	case f.Kind == "map" || f.Type == "object" || componentTypes[f.Type]:
		return "{}"
	case f.Type == "bool":
		return "false"
	case f.Type == "int" || f.Type == "float":
		return "0"
	}
	return `""`
}

func configLeaf(f fieldSpec, indent int) string {
	pad := strings.Repeat(" ", indent)
	if !f.hasDefault() {
		comment := "# No default (required)"
		if f.IsOptional {
			comment = "# No default (optional)"
		}
		return pad + f.Name + ": " + configPlaceholder(f) + " " + comment
	}
	v := f.defaultValue()
	if !isNonEmptyCollection(v) {
		return pad + f.Name + ": " + yamlEmitter{style: yamlPlain}.scalar(v, strings.Repeat(" ", indent+2), false)
	}
	body := strings.TrimSpace(yamlStringify(v, yamlDoubleQuoted))
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

// configObjects writes the field reference and the Common and Advanced
// snippets for each top-level config object that has fields, such as http,
// logger, and redpanda. The list comes from the schema, so a new object gets
// its partials without a change here.
func (w *writer) configObjects(raw []byte) {
	objects, err := topLevelConfigObjects(raw)
	if err != nil {
		panic(err)
	}
	for _, f := range objects {
		w.write(filepath.Join("partials/fields/config", f.Name+".adoc"),
			generatedBanner+"\n\n== Fields\n\n"+renderFields(f.Children, configObjectPrefix(f))+"\n")
		w.write(filepath.Join("examples/common/config", f.Name+".yaml"), buildTopLevelConfigYAML(f, false))
		w.write(filepath.Join("examples/advanced/config", f.Name+".yaml"), buildTopLevelConfigYAML(f, true))
	}
}

// templateFields writes the fields of a template file, for the templating
// page. A template file is a config of its own, so its fields are the
// top-level fields of the template schema.
func (w *writer) templateFields(raw []byte) {
	var doc struct {
		Config []fieldSpec `json:"config"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		panic(err)
	}
	if len(doc.Config) == 0 {
		panic("the template schema has no fields")
	}
	w.write("partials/fields/config/templates.adoc", generatedBanner+"\n\n== Fields\n\n"+renderFields(doc.Config, "")+"\n")
}

// configObjectPrefix is the path prefix for the fields of a top-level config
// object. An object such as redpanda documents its fields by their own names,
// but a list such as tests is a list of objects, so its fields are documented
// as tests[].<field>.
func configObjectPrefix(f fieldSpec) string {
	if f.Kind == "array" || f.Kind == "2darray" {
		return fieldName(f)
	}
	return ""
}

// topLevelConfigObjects returns the top-level config fields of a schema that
// have child fields. Component slots such as input and metrics, and scalars
// such as shutdown_timeout, have none.
func topLevelConfigObjects(raw []byte) ([]fieldSpec, error) {
	var doc struct {
		Config []fieldSpec `json:"config"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	var objects []fieldSpec
	for _, f := range doc.Config {
		if !f.IsDeprecated && len(f.Children) > 0 {
			objects = append(objects, f)
		}
	}
	return objects, nil
}

func buildTopLevelConfigYAML(f fieldSpec, includeAdvanced bool) string {
	return strings.Join(configNamed(f.Name, f, 0, includeAdvanced), "\n") + "\n"
}

// versionLater reports whether version a is later than b. Either one that
// isn't an x.y.z number counts as unknown, so a is shown.
func versionLater(a, b string) bool {
	pa, okA := parseXYZ(a)
	pb, okB := parseXYZ(b)
	if !okA || !okB {
		return true
	}
	for i := range pa {
		if pa[i] != pb[i] {
			return pa[i] > pb[i]
		}
	}
	return false
}

func parseXYZ(v string) ([3]int, bool) {
	var out [3]int
	parts := strings.Split(strings.TrimPrefix(v, "v"), ".")
	if len(parts) != 3 {
		return out, false
	}
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			return out, false
		}
		out[i] = n
	}
	return out, true
}
