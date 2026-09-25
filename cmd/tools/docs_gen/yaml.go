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
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf16"
)

// A small YAML emitter for defaults and examples in the reference docs. Its
// output matches the `yaml` npm package (v2, block style, no line folding),
// which the published docs were generated with, so regenerating from Go does
// not reformat every snippet. gopkg.in/yaml.v3 quotes and indents
// differently.

type yamlStringStyle int

const (
	// yamlPlain writes strings unquoted wherever YAML allows it.
	yamlPlain yamlStringStyle = iota
	// yamlDoubleQuoted writes every string value in double quotes. Keys stay
	// plain.
	yamlDoubleQuoted
)

// yamlScalar renders a string as the value of a mapping entry, quoting it
// when YAML would otherwise read it as another type or as syntax (for
// example `*`, `true`, or `a: b`). indent is used for any continuation lines.
func yamlScalar(s, indent string) string {
	return yamlEmitter{style: yamlPlain}.str(s, indent, false)
}

// yamlStringify renders v as a YAML document, including the trailing newline.
func yamlStringify(v any, style yamlStringStyle) string {
	e := yamlEmitter{style: style}
	switch t := v.(type) {
	case map[string]any:
		if len(t) > 0 {
			return e.mapping(t, "")
		}
	case []any:
		if len(t) > 0 {
			return e.sequence(t, "")
		}
	}
	return e.scalar(v, "", false) + "\n"
}

type yamlEmitter struct {
	style yamlStringStyle
}

func isNonEmptyCollection(v any) bool {
	switch t := v.(type) {
	case map[string]any:
		return len(t) > 0
	case []any:
		return len(t) > 0
	}
	return false
}

func (e yamlEmitter) mapping(m map[string]any, indent string) string {
	var sb strings.Builder
	for _, k := range objectKeys(m) {
		v := m[k]
		sb.WriteString(indent)
		// Keys get the child indent, as in npm yaml's stringifyPair.
		sb.WriteString(e.str(k, indent+"  ", true))
		sb.WriteString(":")
		switch {
		case isNonEmptyCollection(v):
			sb.WriteString("\n")
			sb.WriteString(e.block(v, indent+"  "))
		default:
			sb.WriteString(" ")
			sb.WriteString(e.scalar(v, indent+"  ", false))
			sb.WriteString("\n")
		}
	}
	return sb.String()
}

func (e yamlEmitter) sequence(s []any, indent string) string {
	var sb strings.Builder
	for _, v := range s {
		if isNonEmptyCollection(v) {
			// The first line of a nested collection shares the dash line.
			nested := e.block(v, indent+"  ")
			sb.WriteString(indent)
			sb.WriteString("- ")
			sb.WriteString(strings.TrimPrefix(nested, indent+"  "))
			continue
		}
		sb.WriteString(indent)
		sb.WriteString("- ")
		sb.WriteString(e.scalar(v, indent+"  ", false))
		sb.WriteString("\n")
	}
	return sb.String()
}

func (e yamlEmitter) block(v any, indent string) string {
	if m, ok := v.(map[string]any); ok {
		return e.mapping(m, indent)
	}
	return e.sequence(v.([]any), indent)
}

// scalar renders a scalar or empty collection. indent is the indentation of
// any continuation lines, such as the body of a block string.
func (e yamlEmitter) scalar(v any, indent string, implicitKey bool) string {
	switch t := v.(type) {
	case nil:
		return "null"
	case bool:
		return strconv.FormatBool(t)
	case json.Number:
		return jsNumber(t)
	case string:
		return e.str(t, indent, implicitKey)
	case map[string]any:
		return "{}"
	case []any:
		return "[]"
	}
	return fmt.Sprint(v)
}

var (
	yamlControlChars   = regexp.MustCompile(`[\x00-\x08\x0b-\x1f\x7f-\x{9f}]`)
	yamlPlainForbidden = regexp.MustCompile("^[\n\t ,\\[\\]{}#&*!|>'\"%@`]|^[?-]$|^[?-][ \t]|[\n:][ \t]|[ \t]\n|[\n\t ]#|[\n\t :]$")
	yamlDocumentMarker = regexp.MustCompile(`(?m)^(%|---|\.\.\.)`)
	yamlBlockTrailing  = regexp.MustCompile(`\n[\t ]+$`)
	// Plain scalars that YAML 1.2 core schema resolves to something other
	// than a string, so a string with this content must be quoted.
	yamlNonStringScalars = []*regexp.Regexp{
		regexp.MustCompile(`^(?:~|[Nn]ull|NULL)?$`),
		regexp.MustCompile(`^(?:[Tt]rue|TRUE|[Ff]alse|FALSE)$`),
		regexp.MustCompile(`^0o[0-7]+$`),
		regexp.MustCompile(`^[-+]?[0-9]+$`),
		regexp.MustCompile(`^0x[0-9a-fA-F]+$`),
		regexp.MustCompile(`^(?:[-+]?\.(?:inf|Inf|INF)|\.nan|\.NaN|\.NAN)$`),
		regexp.MustCompile(`^[-+]?(?:\.[0-9]+|[0-9]+(?:\.[0-9]*)?)[eE][-+]?[0-9]+$`),
		regexp.MustCompile(`^[-+]?(?:\.[0-9]+|[0-9]+\.[0-9]*)$`),
	}
)

func (e yamlEmitter) str(s, indent string, implicitKey bool) string {
	if !implicitKey && e.style == yamlDoubleQuoted || yamlControlChars.MatchString(s) {
		return yamlDoubleQuotedString(s, indent, implicitKey)
	}
	return yamlPlainString(s, indent, implicitKey)
}

func yamlPlainString(s, indent string, implicitKey bool) string {
	if implicitKey && strings.Contains(s, "\n") {
		return yamlQuotedString(s, indent, implicitKey)
	}
	if yamlPlainForbidden.MatchString(s) {
		if implicitKey || !strings.Contains(s, "\n") {
			return yamlQuotedString(s, indent, implicitKey)
		}
		return yamlBlockString(s, indent, implicitKey)
	}
	if !implicitKey && strings.Contains(s, "\n") {
		return yamlBlockString(s, indent, implicitKey)
	}
	if yamlDocumentMarker.MatchString(s) {
		if indent == "" {
			return yamlBlockString(s, "  ", implicitKey)
		} else if implicitKey && indent == "  " {
			return yamlQuotedString(s, indent, implicitKey)
		}
	}
	for _, re := range yamlNonStringScalars {
		if re.MatchString(s) {
			return yamlQuotedString(s, indent, implicitKey)
		}
	}
	return s
}

func yamlQuotedString(s, indent string, implicitKey bool) string {
	hasDouble := strings.Contains(s, `"`)
	hasSingle := strings.Contains(s, "'")
	if hasDouble && !hasSingle {
		return yamlSingleQuotedString(s, indent, implicitKey)
	}
	return yamlDoubleQuotedString(s, indent, implicitKey)
}

var yamlSpaceAroundNewline = regexp.MustCompile("[ \t]\n|\n[ \t]")

func yamlSingleQuotedString(s, indent string, implicitKey bool) string {
	if implicitKey && strings.Contains(s, "\n") || yamlSpaceAroundNewline.MatchString(s) {
		return yamlDoubleQuotedString(s, indent, implicitKey)
	}
	if indent == "" && yamlDocumentMarker.MatchString(s) {
		indent = "  "
	}
	body := strings.ReplaceAll(s, "'", "''")
	body = replaceNewlineRuns(body, func(run string) string { return run + "\n" + indent })
	return "'" + body + "'"
}

// yamlDoubleQuotedMinMultiLine is the length below which a double-quoted
// string keeps \n escapes instead of spreading over several lines.
const yamlDoubleQuotedMinMultiLine = 40

func yamlDoubleQuotedString(s, indent string, implicitKey bool) string {
	js := jsonStringify(s)
	if indent == "" && yamlDocumentMarker.MatchString(s) {
		indent = "  "
	}
	var sb strings.Builder
	start := 0
	for i := 0; i < len(js); i++ {
		ch := js[i]
		if ch == ' ' && i+2 < len(js) && js[i+1] == '\\' && js[i+2] == 'n' {
			sb.WriteString(js[start:i])
			sb.WriteString(`\ `)
			i++
			start = i
			ch = '\\'
		}
		if ch != '\\' || i+1 >= len(js) {
			continue
		}
		switch js[i+1] {
		case 'u':
			sb.WriteString(js[start:i])
			code := js[i+2 : i+6]
			switch code {
			case "0000":
				sb.WriteString(`\0`)
			case "0007":
				sb.WriteString(`\a`)
			case "000b":
				sb.WriteString(`\v`)
			case "001b":
				sb.WriteString(`\e`)
			case "0085":
				sb.WriteString(`\N`)
			case "00a0":
				sb.WriteString(`\_`)
			case "2028":
				sb.WriteString(`\L`)
			case "2029":
				sb.WriteString(`\P`)
			default:
				if code[:2] == "00" {
					sb.WriteString(`\x` + code[2:])
				} else {
					sb.WriteString(js[i : i+6])
				}
			}
			i += 5
			start = i + 1
		case 'n':
			if implicitKey || (i+2 < len(js) && js[i+2] == '"') || utf16Len(js) < yamlDoubleQuotedMinMultiLine {
				i++
				continue
			}
			sb.WriteString(js[start:i])
			sb.WriteString("\n\n")
			for i+4 < len(js) && js[i+2] == '\\' && js[i+3] == 'n' && js[i+4] != '"' {
				sb.WriteString("\n")
				i += 2
			}
			sb.WriteString(indent)
			if i+2 < len(js) && js[i+2] == ' ' {
				sb.WriteString(`\`)
			}
			i++
			start = i + 1
		default:
			i++
		}
	}
	if start == 0 {
		return js
	}
	sb.WriteString(js[start:])
	return sb.String()
}

func yamlBlockString(s, indent string, implicitKey bool) string {
	if yamlBlockTrailing.MatchString(s) {
		return yamlQuotedString(s, indent, implicitKey)
	}
	if indent == "" && yamlDocumentMarker.MatchString(s) {
		indent = "  "
	}
	if s == "" {
		return "|\n"
	}
	endStart := len(s)
	for endStart > 0 {
		ch := s[endStart-1]
		if ch != '\n' && ch != '\t' && ch != ' ' {
			break
		}
		endStart--
	}
	end := s[endStart:]
	chomp := ""
	switch nl := strings.Index(end, "\n"); {
	case nl == -1:
		chomp = "-"
	case s == end || nl != len(end)-1:
		chomp = "+"
	}
	value := s
	if end != "" {
		value = s[:len(s)-len(end)]
		end = strings.TrimSuffix(end, "\n")
		end = indentInnerNewlineRuns(end, indent)
	}
	startWithSpace := false
	startNl := -1
	startEnd := 0
	for ; startEnd < len(value); startEnd++ {
		switch value[startEnd] {
		case ' ':
			startWithSpace = true
			continue
		case '\n':
			startNl = startEnd
			continue
		}
		break
	}
	lead := value[:startEnd]
	if startNl < startEnd {
		lead = value[:startNl+1]
	}
	if lead != "" {
		value = value[len(lead):]
		lead = replaceNewlineRuns(lead, func(run string) string { return run + indent })
	}
	header := chomp
	if startWithSpace {
		size := "1"
		if indent != "" {
			size = "2"
		}
		header = size + chomp
	}
	value = replaceNewlineRuns(value, func(run string) string { return run + indent })
	return "|" + header + "\n" + indent + lead + value + end
}

// replaceNewlineRuns rewrites every maximal run of \n characters.
func replaceNewlineRuns(s string, fn func(run string) string) string {
	var sb strings.Builder
	for i := 0; i < len(s); {
		if s[i] != '\n' {
			sb.WriteByte(s[i])
			i++
			continue
		}
		j := i
		for j < len(s) && s[j] == '\n' {
			j++
		}
		sb.WriteString(fn(s[i:j]))
		i = j
	}
	return sb.String()
}

// indentInnerNewlineRuns appends indent to runs of newlines that are neither
// at the end of s nor directly followed by more newlines.
func indentInnerNewlineRuns(s, indent string) string {
	var sb strings.Builder
	for i := 0; i < len(s); {
		if s[i] != '\n' {
			sb.WriteByte(s[i])
			i++
			continue
		}
		j := i
		for j < len(s) && s[j] == '\n' {
			j++
		}
		sb.WriteString(s[i:j])
		if j < len(s) {
			sb.WriteString(indent)
		}
		i = j
	}
	return sb.String()
}

// jsonStringify quotes s the way JavaScript's JSON.stringify does: unlike
// encoding/json it leaves <, > and & alone and escapes lone surrogates.
func jsonStringify(s string) string {
	var sb strings.Builder
	sb.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			sb.WriteString(`\"`)
		case '\\':
			sb.WriteString(`\\`)
		case '\b':
			sb.WriteString(`\b`)
		case '\f':
			sb.WriteString(`\f`)
		case '\n':
			sb.WriteString(`\n`)
		case '\r':
			sb.WriteString(`\r`)
		case '\t':
			sb.WriteString(`\t`)
		default:
			switch {
			case r < 0x20:
				fmt.Fprintf(&sb, `\u%04x`, r)
			case r >= 0xd800 && r <= 0xdfff:
				fmt.Fprintf(&sb, `\u%04x`, r)
			default:
				sb.WriteRune(r)
			}
		}
	}
	sb.WriteByte('"')
	return sb.String()
}

// utf16Len is the length JavaScript reports for s.
func utf16Len(s string) int { return len(utf16.Encode([]rune(s))) }
