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
	"slices"
	"strings"
)

var (
	blockDelimiter   = regexp.MustCompile(`^-{4,}$`)
	literalDelimiter = regexp.MustCompile(`^\.{4,}$`)
	fenceDelimiter   = regexp.MustCompile("^(`{3,}|~{3,})")
	metadataHeading  = regexp.MustCompile(`^==\s+Metadata\s*$`)
	sectionEnd       = regexp.MustCompile(`^(?:==\s+\S|include::|//\s*(?:tag|end)::)`)
	headingLine      = regexp.MustCompile(`^(?:={2,6}|#{2,6})\s+\S`)
	placeholderBrace = regexp.MustCompile(`\{([A-Za-z_][\w.-]{1,30})\}`)
)

type annotatedLine struct {
	text     string
	verbatim bool
}

// annotateLines splits text into lines and flags the ones inside listing,
// literal, or fenced blocks, where AsciiDoc substitutions don't apply.
func annotateLines(text string) []annotatedLine {
	var (
		inBlock, inLiteral bool
		fence              byte
	)
	lines := strings.Split(text, "\n")
	out := make([]annotatedLine, len(lines))
	for i, l := range lines {
		verbatim := true
		switch {
		case inBlock:
			if blockDelimiter.MatchString(l) {
				inBlock = false
			}
		case inLiteral:
			if literalDelimiter.MatchString(l) {
				inLiteral = false
			}
		case fence != 0:
			if m := fenceDelimiter.FindString(l); m != "" && m[0] == fence {
				fence = 0
			}
		default:
			if m := fenceDelimiter.FindString(l); m != "" {
				fence = m[0]
			} else if blockDelimiter.MatchString(l) {
				inBlock = true
			} else if literalDelimiter.MatchString(l) {
				inLiteral = true
			} else {
				verbatim = false
			}
		}
		out[i] = annotatedLine{text: l, verbatim: verbatim}
	}
	return out
}

func joinLines(lines []annotatedLine) string {
	parts := make([]string, len(lines))
	for i, l := range lines {
		parts[i] = l.text
	}
	return strings.Join(parts, "\n")
}

// escapePlaceholderBraces escapes {name} outside verbatim blocks so Asciidoctor
// doesn't treat it as an attribute reference and drop it.
func escapePlaceholderBraces(body string) string {
	lines := annotateLines(body)
	for i, l := range lines {
		if l.verbatim {
			continue
		}
		var sb strings.Builder
		last := 0
		for _, m := range placeholderBrace.FindAllStringIndex(l.text, -1) {
			if m[0] > 0 && (l.text[m[0]-1] == '\\' || l.text[m[0]-1] == '{') {
				continue
			}
			sb.WriteString(l.text[last:m[0]])
			sb.WriteString(`\`)
			sb.WriteString(l.text[m[0]:m[1]])
			last = m[1]
		}
		sb.WriteString(l.text[last:])
		lines[i].text = sb.String()
	}
	return joinLines(lines)
}

var (
	// Doubled characters that Asciidoctor treats as unconstrained emphasis,
	// bold, and highlight markers. They apply inside a backtick code span and
	// pair up across spans, so `__c` and `__b` on one line render with <em>
	// between them, and a glob like `'**/*.md'` renders as <strong>.
	unconstrainedMarkers = regexp.MustCompile(`__|\*\*|##`)
	codeSpan             = regexp.MustCompile("`([^`\n]+)`")
	passthroughEdge      = regexp.MustCompile(`^\+|\+$`)
	whitespaceEdge       = regexp.MustCompile(`^\s|\s$`)
)

// protectCodeSpans wraps code spans that contain unconstrained formatting
// markers in a +...+ passthrough so they render literally. It runs after
// escapePlaceholderBraces and removes the \{ escapes inside such spans, since
// a passthrough applies no attribute substitution. Verbatim blocks, existing
// passthroughs, and spans with whitespace at either edge (backticks that
// don't pair up as a code span) are left alone.
func protectCodeSpans(body string) string {
	lines := annotateLines(body)
	for i, l := range lines {
		if l.verbatim {
			continue
		}
		lines[i].text = codeSpan.ReplaceAllStringFunc(l.text, func(span string) string {
			content := span[1 : len(span)-1]
			if !unconstrainedMarkers.MatchString(content) || passthroughEdge.MatchString(content) || whitespaceEdge.MatchString(content) {
				return span
			}
			return "`+" + strings.ReplaceAll(content, `\{`, "{") + "+`"
		})
	}
	return joinLines(lines)
}

// ensureHeadingSeparation puts a blank line before every heading so that a
// heading directly under a paragraph isn't rendered as part of it.
func ensureHeadingSeparation(body string) string {
	var out []string
	for _, l := range annotateLines(body) {
		if !l.verbatim && headingLine.MatchString(l.text) && len(out) > 0 && strings.TrimSpace(out[len(out)-1]) != "" {
			out = append(out, "")
		}
		out = append(out, l.text)
	}
	return strings.Join(out, "\n")
}

// locateMetadata finds the `== Metadata` section of a description and returns
// its first and last line (inclusive), or ok false when there isn't one.
func locateMetadata(lines []annotatedLine) (first, last int, ok bool) {
	first = -1
	for i, l := range lines {
		if !l.verbatim && metadataHeading.MatchString(l.text) {
			first = i
			break
		}
	}
	if first == -1 {
		return 0, 0, false
	}
	end := len(lines)
	for i := first + 1; i < len(lines); i++ {
		if !lines[i].verbatim && sectionEnd.MatchString(lines[i].text) {
			end = i
			break
		}
	}
	last = end - 1
	for last > first && strings.TrimSpace(lines[last].text) == "" {
		last--
	}
	return first, last, true
}

func extractMetadata(description string) string {
	lines := annotateLines(description)
	first, last, ok := locateMetadata(lines)
	if !ok {
		return ""
	}
	return joinLines(lines[first : last+1])
}

// descriptionWithMetadataInclude replaces the `== Metadata` section with an
// include of the metadata partial, so the section renders from one place.
func descriptionWithMetadataInclude(description, typeDir, name string) string {
	lines := annotateLines(description)
	first, last, ok := locateMetadata(lines)
	if !ok {
		return description
	}
	include := annotatedLine{text: "include::connect:components:partial$metadata/" + typeDir + "/" + name + ".adoc[]"}
	replaced := append(append(append([]annotatedLine{}, lines[:first]...), include), lines[last+1:]...)
	return joinLines(replaced)
}

func renderDescriptionBody(description, typeDir, name string) string {
	body := descriptionWithMetadataInclude(description, typeDir, name)
	body = strings.TrimFunc(body, jsIsSpace)
	if body == "" {
		return ""
	}
	return protectCodeSpans(escapePlaceholderBraces(ensureHeadingSeparation(body)))
}

var (
	fenceOpen  = regexp.MustCompile("^(\\s*)(```|~~~)(.*)$")
	bulletLine = regexp.MustCompile(`^(\s*[-*]\s+)(.*)$`)
	// A parenthetical description may nest its own parentheses, for example
	// "lsn (... Not present on snapshot (`read`) messages.)".
	fieldBullet = regexp.MustCompile(`(?s)^([a-z][a-z0-9_]*)((?:\s*\(.*\))?(?:\s*(?::|-)\s.*)?)$`)
	listStart   = regexp.MustCompile(`^[=/+]`)
)

func normalizeBullet(prefix, content string) string {
	if strings.HasPrefix(content, "`") {
		return prefix + content
	}
	m := fieldBullet.FindStringSubmatch(content)
	if m == nil {
		return prefix + content
	}
	return prefix + "`" + m[1] + "`" + m[2]
}

func isFieldListFence(info string, content []string) bool {
	info = strings.TrimSpace(info)
	if info != "" && info != "text" {
		return false
	}
	var nonBlank []string
	for _, l := range content {
		if strings.TrimSpace(l) != "" {
			nonBlank = append(nonBlank, l)
		}
	}
	if len(nonBlank) == 0 {
		return false
	}
	anyField := false
	for _, l := range nonBlank {
		b := bulletLine.FindStringSubmatch(l)
		if b == nil {
			return false
		}
		if fieldBullet.MatchString(b[2]) {
			anyField = true
		}
	}
	return anyField
}

// normalizeMetadata puts metadata field names in inline code, unwraps field
// lists that upstream descriptions put in code fences, and adds the blank line
// a list needs when it directly follows a paragraph.
func normalizeMetadata(block string) string {
	if block == "" {
		return block
	}
	lines := strings.Split(block, "\n")
	var out []string
	// True while the lines since the last blank line belong to a list, so a
	// bullet after a wrapped bullet line continues the list.
	inList := false
	pushBullet := func(text string) {
		prev := ""
		if len(out) > 0 {
			prev = out[len(out)-1]
		}
		if !inList && strings.TrimSpace(prev) != "" && !listStart.MatchString(prev) {
			out = append(out, "")
		}
		inList = true
		out = append(out, text)
	}
	pushLine := func(text string) {
		if strings.TrimSpace(text) == "" {
			inList = false
		}
		out = append(out, text)
	}
	for i := 0; i < len(lines); i++ {
		if blockDelimiter.MatchString(lines[i]) {
			out = append(out, lines[i])
			j := i + 1
			for ; j < len(lines); j++ {
				out = append(out, lines[j])
				if blockDelimiter.MatchString(lines[j]) {
					break
				}
			}
			i = j
			continue
		}
		if fence := fenceOpen.FindStringSubmatch(lines[i]); fence != nil {
			marker := fence[2]
			var content []string
			j := i + 1
			closed := false
			for ; j < len(lines); j++ {
				if strings.TrimSpace(lines[j]) == marker {
					closed = true
					break
				}
				content = append(content, lines[j])
			}
			if closed && isFieldListFence(fence[3], content) {
				for _, c := range content {
					if b := bulletLine.FindStringSubmatch(c); b != nil {
						pushBullet(normalizeBullet(b[1], b[2]))
					} else {
						pushLine(c)
					}
				}
			} else {
				out = append(out, lines[i])
				out = append(out, content...)
				if closed {
					out = append(out, lines[j])
				}
			}
			if closed {
				i = j
			} else {
				i = len(lines)
			}
			continue
		}
		if b := bulletLine.FindStringSubmatch(lines[i]); b != nil {
			pushBullet(normalizeBullet(b[1], b[2]))
		} else {
			pushLine(lines[i])
		}
	}
	return strings.Join(out, "\n")
}

var (
	attrXrefLink  = regexp.MustCompile(`(?:xref|link):[^\[\]]+\[([^\]]*)\]`)
	attrBareURL   = regexp.MustCompile(`https?://[^\s\[\]]+\[([^\]]*)\]`)
	attrImage     = regexp.MustCompile(`image:{1,2}[^\[\]\s]*\[[^\]]*\]`)
	attrXrefShort = regexp.MustCompile(`<<[^,>]+,([^>]+)>>`)
	attrXrefBare  = regexp.MustCompile(`<<([^>]+)>>`)
	attrCode      = regexp.MustCompile("`([^`]*)`")
)

// flattenToAttributeValue turns a summary into plain text for the
// :description: attribute, which feeds search snippets and meta tags.
func flattenToAttributeValue(text string) string {
	text = attrXrefLink.ReplaceAllString(text, "${1}")
	text = attrBareURL.ReplaceAllString(text, "${1}")
	text = attrImage.ReplaceAllString(text, "")
	text = attrXrefShort.ReplaceAllString(text, "${1}")
	text = attrXrefBare.ReplaceAllString(text, "${1}")
	text = strings.ReplaceAll(text, "^", "")
	text = attrCode.ReplaceAllString(text, "${1}")
	text = strings.Join(strings.FieldsFunc(text, jsIsSpace), " ")
	return text
}

var (
	summaryBlockLine = regexp.MustCompile("^(?:`{3,}|~{3,}|-{4,}$|={4,}$|\\*{4,}$|\\.{4,}$|\\s*[-*]\\s|\\s*\\d+\\.\\s)")
	sentenceEnd      = regexp.MustCompile(`[.!?](?:\s|$)`)
)

// summaryHasBlocks reports whether a summary holds block content, such as a
// code fence, a delimited block, a list, or a second paragraph. A summary like
// that can't become a one-line :description: attribute. Soft line breaks
// inside a paragraph are fine.
func summaryHasBlocks(summary string) bool {
	trimmed := strings.TrimFunc(summary, jsIsSpace)
	if strings.Contains(trimmed, "\n\n") {
		return true
	}
	return slices.ContainsFunc(strings.Split(trimmed, "\n"), summaryBlockLine.MatchString)
}

// metaSummary returns the text for the :description: attribute. When the
// summary holds block content, such as the sample message in the
// cockroachdb_changefeed summary, only its first sentence is used, so search
// snippets never show flattened code.
func metaSummary(summary string) string {
	if !summaryHasBlocks(summary) {
		return summary
	}
	first := strings.TrimFunc(summary, jsIsSpace)
	if i := strings.Index(first, "\n\n"); i >= 0 {
		first = first[:i]
	}
	for i, l := range strings.Split(first, "\n") {
		if summaryBlockLine.MatchString(l) {
			first = strings.Join(strings.Split(first, "\n")[:i], "\n")
			break
		}
	}
	if loc := sentenceEnd.FindStringIndex(first); loc != nil {
		first = first[:loc[0]+1]
	}
	return strings.TrimFunc(first, jsIsSpace)
}

var componentXref = regexp.MustCompile(`xref:(?:components:)?(inputs|outputs|processors|caches|rate_limits|buffers|metrics|tracers|scanners)/([a-z0-9_]+)\.adoc(?:#[^\[\s]*)?\[([^\]]*)\]`)

// cloudGuardXrefs keeps links to components that Redpanda Cloud doesn't
// include out of the Cloud docs, which include the same partials. Each
// paragraph with such a link renders as is outside Cloud, and in Cloud with
// those links replaced by their text. excluded reports whether the component
// at <typeDir>/<name> is a documented component that Cloud doesn't include.
// Paragraphs already inside an env-cloud conditional are left alone.
func cloudGuardXrefs(body string, excluded func(typeDir, name string) bool) string {
	unlink := func(text string) (string, bool) {
		changed := false
		out := componentXref.ReplaceAllStringFunc(text, func(x string) string {
			m := componentXref.FindStringSubmatch(x)
			if !excluded(m[1], m[2]) {
				return x
			}
			changed = true
			if m[3] != "" {
				return m[3]
			}
			return "`" + m[2] + "`"
		})
		return out, changed
	}
	var out, para, cloudPara []string
	depth := 0
	needsGuard := false
	flush := func() {
		if needsGuard {
			out = append(out, "ifndef::env-cloud[]")
			out = append(out, para...)
			out = append(out, "endif::[]", "ifdef::env-cloud[]")
			out = append(out, cloudPara...)
			out = append(out, "endif::[]")
		} else {
			out = append(out, para...)
		}
		para, cloudPara, needsGuard = nil, nil, false
	}
	for _, l := range annotateLines(body) {
		text := l.text
		switch {
		case l.verbatim:
			para, cloudPara = append(para, text), append(cloudPara, text)
			continue
		case strings.TrimSpace(text) == "":
			flush()
			out = append(out, text)
			continue
		case strings.HasPrefix(text, "ifdef::") || strings.HasPrefix(text, "ifndef::"):
			flush()
			depth++
			out = append(out, text)
			continue
		case strings.HasPrefix(text, "endif::"):
			flush()
			if depth > 0 {
				depth--
			}
			out = append(out, text)
			continue
		}
		cloudText := text
		if depth == 0 {
			var changed bool
			if cloudText, changed = unlink(text); changed {
				needsGuard = true
			}
		}
		para, cloudPara = append(para, text), append(cloudPara, cloudText)
	}
	flush()
	return strings.Join(out, "\n")
}

// componentPlatform is where a component is available, as computed from the
// builds and the Redpanda Cloud schemas.
type componentPlatform struct {
	CgoOnly bool
	Cloud   bool
	CloudAI bool
}

// inCloud reports whether any Redpanda Cloud pipeline can use the component.
func (p componentPlatform) inCloud() bool { return p.Cloud || p.CloudAI }

// renderDescriptionPartial renders the description partial of a component.
// The meta tag sets page attributes for the page header, the body tag renders
// the status notice, summary, version note, and description, and the
// footnotes tag renders the spec footnotes. Every tag is always present, so a
// page that includes one keeps building when the spec drops that content.
// excluded is as in cloudGuardXrefs, and is used only for components that
// Redpanda Cloud includes.
func renderDescriptionPartial(c componentSpec, typeDir string, plat componentPlatform, excluded func(typeDir, name string) bool) string {
	guard := func(body string) string {
		if !plat.inCloud() || excluded == nil {
			return body
		}
		return cloudGuardXrefs(body, excluded)
	}
	summary := escapePlaceholderBraces(c.Summary)
	var b strings.Builder
	b.WriteString(descBanner + "\n\n// tag::meta[]\n")
	if summary != "" {
		b.WriteString(":description: " + flattenToAttributeValue(escapePlaceholderBraces(metaSummary(c.Summary))) + "\n")
	}
	if c.Status != "" {
		b.WriteString(":status: " + c.Status + "\n")
	}
	if plat.CgoOnly {
		b.WriteString(":page-cgo-only: true\n")
	}
	b.WriteString("// end::meta[]\n\n// tag::body[]\n")
	if notice := statusNotice(c.Status, "component"); notice != "" {
		b.WriteString(notice + "\n")
	}
	if summary != "" {
		b.WriteString(guard(summary) + "\n\n")
	}
	if c.Version != "" {
		b.WriteString("ifndef::env-cloud[]\nIntroduced in version " + htmlEscaper.Replace(c.Version) + ".\nendif::[]\n\n")
	}
	b.WriteString(guard(renderDescriptionBody(c.Description, typeDir, c.Name)) + "\n// end::body[]\n\n")
	b.WriteString("// tag::footnotes[]\n")
	if footnotes := renderFootnotes(c.Footnotes); footnotes != "" {
		b.WriteString(guard(footnotes) + "\n")
	}
	b.WriteString("// end::footnotes[]\n")
	return b.String()
}

// renderFootnotes prepares spec footnotes the same way as the description,
// keeping their source heading levels.
func renderFootnotes(footnotes string) string {
	body := strings.TrimFunc(footnotes, jsIsSpace)
	if body == "" {
		return ""
	}
	return protectCodeSpans(escapePlaceholderBraces(ensureHeadingSeparation(body)))
}
