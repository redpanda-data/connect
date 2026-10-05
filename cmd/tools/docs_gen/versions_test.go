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
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/redpanda-data/connect/v4/public/schema"
)

var semver = regexp.MustCompile(`^(\d+)\.(\d+)\.(\d+)$`)

// TestNewSpecsHaveVersions fails when a component, field, Bloblang function,
// or Bloblang method has a version that isn't a release it could have shipped
// in, or when one that isn't in the previous release has no version. The
// version is published as "Introduced in version ..." in the reference docs.
//
// The previous release is the highest vX.Y.Z tag reachable from HEAD. Every
// version must be a plain x.y.z release number with a major version of at
// least 1, and no later than the next minor release after the previous one.
//
// The contents of the previous release are read from the reference partials
// generated into its tag, which CI keeps in step with the code. A new field
// inside a new component or a new object field is covered by its parent's
// version. Deprecated and hidden specs don't need a version. Releases from
// before the partials existed have no reliable record of what they shipped,
// so for them only the version numbers themselves are checked.
//
// CI runs this test with and without the x_benthos_extra build tag, so the
// components that only build with it are checked too.
func TestNewSpecsHaveVersions(t *testing.T) {
	tag, prev := previousRelease(t)
	released := releasedSpecs(t, tag)
	if released == nil {
		t.Logf("%s has no generated reference partials, so only version numbers are checked; the check for unversioned new specs starts with the first release that has them", tag)
	}

	raw, err := schema.Standard("", "").MarshalJSONV0()
	if err != nil {
		t.Fatal(err)
	}
	full, err := parseFullSchema(raw)
	if err != nil {
		t.Fatal(err)
	}

	for _, p := range versionProblems(full, released, tag, prev) {
		t.Error(p)
	}
}

// versionProblems lists the specs in full whose version isn't valid for a
// codebase whose previous release is tag (version prev). A version is valid
// when it's a release number from 1.0.0 up to the next minor release after
// prev. When released (the specs documented in tag) is non-nil, a spec that
// isn't in it must also have a version, and that version must be later than
// prev.
func versionProblems(full *fullSchema, released map[string]bool, tag string, prev [3]int) []string {
	next := [3]int{prev[0], prev[1] + 1, 0}
	nextStr := fmt.Sprintf("%d.%d.%d", next[0], next[1], next[2])
	var problems []string
	check := func(what, version string, isNew, exempt bool) {
		isNew = isNew && released != nil && !exempt
		v, ok := parseVersion(version)
		switch {
		case version != "" && !ok:
			problems = append(problems, fmt.Sprintf("%s: version %q must be a release number such as %q", what, version, nextStr))
		case version != "" && v[0] == 0:
			problems = append(problems, fmt.Sprintf("%s: version %q isn't a release; set the release it first shipped in", what, version))
		case version != "" && newer(version, next):
			problems = append(problems, fmt.Sprintf("%s: version %q is later than %s, the next release after %s", what, version, nextStr, tag))
		case version != "" && isNew && !newer(version, prev):
			problems = append(problems, fmt.Sprintf("%s: is not in %s, so its version %q must be later than that release", what, tag, version))
		case version == "" && isNew:
			problems = append(problems, fmt.Sprintf("%s: is not in %s, so it needs .Version(%q) (the release it first ships in)", what, tag, nextStr))
		}
	}

	var walk func(comp, prefix string, fields []fieldSpec, parentNew, exempt bool)
	walk = func(comp, prefix string, fields []fieldSpec, parentNew, exempt bool) {
		for _, f := range fields {
			p := prefix + f.Name
			fExempt := exempt || f.IsDeprecated
			isNew := !parentNew && !released[comp+":"+p]
			check(comp+" field "+p, f.Version, isNew, fExempt)
			walk(comp, p+".", f.Children, parentNew || isNew, fExempt)
		}
	}
	for _, g := range full.Groups {
		for _, c := range g.Components {
			comp := pageTypeDir(g.Key) + "/" + c.Name
			isNew := !released[comp]
			exempt := c.Status == "deprecated"
			check(comp, c.Version, isNew, exempt)
			walk(comp, "", c.Config.Children, isNew, exempt)
		}
	}
	for _, f := range full.BloblangFunctions {
		check("Bloblang function "+f.Name, f.Version, !released["function:"+f.Name], f.Status == "deprecated" || f.Status == "hidden")
	}
	for _, m := range full.BloblangMethods {
		check("Bloblang method "+m.Name, m.Version, !released["method:"+m.Name], m.Status == "deprecated" || m.Status == "hidden")
	}
	return problems
}

// previousRelease returns the highest stable vX.Y.Z tag reachable from HEAD.
func previousRelease(t *testing.T) (string, [3]int) {
	t.Helper()
	out, err := exec.Command("git", "tag", "--merged", "HEAD", "--list", "v*").Output()
	if err != nil || len(bytes.TrimSpace(out)) == 0 {
		if os.Getenv("CI") != "" {
			t.Fatalf("listing release tags (the checkout needs tags, for example fetch-depth: 0): %v", err)
		}
		t.Skip("no release tags reachable from HEAD")
	}
	best, bestV := "", [3]int{-1}
	for tag := range strings.FieldsSeq(string(out)) {
		if v, ok := parseVersion(strings.TrimPrefix(tag, "v")); ok && newer(fmt.Sprintf("%d.%d.%d", v[0], v[1], v[2]), bestV) {
			best, bestV = tag, v
		}
	}
	if best == "" {
		t.Fatal("no vX.Y.Z tag reachable from HEAD")
	}
	return best, bestV
}

func parseVersion(s string) ([3]int, bool) {
	m := semver.FindStringSubmatch(s)
	if m == nil {
		return [3]int{}, false
	}
	var v [3]int
	for i := range v {
		v[i], _ = strconv.Atoi(m[i+1])
	}
	return v, true
}

func newer(version string, than [3]int) bool {
	v, ok := parseVersion(version)
	if !ok {
		return false
	}
	for i := range v {
		if v[i] != than[i] {
			return v[i] > than[i]
		}
	}
	return false
}

var (
	fieldHeading    = regexp.MustCompile("^=== `([^`]+)`\\s*$")
	mapKeySegment   = regexp.MustCompile(`\.?<[^>]+>`)
	fieldsPartial   = regexp.MustCompile(`^docs/modules/components/partials/fields/([a-z_-]+)/([^/]+)\.adoc$`)
	examplePartial  = regexp.MustCompile(`^docs/modules/components/examples/common/([a-z_-]+)/([^/]+)\.yaml$`)
	bloblangPartial = regexp.MustCompile(`^docs/modules/components/partials/bloblang-(function|method)s/([^/]+)\.adoc$`)
)

// releasedSpecs reads the components, fields, and Bloblang functions and
// methods whose reference partials were generated into tag. It returns nil for
// releases from before the partials existed, which have no reliable record.
func releasedSpecs(t *testing.T, tag string) map[string]bool {
	t.Helper()
	out, err := exec.Command("git", "ls-tree", "-r", "--full-tree", "--name-only", tag, "--", "docs/modules/components").Output()
	if err != nil {
		t.Fatalf("listing docs in %s: %v", tag, err)
	}
	files := strings.Fields(string(out))
	contents := readBlobs(t, tag, files)

	released := map[string]bool{}
	for _, f := range files {
		switch {
		case fieldsPartial.MatchString(f):
			m := fieldsPartial.FindStringSubmatch(f)
			comp := pageTypeDir(m[1]) + "/" + m[2]
			released[comp] = true
			for line := range strings.SplitSeq(contents[f], "\n") {
				if h := fieldHeading.FindStringSubmatch(line); h != nil {
					released[comp+":"+headingPath(h[1])] = true
				}
			}
		case examplePartial.MatchString(f):
			m := examplePartial.FindStringSubmatch(f)
			released[pageTypeDir(m[1])+"/"+m[2]] = true
		case bloblangPartial.MatchString(f):
			m := bloblangPartial.FindStringSubmatch(f)
			released[m[1]+":"+m[2]] = true
		}
	}
	if len(released) == 0 {
		return nil
	}
	return released
}

// headingPath turns a field heading such as `tools[].parameters.properties.<name>.type`
// or `[].check` into the schema path `tools.parameters.properties.type` or `check`.
func headingPath(h string) string {
	h = mapKeySegment.ReplaceAllString(strings.ReplaceAll(h, "[]", ""), "")
	return strings.TrimPrefix(h, ".")
}

// readBlobs reads the .adoc files at tag with a single git cat-file process.
func readBlobs(t *testing.T, tag string, files []string) map[string]string {
	t.Helper()
	var want []string
	var in bytes.Buffer
	for _, f := range files {
		if strings.HasSuffix(f, ".adoc") {
			want = append(want, f)
			in.WriteString(tag + ":" + f + "\n")
		}
	}
	cmd := exec.Command("git", "cat-file", "--batch")
	cmd.Stdin = &in
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("reading docs in %s: %v", tag, err)
	}
	r := bufio.NewReader(bytes.NewReader(out))
	contents := map[string]string{}
	for _, f := range want {
		header, err := r.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		parts := strings.Fields(header)
		if len(parts) != 3 {
			t.Fatalf("reading %s: %q", f, header)
		}
		size, _ := strconv.Atoi(parts[2])
		buf := make([]byte, size+1)
		if _, err := io.ReadFull(r, buf); err != nil {
			t.Fatal(err)
		}
		contents[f] = string(buf[:size])
	}
	return contents
}

func TestVersionProblems(t *testing.T) {
	full := &fullSchema{
		Groups: []componentGroup{{Key: "inputs", Components: []componentSpec{
			{Name: "old", Config: fieldSpec{Children: []fieldSpec{
				{Name: "kept"},
				{Name: "added"},
				{Name: "added_versioned", Version: "4.112.0"},
				{Name: "added_stale", Version: "4.100.0"},
				{Name: "gone", IsDeprecated: true},
				{Name: "obj", Version: "4.112.0", Children: []fieldSpec{{Name: "inner"}}},
				{Name: "bad", Version: "v4.2.0"},
				{Name: "zero", Version: "0.0.1"},
				{Name: "future", Version: "4.200.0"},
			}}},
			{Name: "brand_new", Version: "4.112.0", Config: fieldSpec{Children: []fieldSpec{{Name: "f"}}}},
			{Name: "brand_new_unversioned"},
			{Name: "retired", Status: "deprecated"},
		}}},
		BloblangFunctions: []bloblangSpec{{Name: "old_fn"}, {Name: "new_fn"}, {Name: "secret_fn", Status: "hidden"}},
		BloblangMethods:   []bloblangSpec{{Name: "new_method", Version: "4.112.0"}},
	}
	released := map[string]bool{
		"inputs/old": true, "inputs/old:kept": true, "inputs/old:bad": true, "inputs/old:zero": true, "inputs/old:future": true, "function:old_fn": true,
	}
	got := versionProblems(full, released, "v4.111.0", [3]int{4, 111, 0})
	want := []string{
		`inputs/old field added: is not in v4.111.0, so it needs .Version("4.112.0") (the release it first ships in)`,
		`inputs/old field added_stale: is not in v4.111.0, so its version "4.100.0" must be later than that release`,
		`inputs/old field bad: version "v4.2.0" must be a release number such as "4.112.0"`,
		`inputs/old field zero: version "0.0.1" isn't a release; set the release it first shipped in`,
		`inputs/old field future: version "4.200.0" is later than 4.112.0, the next release after v4.111.0`,
		`inputs/brand_new_unversioned: is not in v4.111.0, so it needs .Version("4.112.0") (the release it first ships in)`,
		`Bloblang function new_fn: is not in v4.111.0, so it needs .Version("4.112.0") (the release it first ships in)`,
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("got:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestHeadingPath(t *testing.T) {
	for in, want := range map[string]string{
		"tls.client_certs[].cert":                   "tls.client_certs.cert",
		"[].check":                                  "check",
		"branches.<name>.request_map":               "branches.request_map",
		"tools[].parameters.properties.<name>.type": "tools.parameters.properties.type",
		"topics": "topics",
	} {
		if got := headingPath(in); got != want {
			t.Errorf("headingPath(%q) = %q, want %q", in, got, want)
		}
	}
}
