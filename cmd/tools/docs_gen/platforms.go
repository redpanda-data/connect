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
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/redpanda-data/connect/v4/internal/plugins"
	"github.com/redpanda-data/connect/v4/public/schema"
)

// allComponentsPkg imports every component docs_gen documents. Comparing the
// Go files a standard build of it compiles with the files the docs_gen build
// compiles shows which components only cgo and x_benthos_extra builds have.
const allComponentsPkg = "github.com/redpanda-data/connect/v4/cmd/tools/docs_gen/allcomponents"

// releaseBinaryPkg is the standard redpanda-connect binary. Components that a
// standard build of allComponentsPkg has and this package doesn't are the
// ones only Redpanda Cloud runs, such as a2a_message.
const releaseBinaryPkg = "github.com/redpanda-data/connect/v4/cmd/redpanda-connect"

// standardBuildEnv returns env with cgo disabled and any -tags flag removed
// from GOFLAGS, which is how .goreleaser/connect.yaml builds the standard
// release binaries (timetzdata, its one tag, doesn't change the components).
func standardBuildEnv(env []string) []string {
	out := make([]string, 0, len(env)+1)
	for _, kv := range env {
		k, v, _ := strings.Cut(kv, "=")
		switch k {
		case "CGO_ENABLED":
			continue
		case "GOFLAGS":
			var kept []string
			for f := range strings.FieldsSeq(v) {
				if !strings.HasPrefix(f, "-tags=") && !strings.HasPrefix(f, "--tags=") {
					kept = append(kept, f)
				}
			}
			if len(kept) == 0 {
				continue
			}
			kv = k + "=" + strings.Join(kept, " ")
		}
		out = append(out, kv)
	}
	return append(out, "CGO_ENABLED=0")
}

// buildFiles returns the Go files, by path, that a build of pkg compiles with
// env and the given go flags. go list reads build constraints
// without compiling, so this takes about a second.
func buildFiles(pkg string, env []string, flags ...string) (map[string]bool, error) {
	args := append([]string{"list", "-deps", "-f", "{{.Dir}}{{range .GoFiles}}|{{.}}{{end}}{{range .CgoFiles}}|{{.}}{{end}}"}, flags...)
	cmd := exec.Command("go", append(args, pkg)...)
	cmd.Env = env
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("running `go %v`: %w\n%s", strings.Join(cmd.Args[1:], " "), err, stderr.String())
	}
	files := map[string]bool{}
	for line := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		dir, names, _ := strings.Cut(line, "|")
		for name := range strings.SplitSeq(names, "|") {
			if name != "" {
				files[filepath.Join(dir, name)] = true
			}
		}
	}
	return files, nil
}

// docsGenBuildEnv returns the environment and flags of the build docs_gen
// itself runs as. The x_benthos_extra tag can't be read from the environment,
// so builtWithAllComponents supplies it, and it implies cgo.
func docsGenBuildEnv() ([]string, []string) {
	env := os.Environ()
	if !builtWithAllComponents {
		return env, nil
	}
	out := make([]string, 0, len(env)+1)
	for _, kv := range env {
		if !strings.HasPrefix(kv, "CGO_ENABLED=") {
			out = append(out, kv)
		}
	}
	return append(out, "CGO_ENABLED=1"), []string{"-tags=x_benthos_extra"}
}

// registerGroups maps a benthos service.MustRegister... or Register...
// function, without its prefix, to the schema group of what it registers.
var registerGroups = map[string]string{
	"Input": "inputs", "BatchInput": "inputs",
	"Output": "outputs", "BatchOutput": "outputs",
	"Processor": "processors", "BatchProcessor": "processors",
	"Cache": "caches", "RateLimit": "rate-limits",
	"Buffer": "buffers", "BatchBuffer": "buffers",
	"MetricsExporter": "metrics", "OtelTracerProvider": "tracers",
	"BatchScannerCreator": "scanners",
}

// cgoOnlyComponents returns the components, as componentKey values, that the
// docs_gen build registers in Go files a standard build (no cgo, no tags)
// doesn't compile. It reads each name from the service.MustRegister... call
// in those files, resolving a constant name within its package.
//
// Anything it can't read is an error rather than a silent gap: a register
// call of a kind it doesn't know (such as a template, whose name is in its
// YAML), or an init function in such a file that registers nothing it can
// see (such as one that calls a register helper in a shared file).
func cgoOnlyComponents() (map[string]bool, error) {
	env, flags := docsGenBuildEnv()
	full, err := buildFiles(allComponentsPkg, env, flags...)
	if err != nil {
		return nil, err
	}
	standard, err := buildFiles(allComponentsPkg, standardBuildEnv(os.Environ()))
	if err != nil {
		return nil, err
	}
	return registrationsOnlyIn(full, standard)
}

// notInReleaseBinary returns the components that a standard build of
// allcomponents registers and the standard redpanda-connect binary doesn't:
// the components only Redpanda Cloud runs.
func notInReleaseBinary() (map[string]bool, error) {
	env := standardBuildEnv(os.Environ())
	all, err := buildFiles(allComponentsPkg, env)
	if err != nil {
		return nil, err
	}
	binary, err := buildFiles(releaseBinaryPkg, env)
	if err != nil {
		return nil, err
	}
	return registrationsOnlyIn(all, binary)
}

// registrationsOnlyIn returns the components registered in the files of
// files that other doesn't compile.
func registrationsOnlyIn(files, other map[string]bool) (map[string]bool, error) {
	// Only this module and benthos register components. Other dependencies
	// can also have files that only cgo builds compile, such as a macOS
	// keychain backend.
	modDirs, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "github.com/redpanda-data/connect/v4", "github.com/redpanda-data/benthos/v4").Output()
	if err != nil {
		return nil, fmt.Errorf("finding the module directories: %w", err)
	}
	var mods []string
	for d := range strings.FieldsSeq(string(modDirs)) {
		mods = append(mods, d+string(filepath.Separator))
	}
	inModule := func(path string) bool {
		for _, m := range mods {
			if strings.HasPrefix(path, m) {
				return true
			}
		}
		return false
	}
	keys := map[string]bool{}
	fset := token.NewFileSet()
	for path := range files {
		if other[path] || !inModule(path) {
			continue
		}
		found, err := fileRegistrations(path, files, fset)
		if err != nil {
			return nil, err
		}
		for _, k := range found {
			keys[k] = true
		}
	}
	return keys, nil
}

// fileRegistrations returns the components, as componentKey values, that the
// file at path registers, or an error when it registers something it can't
// read. files is the docs_gen build's file set, for resolving constants.
func fileRegistrations(path string, files map[string]bool, fset *token.FileSet) ([]string, error) {
	f, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		return nil, err
	}
	var keys []string
	var walkErr error
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) == 0 || walkErr != nil {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if x, ok := sel.X.(*ast.Ident); !ok || x.Name != "service" {
			return true
		}
		if !strings.HasPrefix(strings.TrimPrefix(sel.Sel.Name, "Must"), "Register") {
			return true
		}
		kind := strings.TrimPrefix(strings.TrimPrefix(sel.Sel.Name, "Must"), "Register")
		group, ok := registerGroups[kind]
		if !ok {
			walkErr = fmt.Errorf("%v: service.%v registers a component that only cgo builds have, but docs_gen can't read its name; add the kind to registerGroups", fset.Position(call.Pos()), sel.Sel.Name)
			return false
		}
		name, err := registeredName(call.Args[0], filepath.Dir(path), files, fset)
		if err != nil {
			walkErr = fmt.Errorf("%v: %w", fset.Position(call.Pos()), err)
			return false
		}
		keys = append(keys, componentKey(group, name))
		return true
	})
	if walkErr != nil {
		return nil, walkErr
	}
	if len(keys) == 0 && hasInit(f) {
		return nil, fmt.Errorf("%v: this file only cgo builds compile has an init function, but docs_gen finds no component registration in it; register the component in this file, or teach cgoOnlyComponents where it is", path)
	}
	return keys, nil
}

// registeredName returns the component name a register call passes: a string
// literal, or a string constant declared in the same package.
func registeredName(arg ast.Expr, dir string, files map[string]bool, fset *token.FileSet) (string, error) {
	switch a := arg.(type) {
	case *ast.BasicLit:
		if a.Kind == token.STRING {
			return strconv.Unquote(a.Value)
		}
	case *ast.Ident:
		for path := range files {
			if filepath.Dir(path) != dir {
				continue
			}
			f, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				return "", err
			}
			if obj := f.Scope.Lookup(a.Name); obj != nil && obj.Kind == ast.Con {
				if vs, ok := obj.Decl.(*ast.ValueSpec); ok {
					for i, n := range vs.Names {
						if n.Name == a.Name && i < len(vs.Values) {
							return registeredName(vs.Values[i], dir, files, fset)
						}
					}
				}
			}
		}
	}
	return "", fmt.Errorf("can't read the component name from %T", arg)
}

// withoutComponents returns a copy of s without the given components, which
// is the schema a standard build has.
func withoutComponents(s *fullSchema, keys map[string]bool) *fullSchema {
	out := &fullSchema{BloblangFunctions: s.BloblangFunctions, BloblangMethods: s.BloblangMethods}
	for _, g := range s.Groups {
		ng := componentGroup{Key: g.Key}
		for _, c := range g.Components {
			if !keys[componentKey(g.Key, c.Name)] {
				ng.Components = append(ng.Components, c)
			}
		}
		out.Groups = append(out.Groups, ng)
	}
	return out
}

func componentKey(group, name string) string { return group + "/" + name }

func componentNames(s *fullSchema) map[string]bool {
	names := map[string]bool{}
	for _, g := range s.Groups {
		for _, c := range g.Components {
			if c.Name != "" {
				names[componentKey(g.Key, c.Name)] = true
			}
		}
	}
	return names
}

// platformSet records where each component in the docs_gen build is
// available, keyed by componentKey.
type platformSet struct {
	byKey map[string]componentPlatform
	// documented is every component in the docs_gen build.
	documented map[string]bool
}

// newPlatformSet compares the docs_gen schema (full) with the schema of a
// standard build and with the Redpanda Cloud and Cloud GPU schemas. A
// component is cgo-only when the docs_gen build has it and the standard build
// doesn't.
func newPlatformSet(full, standard, cloud, cloudAI *fullSchema) (platformSet, error) {
	fullNames, stdNames := componentNames(full), componentNames(standard)
	if len(stdNames) == 0 {
		return platformSet{}, errors.New("the standard build registers no components")
	}
	var extra []string
	for k := range stdNames {
		if !fullNames[k] {
			extra = append(extra, k)
		}
	}
	if len(extra) > 0 {
		sort.Strings(extra)
		return platformSet{}, fmt.Errorf("the standard build registers components that the docs_gen build doesn't, so the two import different packages: %v", strings.Join(extra, ", "))
	}
	cloudNames, cloudAINames := componentNames(cloud), componentNames(cloudAI)
	p := platformSet{byKey: map[string]componentPlatform{}, documented: fullNames}
	for k := range fullNames {
		p.byKey[k] = componentPlatform{
			CgoOnly: !stdNames[k],
			Cloud:   cloudNames[k],
			CloudAI: cloudAINames[k],
		}
	}
	return p, nil
}

// cgoOnlyKeys returns the cgo-only components, sorted.
func (p platformSet) cgoOnlyKeys() []string {
	var keys []string
	for k, v := range p.byKey {
		if v.CgoOnly {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}

func (p platformSet) get(group, name string) componentPlatform {
	return p.byKey[componentKey(group, name)]
}

// cloudExcluded reports whether <typeDir>/<name> is a documented component
// that no Redpanda Cloud pipeline can use. Links to anything else, including
// pages that aren't components, are left to the Cloud docs build to check.
func (p platformSet) cloudExcluded(typeDir, name string) bool {
	group, ok := keyByTypeDir[typeDir]
	if !ok {
		return false
	}
	k := componentKey(group, name)
	return p.documented[k] && !p.byKey[k].inCloud()
}

// loadPlatforms computes the platform set for the docs_gen schema.
func loadPlatforms(full *fullSchema) (platformSet, error) {
	cgoOnly, err := cgoOnlyComponents()
	if err != nil {
		return platformSet{}, fmt.Errorf("finding cgo-only components: %w", err)
	}
	fullNames := componentNames(full)
	var unknown []string
	for k := range cgoOnly {
		if !fullNames[k] {
			unknown = append(unknown, k)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return platformSet{}, fmt.Errorf("found registrations for components the docs_gen build doesn't have: %v", strings.Join(unknown, ", "))
	}
	standard := withoutComponents(full, cgoOnly)
	cloud, err := marshalSchema(schema.Cloud("", "").MarshalJSONV0())
	if err != nil {
		return platformSet{}, fmt.Errorf("cloud schema: %w", err)
	}
	cloudAI, err := marshalSchema(schema.CloudAI("", "").MarshalJSONV0())
	if err != nil {
		return platformSet{}, fmt.Errorf("cloud AI schema: %w", err)
	}
	return newPlatformSet(full, standard, cloud, cloudAI)
}

func marshalSchema(raw []byte, err error) (*fullSchema, error) {
	if err != nil {
		return nil, err
	}
	return parseFullSchema(raw)
}

type catalogEntry struct {
	Type            string   `json:"type"`
	Name            string   `json:"name"`
	Status          string   `json:"status"`
	Version         string   `json:"version"`
	Categories      []string `json:"categories"`
	Summary         string   `json:"summary"`
	Cloud           bool     `json:"cloud"`
	CloudAI         bool     `json:"cloud_ai"`
	CgoOnly         bool     `json:"cgo_only"`
	Support         string   `json:"support"`
	CommercialNames []string `json:"commercial_names"`
}

// renderCatalog renders partials/platforms/catalog.json: one entry per
// documented component, sorted by type and then name, with its availability
// and its internal/plugins/info.csv support level and commercial name.
func renderCatalog(full *fullSchema, plat platformSet, info plugins.InfoCollection) (string, error) {
	type infoKey struct {
		t    plugins.TypeName
		name string
	}
	rows := map[infoKey]plugins.PluginInfo{}
	for _, r := range info {
		rows[infoKey{r.Type, r.Name}] = r
	}
	var entries []catalogEntry
	for _, g := range full.Groups {
		t, ok := pluginTypes[g.Key]
		if !ok {
			return "", fmt.Errorf("no plugin type for schema key %v", g.Key)
		}
		for _, c := range g.Components {
			if c.Name == "" {
				continue
			}
			p := plat.get(g.Key, c.Name)
			e := catalogEntry{
				Type:            string(t),
				Name:            c.Name,
				Status:          c.Status,
				Version:         c.Version,
				Categories:      append([]string{}, c.Categories...),
				Summary:         flattenToAttributeValue(metaSummary(c.Summary)),
				Cloud:           p.Cloud,
				CloudAI:         p.CloudAI,
				CgoOnly:         p.CgoOnly,
				CommercialNames: []string{},
			}
			if r, ok := rows[infoKey{t, c.Name}]; ok {
				e.Support = r.Support
				if r.CommercialName != "" {
					e.CommercialNames = append(e.CommercialNames, r.CommercialName)
				}
			}
			entries = append(entries, e)
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Type != entries[j].Type {
			return entries[i].Type < entries[j].Type
		}
		return entries[i].Name < entries[j].Name
	})
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(entries); err != nil {
		return "", err
	}
	return b.String(), nil
}

const (
	availabilityBanner      = bannerPrefix + " Availability comes from comparing the component lists of the cgo and standard builds, and from the Redpanda Cloud lists in `internal/plugins/info.csv`. To change it, edit the build constraints or info.csv and run `task docs` in that repository."
	emptyAvailabilityBanner = bannerPrefix + " The component has no availability notes, so this partial is empty."

	cgoOnlyNote = `ifndef::env-cloud[]
[NOTE]
====
This component is available only in builds of Redpanda Connect that enable cgo. The standard release binaries, the Docker images, and the standard ` + "`rpk connect`" + ` plugin don't include it. To use it, do one of the following:

* Download the ` + "`redpanda-connect-cgo_<version>_linux_amd64`" + ` archive from the https://github.com/redpanda-data/connect/releases[GitHub releases page^]. This archive is available for Linux AMD64 only.
* Build Redpanda Connect from source with ` + "`CGO_ENABLED=1 go build -tags x_benthos_extra,timetzdata ./cmd/redpanda-connect`" + `. The build needs a C compiler.

The cgo archive and the source build both need the ZeroMQ library (` + "`libzmq`" + `), because they include the ` + "`zmq4`" + ` components.
====
endif::[]
`
	gpuPipelinesLink = "xref:develop:connect/configuration/resource-management.adoc[GPU-enabled pipelines]"
	noGPUNote        = "ifdef::env-cloud[]\nNOTE: This component isn't available in " + gpuPipelinesLink + ".\nendif::[]\n"
	gpuOnlyNote      = "ifdef::env-cloud[]\nNOTE: This component is available only in " + gpuPipelinesLink + ".\nendif::[]\n"
)

// renderAvailabilityPartial renders partials/availability/<type>/<name>.adoc.
// It notes when only cgo builds include the component, and, for Redpanda
// Cloud, when only GPU-enabled pipelines or only other pipelines can use it.
// With nothing to note it renders a banner only, so pages can always include
// it.
func renderAvailabilityPartial(p componentPlatform) string {
	var notes []string
	if p.CgoOnly {
		notes = append(notes, cgoOnlyNote)
	}
	switch {
	case p.Cloud && !p.CloudAI:
		notes = append(notes, noGPUNote)
	case !p.Cloud && p.CloudAI:
		notes = append(notes, gpuOnlyNote)
	}
	if len(notes) == 0 {
		return emptyAvailabilityBanner + "\n"
	}
	return availabilityBanner + "\n\n" + strings.Join(notes, "\n")
}

// renderCgoOnlyList renders partials/availability/cgo_only.adoc, a list of
// links to the cgo-only components for the install pages.
func renderCgoOnlyList(full *fullSchema, plat platformSet) string {
	var items []string
	for _, g := range full.Groups {
		typeDir := pageTypeDir(g.Key)
		for _, c := range g.Components {
			if c.Name != "" && plat.get(g.Key, c.Name).CgoOnly {
				items = append(items, "* xref:components:"+typeDir+"/"+c.Name+".adoc[`"+c.Name+"` "+strings.ReplaceAll(string(pluginTypes[g.Key]), "_", " ")+"]")
			}
		}
	}
	if len(items) == 0 {
		return bannerPrefix + " No component is cgo-only, so this partial is empty.\n"
	}
	sort.Strings(items)
	return availabilityBanner + "\n\n" + strings.Join(items, "\n") + "\n"
}

// hasInit reports whether f declares an init function.
func hasInit(f *ast.File) bool {
	for _, d := range f.Decls {
		if fn, ok := d.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == "init" {
			return true
		}
	}
	return false
}
