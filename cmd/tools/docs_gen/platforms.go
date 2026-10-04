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
	"os"
	"os/exec"
	"sort"
	"strings"

	"github.com/redpanda-data/connect/v4/internal/plugins"
	"github.com/redpanda-data/connect/v4/public/schema"
)

// componentListPkg prints the standard schema of whatever build runs it. It
// imports the same components as docs_gen, so the only difference between
// its output and the docs_gen schema comes from build constraints.
const componentListPkg = "github.com/redpanda-data/connect/v4/cmd/tools/docs_gen/componentlist"

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

// standardBuildSchema compiles and runs componentlist without cgo or build
// tags, and returns the schema it prints.
func standardBuildSchema() (*fullSchema, error) {
	cmd := exec.Command("go", "run", componentListPkg)
	cmd.Env = standardBuildEnv(os.Environ())
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("running `CGO_ENABLED=0 go run %v`: %w\n%s", componentListPkg, err, stderr.String())
	}
	return parseFullSchema(out)
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
	group := typeDir
	if typeDir == "rate_limits" {
		group = "rate-limits"
	}
	k := componentKey(group, name)
	return p.documented[k] && !p.byKey[k].inCloud()
}

// loadPlatforms computes the platform set for the docs_gen schema.
func loadPlatforms(full *fullSchema) (platformSet, error) {
	standard, err := standardBuildSchema()
	if err != nil {
		return platformSet{}, err
	}
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

// pluginTypes maps a schema key to the type name that internal/plugins/info.csv
// uses.
var pluginTypes = map[string]plugins.TypeName{
	"buffers": plugins.TypeBuffer, "caches": plugins.TypeCache, "inputs": plugins.TypeInput,
	"outputs": plugins.TypeOutput, "processors": plugins.TypeProcessor, "rate-limits": plugins.TypeRateLimit,
	"metrics": plugins.TypeMetric, "tracers": plugins.TypeTracer, "scanners": plugins.TypeScanner,
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
