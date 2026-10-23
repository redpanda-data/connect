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

// Package docslinks holds where each Redpanda Connect distribution's docs live
// and how doc string links resolve when the doc strings render as Markdown.
// It has no component imports, so every binary, including the Apache 2.0
// community build, can use it without linking enterprise code.
package docslinks

import (
	"slices"
	"sync"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/plugins"
)

// Where each distribution's documentation lives. The CLI prints the URL as
// its documentation link and resolves doc string links against it when it
// renders them as Markdown.
const (
	DocumentationURL      = "https://docs.redpanda.com/connect"
	CloudDocumentationURL = "https://docs.redpanda.com/cloud-data-platform/develop/connect"
)

// ComponentURLs maps the other docs site components that doc strings link
// to with component-qualified xrefs, such as xref:streaming:manage:x.adoc[],
// to their base URLs.
var ComponentURLs = map[string]string{
	// Version-less streaming URLs redirect to the current version; link there
	// directly.
	"streaming":           "https://docs.redpanda.com/streaming/current",
	"cloud-data-platform": "https://docs.redpanda.com/cloud-data-platform",
	"connect":             DocumentationURL,
}

// MarkdownOptions returns the link settings for the doc strings of the
// distribution with schema s. A schema that declares env-cloud links to the
// Cloud copy of the Connect docs, because the Cloud docs site sets the same
// attribute. A new attribute or docs site needs a new case here.
func MarkdownOptions(s *service.ConfigSchema) service.DocsMarkdownOptions {
	url := DocumentationURL
	if slices.Contains(s.DocsAttributes(), service.DocsAttributeCloud) {
		url = CloudDocumentationURL
	}
	return service.DocsMarkdownOptions{
		DocumentationURL: url,
		ComponentURLs:    ComponentURLs,
		HasComponent:     HasComponent(s.Environment()),
	}
}

// CLIOpts returns the CLI options that set MarkdownOptions(s) for the binary
// whose main schema is s: the documentation link in its help and the links in
// its doc strings rendered as Markdown. Options passed after these override
// them, for example to point at a staging docs site.
func CLIOpts(s *service.ConfigSchema) []service.CLIOptFunc {
	o := MarkdownOptions(s)
	return []service.CLIOptFunc{
		service.CLIOptSetDocumentationURL(o.DocumentationURL),
		service.CLIOptSetDocsComponentURLs(o.ComponentURLs),
		service.CLIOptSetDocsHasComponent(o.HasComponent),
	}
}

// componentTypes maps the docs directory of each component family, as it
// appears in xref:components:<dir>/<name>.adoc, to its plugin type.
var componentTypes = map[string]plugins.TypeName{
	"buffers":     plugins.TypeBuffer,
	"caches":      plugins.TypeCache,
	"inputs":      plugins.TypeInput,
	"metrics":     plugins.TypeMetric,
	"outputs":     plugins.TypeOutput,
	"processors":  plugins.TypeProcessor,
	"rate_limits": plugins.TypeRateLimit,
	"scanners":    plugins.TypeScanner,
	"tracers":     plugins.TypeTracer,
}

// HasComponent returns the function that tells the Markdown renderer
// whether the distribution whose components are registered in env ships the
// component an xref points at, for service.DocsMarkdownOptions.HasComponent and
// service.CLIOptSetDocsHasComponent. A link to a component the distribution
// lacks renders as text. A target under components/<dir>/ that is not a
// Connect component at all, such as a migration guide page, is a docs page
// every distribution has, so its link is kept.
//
// The components env ships are read the first time the function is called, so
// components removed from env after this call, for example by a connector
// list, are already gone by then.
func HasComponent(env *service.Environment) func(typeDir, name string) bool {
	var (
		once  sync.Once
		ships map[string]bool
	)
	return func(typeDir, name string) bool {
		t, exists := componentTypes[typeDir]
		if !exists {
			return true
		}
		if !plugins.BaseInfo.Has(name, t) {
			return true
		}
		once.Do(func() { ships = shippedComponents(env) })
		return ships[typeDir+"/"+name]
	}
}

func shippedComponents(env *service.Environment) map[string]bool {
	ships := map[string]bool{}
	add := func(dir string) func(string, *service.ConfigView) {
		return func(name string, _ *service.ConfigView) { ships[dir+"/"+name] = true }
	}
	env.WalkBuffers(add("buffers"))
	env.WalkCaches(add("caches"))
	env.WalkInputs(add("inputs"))
	env.WalkMetrics(add("metrics"))
	env.WalkOutputs(add("outputs"))
	env.WalkProcessors(add("processors"))
	env.WalkRateLimits(add("rate_limits"))
	env.WalkScanners(add("scanners"))
	env.WalkTracers(add("tracers"))
	return ships
}
