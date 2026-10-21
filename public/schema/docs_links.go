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

package schema

import (
	"sync"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/plugins"
)

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

// DocsHasComponent returns the function that tells the Markdown renderer
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
func DocsHasComponent(env *service.Environment) func(typeDir, name string) bool {
	var (
		once  sync.Once
		ships map[string]bool
	)
	return func(typeDir, name string) bool {
		t, ok := componentTypes[typeDir]
		if !ok {
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
