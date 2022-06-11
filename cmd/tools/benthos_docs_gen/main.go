package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	tdocs "github.com/benthosdev/benthos/v4/internal/cli/test/docs"
	"github.com/benthosdev/benthos/v4/internal/component/buffer"
	"github.com/benthosdev/benthos/v4/internal/component/cache"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/component/ratelimit"
	"github.com/benthosdev/benthos/v4/internal/component/tracer"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/template"

	_ "github.com/benthosdev/benthos/v4/public/components/all"
)

func create(t, path string, resBytes []byte) {
	if existing, err := os.ReadFile(path); err == nil {
		if bytes.Equal(existing, resBytes) {
			return
		}
	}
	if err := os.WriteFile(path, resBytes, 0o644); err != nil {
		panic(err)
	}
	fmt.Printf("Documentation for '%v' has changed, updating: %v\n", t, path)
}

func render(dir string, embed bool, conf interface{}, spec docs.ComponentSpec) {
	mdSpec, err := spec.AsMarkdown(embed, conf)
	if err != nil {
		panic(fmt.Sprintf("Failed to generate docs for '%v': %v", spec.Name, err))
	}

	create(spec.Name, dir, mdSpec)
}

func main() {
	docsDir := "./website/docs/components"
	flag.StringVar(&docsDir, "dir", docsDir, "The directory to write docs to")
	flag.Parse()

	if err := template.InitNativeTemplates(); err != nil {
		panic(err)
	}

	doInputs(docsDir)
	doBuffers(docsDir)
	doCaches(docsDir)
	// Note, disabling condition docs generation now as a convenience, but we
	// can add it back in if there are automated changes required.
	// TODO: V4 Delete entirely
	// doConditions(docsDir)
	doMetrics(docsDir)
	doOutputs(docsDir)
	doProcessors(docsDir)
	doRateLimits(docsDir)
	doTracers(docsDir)

	// Bloblang stuff
	doBloblang(docsDir)

	// Unit test docs
	doTestDocs(docsDir)

	// Logger docs
	doLogger(docsDir)

	// Template docs
	doTemplates(docsDir)
}

func doInputs(docsDir string) {
	for _, v := range bundle.AllInputs.Docs() {
		conf := input.NewConfig()
		conf.Type = v.Name
		render(path.Join(docsDir, "./inputs", v.Name+".md"), true, conf, v)
	}
}

func doBuffers(docsDir string) {
	for _, v := range bundle.AllBuffers.Docs() {
		conf := buffer.NewConfig()
		conf.Type = v.Name
		render(path.Join(docsDir, "./buffers", v.Name+".md"), true, conf, v)
	}
}

func doCaches(docsDir string) {
	for _, v := range bundle.AllCaches.Docs() {
		conf := cache.NewConfig()
		conf.Type = v.Name
		render(path.Join(docsDir, "./caches", v.Name+".md"), false, conf, v)
	}
}

func doMetrics(docsDir string) {
	for _, v := range bundle.AllMetrics.Docs() {
		conf := metrics.NewConfig()
		conf.Type = v.Name
		render(path.Join(docsDir, "./metrics", v.Name+".md"), true, conf, v)
	}
}

func doOutputs(docsDir string) {
	for _, v := range bundle.AllOutputs.Docs() {
		conf := output.NewConfig()
		conf.Type = v.Name
		render(path.Join(docsDir, "./outputs", v.Name+".md"), true, conf, v)
	}
}

func doProcessors(docsDir string) {
	for _, v := range bundle.AllProcessors.Docs() {
		conf := processor.NewConfig()
		conf.Type = v.Name
		render(path.Join(docsDir, "./processors", v.Name+".md"), false, conf, v)
	}
}

func doRateLimits(docsDir string) {
	for _, v := range bundle.AllRateLimits.Docs() {
		conf := ratelimit.NewConfig()
		conf.Type = v.Name
		render(path.Join(docsDir, "./rate_limits", v.Name+".md"), false, conf, v)
	}
}

func doTracers(docsDir string) {
	for _, v := range bundle.AllTracers.Docs() {
		conf := tracer.NewConfig()
		conf.Type = v.Name
		render(path.Join(docsDir, "./tracers", v.Name+".md"), true, conf, v)
	}
}

func doBloblang(dir string) {
	mdSpec, err := docs.BloblangFunctionsMarkdown()
	if err != nil {
		panic(fmt.Sprintf("Failed to generate docs for bloblang functions: %v", err))
	}

	create("bloblang functions", filepath.Join(dir, "..", "guides", "bloblang", "functions.md"), mdSpec)

	if mdSpec, err = docs.BloblangMethodsMarkdown(); err != nil {
		panic(fmt.Sprintf("Failed to generate docs for bloblang methods: %v", err))
	}

	create("bloblang methods", filepath.Join(dir, "..", "guides", "bloblang", "methods.md"), mdSpec)
}

func doTestDocs(dir string) {
	mdSpec, err := tdocs.DocsMarkdown()
	if err != nil {
		panic(fmt.Sprintf("Failed to generate docs for unit tests: %v", err))
	}

	create("test docs", filepath.Join(dir, "..", "configuration", "unit_testing.md"), mdSpec)
}

func doLogger(dir string) {
	mdSpec, err := log.DocsMarkdown()
	if err != nil {
		panic(fmt.Sprintf("Failed to generate docs for logger: %v", err))
	}

	create("logger docs", filepath.Join(dir, "logger", "about.md"), mdSpec)
}

func doTemplates(dir string) {
	mdSpec, err := template.DocsMarkdown()
	if err != nil {
		panic(fmt.Sprintf("Failed to generate docs for templates: %v", err))
	}

	create("template docs", filepath.Join(dir, "..", "configuration", "templating.md"), mdSpec)
}
