package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/Jeffail/benthos/v3/internal/bundle"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/template"
	"github.com/Jeffail/benthos/v3/lib/buffer"
	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/ratelimit"
	"github.com/Jeffail/benthos/v3/lib/tracer"
	"github.com/Jeffail/benthos/v3/lib/util/config"

	_ "github.com/Jeffail/benthos/v3/public/components/all"
)

//------------------------------------------------------------------------------

var verbose bool

func create(t, path string, resBytes []byte) {
	if existing, err := os.ReadFile(path); err == nil {
		if bytes.Equal(existing, resBytes) {
			if verbose {
				fmt.Printf("Skipping '%v' at: %v\n", t, path)
			}
			return
		}
	}
	if err := os.WriteFile(path, resBytes, 0o644); err != nil {
		panic(err)
	}
	fmt.Printf("Documentation for '%v' has changed, updating: %v\n", t, path)
}

func render(dir string, embed bool, confSanit interface{}, spec docs.ComponentSpec) {
	if s, ok := confSanit.(config.Sanitised); ok {
		confSanit = map[string]interface{}(s)
	}

	mdSpec, err := spec.AsMarkdown(embed, confSanit)
	if err != nil {
		panic(fmt.Sprintf("Failed to generate docs for '%v': %v", spec.Name, err))
	}

	create(spec.Name, dir, mdSpec)
}

func main() {
	docsDir := "./website/docs/components"
	flag.StringVar(&docsDir, "dir", docsDir, "The directory to write docs to")
	flag.BoolVar(&verbose, "v", false, "Writes more information to stdout, including configs that aren't updated")
	flag.Parse()

	if _, err := template.InitTemplates(); err != nil {
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

	// Template docs
	doTemplates(docsDir)
}

func doInputs(docsDir string) {
	for _, v := range bundle.AllInputs.Docs() {
		conf := input.NewConfig()
		conf.Type = v.Name
		confSanit, err := input.SanitiseConfig(conf)
		if err != nil {
			panic(fmt.Sprintf("Failed to generate docs for '%v': %v", v.Name, err))
		}
		render(path.Join(docsDir, "./inputs", v.Name+".md"), true, confSanit, v)
	}
}

func doBuffers(docsDir string) {
	for _, v := range bundle.AllBuffers.Docs() {
		conf := buffer.NewConfig()
		conf.Type = v.Name
		confSanit, err := buffer.SanitiseConfig(conf)
		if err != nil {
			panic(fmt.Sprintf("Failed to generate docs for '%v': %v", v.Name, err))
		}
		render(path.Join(docsDir, "./buffers", v.Name+".md"), true, confSanit, v)
	}
}

func doCaches(docsDir string) {
	for _, v := range bundle.AllCaches.Docs() {
		conf := cache.NewConfig()
		conf.Type = v.Name
		confSanit, err := cache.SanitiseConfig(conf)
		if err != nil {
			panic(fmt.Sprintf("Failed to generate docs for '%v': %v", v.Name, err))
		}
		render(path.Join(docsDir, "./caches", v.Name+".md"), false, confSanit, v)
	}
}

func doConditions(docsDir string) {
	for k, v := range condition.Constructors {
		spec := docs.ComponentSpec{
			Type:        "condition",
			Name:        k,
			Summary:     v.Summary,
			Description: v.Description,
			Footnotes:   v.Footnotes,
			Config:      docs.FieldComponent().WithChildren(v.FieldSpecs...),
			Status:      v.Status,
		}

		conf := condition.NewConfig()
		conf.Type = k
		confSanit, err := condition.SanitiseConfig(conf)
		if err != nil {
			panic(fmt.Sprintf("Failed to generate docs for '%v': %v", k, err))
		}

		render(path.Join(docsDir, "./conditions", k+".md"), false, confSanit, spec)
	}
}

func doMetrics(docsDir string) {
	for _, v := range bundle.AllMetrics.Docs() {
		conf := metrics.NewConfig()
		conf.Type = v.Name
		confSanit, err := metrics.SanitiseConfig(conf)
		if err != nil {
			panic(fmt.Sprintf("Failed to generate docs for '%v': %v", v.Name, err))
		}
		render(path.Join(docsDir, "./metrics", v.Name+".md"), true, confSanit, v)
	}
}

func doOutputs(docsDir string) {
	for _, v := range bundle.AllOutputs.Docs() {
		conf := output.NewConfig()
		conf.Type = v.Name
		confSanit, err := output.SanitiseConfig(conf)
		if err != nil {
			panic(fmt.Sprintf("Failed to generate docs for '%v': %v", v.Name, err))
		}
		render(path.Join(docsDir, "./outputs", v.Name+".md"), true, confSanit, v)
	}
}

func doProcessors(docsDir string) {
	for _, v := range bundle.AllProcessors.Docs() {
		conf := processor.NewConfig()
		conf.Type = v.Name
		confSanit, err := processor.SanitiseConfig(conf)
		if err != nil {
			panic(fmt.Sprintf("Failed to generate docs for '%v': %v", v.Name, err))
		}
		render(path.Join(docsDir, "./processors", v.Name+".md"), false, confSanit, v)
	}
}

func doRateLimits(docsDir string) {
	for _, v := range bundle.AllRateLimits.Docs() {
		conf := ratelimit.NewConfig()
		conf.Type = v.Name
		confSanit, err := ratelimit.SanitiseConfig(conf)
		if err != nil {
			panic(fmt.Sprintf("Failed to generate docs for '%v': %v", v.Name, err))
		}
		render(path.Join(docsDir, "./rate_limits", v.Name+".md"), false, confSanit, v)
	}
}

func doTracers(docsDir string) {
	for _, v := range bundle.AllTracers.Docs() {
		conf := tracer.NewConfig()
		conf.Type = v.Name
		confSanit, err := tracer.SanitiseConfig(conf)
		if err != nil {
			panic(fmt.Sprintf("Failed to generate docs for '%v': %v", v.Name, err))
		}
		render(path.Join(docsDir, "./tracers", v.Name+".md"), true, confSanit, v)
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

func doTemplates(dir string) {
	mdSpec, err := template.DocsMarkdown()
	if err != nil {
		panic(fmt.Sprintf("Failed to generate docs for templates: %v", err))
	}

	create("template docs", filepath.Join(dir, "..", "configuration", "templating.md"), mdSpec)
}

//------------------------------------------------------------------------------
