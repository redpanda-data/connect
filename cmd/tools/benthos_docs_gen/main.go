package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"path"
	"path/filepath"

	"github.com/Jeffail/benthos/v3/internal/docs"
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
)

//------------------------------------------------------------------------------

func create(t, path string, resBytes []byte) {
	if existing, err := ioutil.ReadFile(path); err == nil {
		if bytes.Equal(existing, resBytes) {
			fmt.Printf("Skipping '%v' at: %v\n", t, path)
			return
		}
	}
	if err := ioutil.WriteFile(path, resBytes, 0644); err != nil {
		panic(err)
	}
	fmt.Printf("Generated '%v' doc at: %v\n", t, path)
}

func render(dir string, embed bool, confSanit interface{}, spec docs.ComponentSpec) {
	var segment interface{}
	switch t := confSanit.(type) {
	case map[string]interface{}:
		segment = t[spec.Name]
	case config.Sanitised:
		segment = t[spec.Name]
	default:
		panic(fmt.Sprintf("Failed to generate docs for '%v': sanitised config wrong type: %T", spec.Name, confSanit))
	}

	mdSpec, err := spec.AsMarkdown(embed, segment)
	if err != nil {
		panic(fmt.Sprintf("Failed to generate docs for '%v': %v", spec.Name, err))
	}

	create(spec.Name, dir, mdSpec)
}

func main() {
	docsDir := "./website/docs/components"
	flag.StringVar(&docsDir, "dir", docsDir, "The directory to write docs to")
	flag.Parse()

	doInputs(docsDir)
	doBuffers(docsDir)
	doCaches(docsDir)
	doConditions(docsDir)
	doMetrics(docsDir)
	doOutputs(docsDir)
	doProcessors(docsDir)
	doRateLimits(docsDir)
	doTracers(docsDir)

	// Bloblang stuff
	doBloblang(docsDir)
}

func doInputs(docsDir string) {
	for k, v := range input.Constructors {
		spec := docs.ComponentSpec{
			Type:        "input",
			Name:        k,
			Summary:     v.Summary,
			Description: v.Description,
			Footnotes:   v.Footnotes,
			Examples:    v.Examples,
			Fields:      v.FieldSpecs,
			Beta:        v.Beta,
			Deprecated:  v.Deprecated,
		}
		if len(v.Categories) > 0 {
			spec.Categories = make([]string, 0, len(v.Categories))
			for _, cat := range v.Categories {
				spec.Categories = append(spec.Categories, string(cat))
			}
		}

		conf := input.NewConfig()
		conf.Type = k
		confSanit, err := input.SanitiseConfig(conf)
		if err != nil {
			panic(fmt.Sprintf("Failed to generate docs for '%v': %v", k, err))
		}

		render(path.Join(docsDir, "./inputs", k+".md"), true, confSanit, spec)
	}
}

func doBuffers(docsDir string) {
	for k, v := range buffer.Constructors {
		spec := docs.ComponentSpec{
			Type:        "buffer",
			Name:        k,
			Summary:     v.Summary,
			Description: v.Description,
			Footnotes:   v.Footnotes,
			Fields:      v.FieldSpecs,
			Beta:        v.Beta,
			Deprecated:  v.Deprecated,
		}

		conf := buffer.NewConfig()
		conf.Type = k
		confSanit, err := buffer.SanitiseConfig(conf)
		if err != nil {
			panic(fmt.Sprintf("Failed to generate docs for '%v': %v", k, err))
		}

		render(path.Join(docsDir, "./buffers", k+".md"), true, confSanit, spec)
	}
}

func doCaches(docsDir string) {
	for k, v := range cache.Constructors {
		spec := docs.ComponentSpec{
			Type:        "cache",
			Name:        k,
			Summary:     v.Summary,
			Description: v.Description,
			Footnotes:   v.Footnotes,
			Fields:      v.FieldSpecs,
			Beta:        v.Beta,
			Deprecated:  v.Deprecated,
		}

		conf := cache.NewConfig()
		conf.Type = k
		confSanit, err := cache.SanitiseConfig(conf)
		if err != nil {
			panic(fmt.Sprintf("Failed to generate docs for '%v': %v", k, err))
		}

		render(path.Join(docsDir, "./caches", k+".md"), false, confSanit, spec)
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
			Fields:      v.FieldSpecs,
			Beta:        v.Beta,
			Deprecated:  v.Deprecated,
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
	for k, v := range metrics.Constructors {
		spec := docs.ComponentSpec{
			Type:        "metrics",
			Name:        k,
			Summary:     v.Summary,
			Description: v.Description,
			Footnotes:   v.Footnotes,
			Fields:      v.FieldSpecs,
			Beta:        v.Beta,
			Deprecated:  v.Deprecated,
		}

		conf := metrics.NewConfig()
		conf.Type = k
		confSanit, err := metrics.SanitiseConfig(conf)
		if err != nil {
			panic(fmt.Sprintf("Failed to generate docs for '%v': %v", k, err))
		}

		render(path.Join(docsDir, "./metrics", k+".md"), true, confSanit, spec)
	}
}

func doOutputs(docsDir string) {
	for k, v := range output.Constructors {
		spec := docs.ComponentSpec{
			Type:        "output",
			Name:        k,
			Summary:     v.Summary,
			Description: v.Description,
			Footnotes:   v.Footnotes,
			Fields:      v.FieldSpecs,
			Examples:    v.Examples,
			Beta:        v.Beta,
			Deprecated:  v.Deprecated,
		}
		if len(v.Categories) > 0 {
			spec.Categories = make([]string, 0, len(v.Categories))
			for _, cat := range v.Categories {
				spec.Categories = append(spec.Categories, string(cat))
			}
		}
		if v.Async || v.Batches {
			spec.Description = spec.Description + "\n\n## Performance"
		}
		if v.Async {
			spec.Description = spec.Description + "\n" + output.DocsAsync
		}
		if v.Batches {
			spec.Description = spec.Description + "\n" + output.DocsBatches
		}

		conf := output.NewConfig()
		conf.Type = k
		confSanit, err := output.SanitiseConfig(conf)
		if err != nil {
			panic(fmt.Sprintf("Failed to generate docs for '%v': %v", k, err))
		}

		render(path.Join(docsDir, "./outputs", k+".md"), true, confSanit, spec)
	}
}

func doProcessors(docsDir string) {
	for k, v := range processor.Constructors {
		spec := docs.ComponentSpec{
			Type:        "processor",
			Name:        k,
			Summary:     v.Summary,
			Description: v.Description,
			Examples:    v.Examples,
			Footnotes:   v.Footnotes,
			Fields:      v.FieldSpecs,
			Beta:        v.Beta,
			Deprecated:  v.Deprecated,
		}
		if len(v.Categories) > 0 {
			spec.Categories = make([]string, 0, len(v.Categories))
			for _, cat := range v.Categories {
				spec.Categories = append(spec.Categories, string(cat))
			}
		}
		if v.UsesBatches {
			spec.Description = spec.Description + "\n" + processor.DocsUsesBatches
		}

		conf := processor.NewConfig()
		conf.Type = k
		confSanit, err := processor.SanitiseConfig(conf)
		if err != nil {
			panic(fmt.Sprintf("Failed to generate docs for '%v': %v", k, err))
		}

		render(path.Join(docsDir, "./processors", k+".md"), false, confSanit, spec)
	}
}

func doRateLimits(docsDir string) {
	for k, v := range ratelimit.Constructors {
		spec := docs.ComponentSpec{
			Type:        "rate_limit",
			Name:        k,
			Summary:     v.Summary,
			Description: v.Description,
			Footnotes:   v.Footnotes,
			Fields:      v.FieldSpecs,
			Beta:        v.Beta,
			Deprecated:  v.Deprecated,
		}

		conf := ratelimit.NewConfig()
		conf.Type = k
		confSanit, err := ratelimit.SanitiseConfig(conf)
		if err != nil {
			panic(fmt.Sprintf("Failed to generate docs for '%v': %v", k, err))
		}

		render(path.Join(docsDir, "./rate_limits", k+".md"), false, confSanit, spec)
	}
}

func doTracers(docsDir string) {
	for k, v := range tracer.Constructors {
		spec := docs.ComponentSpec{
			Type:        "tracer",
			Name:        k,
			Summary:     v.Summary,
			Description: v.Description,
			Footnotes:   v.Footnotes,
			Fields:      v.FieldSpecs,
			Beta:        v.Beta,
			Deprecated:  v.Deprecated,
		}

		conf := tracer.NewConfig()
		conf.Type = k
		confSanit, err := tracer.SanitiseConfig(conf)
		if err != nil {
			panic(fmt.Sprintf("Failed to generate docs for '%v': %v", k, err))
		}

		render(path.Join(docsDir, "./tracers", k+".md"), true, confSanit, spec)
	}
}

func doBloblang(dir string) {
	mdSpec, err := docs.BloblangFunctionsMarkdown()
	if err != nil {
		panic(fmt.Sprintf("Failed to generate docs for bloblang functions: %v", err))
	}

	create("bloblang functions", filepath.Join(dir, "../guides/bloblang/functions.md"), mdSpec)

	if mdSpec, err = docs.BloblangMethodsMarkdown(); err != nil {
		panic(fmt.Sprintf("Failed to generate docs for bloblang methods: %v", err))
	}

	create("bloblang methods", filepath.Join(dir, "../guides/bloblang/methods.md"), mdSpec)
}

//------------------------------------------------------------------------------
