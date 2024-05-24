package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/benthosdev/benthos/v4/internal/api"
	"github.com/benthosdev/benthos/v4/internal/config/test"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/template"
	"github.com/benthosdev/benthos/v4/public/service"

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

func main() {
	docsDir := "./docs/modules/components/pages"
	flag.StringVar(&docsDir, "dir", docsDir, "The directory to write docs to")
	flag.Parse()

	service.GlobalEnvironment().WalkInputs(viewForDir(path.Join(docsDir, "./inputs")))
	service.GlobalEnvironment().WalkBuffers(viewForDir(path.Join(docsDir, "./buffers")))
	service.GlobalEnvironment().WalkCaches(viewForDir(path.Join(docsDir, "./caches")))
	service.GlobalEnvironment().WalkMetrics(viewForDir(path.Join(docsDir, "./metrics")))
	service.GlobalEnvironment().WalkOutputs(viewForDir(path.Join(docsDir, "./outputs")))
	service.GlobalEnvironment().WalkProcessors(viewForDir(path.Join(docsDir, "./processors")))
	service.GlobalEnvironment().WalkRateLimits(viewForDir(path.Join(docsDir, "./rate_limits")))
	service.GlobalEnvironment().WalkTracers(viewForDir(path.Join(docsDir, "./tracers")))
	service.GlobalEnvironment().WalkScanners(viewForDir(path.Join(docsDir, "./scanners")))

	// Bloblang stuff
	doBloblang(docsDir)

	// Unit test docs
	doTestDocs(docsDir)

	// HTTP docs
	doHTTP(docsDir)

	// Logger docs
	doLogger(docsDir)

	// Template docs
	doTemplates(docsDir)
}

func viewForDir(docsDir string) func(name string, config *service.ConfigView) {
	return func(name string, config *service.ConfigView) {
		adocSpec, err := config.RenderDocs()
		if err != nil {
			panic(fmt.Sprintf("Failed to generate docs for '%v': %v", name, err))
		}
		create(name, path.Join(docsDir, name+".adoc"), adocSpec)
	}
}

func doBloblang(dir string) {
	adocSpec, err := docs.BloblangFunctionsMarkdown()
	if err != nil {
		panic(fmt.Sprintf("Failed to generate docs for bloblang functions: %v", err))
	}

	create("bloblang functions", filepath.Join(dir, "../..", "guides", "pages", "bloblang", "functions.adoc"), adocSpec)

	if adocSpec, err = docs.BloblangMethodsMarkdown(); err != nil {
		panic(fmt.Sprintf("Failed to generate docs for bloblang methods: %v", err))
	}

	create("bloblang methods", filepath.Join(dir, "../..", "guides", "pages", "bloblang", "methods.adoc"), adocSpec)
}

func doTestDocs(dir string) {
	adocSpec, err := test.DocsMarkdown()
	if err != nil {
		panic(fmt.Sprintf("Failed to generate docs for unit tests: %v", err))
	}

	create("test docs", filepath.Join(dir, "../..", "configuration", "pages", "unit_testing.adoc"), adocSpec)
}

func doHTTP(dir string) {
	adocSpec, err := api.DocsMarkdown()
	if err != nil {
		panic(fmt.Sprintf("Failed to generate docs for http: %v", err))
	}

	create("http docs", filepath.Join(dir, "http", "about.adoc"), adocSpec)
}

func doLogger(dir string) {
	adocSpec, err := log.DocsMarkdown()
	if err != nil {
		panic(fmt.Sprintf("Failed to generate docs for logger: %v", err))
	}

	create("logger docs", filepath.Join(dir, "logger", "about.adoc"), adocSpec)
}

func doTemplates(dir string) {
	adocSpec, err := template.DocsMarkdown()
	if err != nil {
		panic(fmt.Sprintf("Failed to generate docs for templates: %v", err))
	}

	create("template docs", filepath.Join(dir, "../..", "configuration", "pages", "templating.adoc"), adocSpec)
}
