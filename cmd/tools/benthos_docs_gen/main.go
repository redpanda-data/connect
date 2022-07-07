package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/benthosdev/benthos/v4/internal/api"
	tdocs "github.com/benthosdev/benthos/v4/internal/cli/test/docs"
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
	docsDir := "./website/docs/components"
	flag.StringVar(&docsDir, "dir", docsDir, "The directory to write docs to")
	flag.Parse()

	if err := template.InitNativeTemplates(); err != nil {
		panic(err)
	}

	service.GlobalEnvironment().WalkInputs(viewForDir(path.Join(docsDir, "./inputs")))
	service.GlobalEnvironment().WalkBuffers(viewForDir(path.Join(docsDir, "./buffers")))
	service.GlobalEnvironment().WalkCaches(viewForDir(path.Join(docsDir, "./caches")))
	service.GlobalEnvironment().WalkMetrics(viewForDir(path.Join(docsDir, "./metrics")))
	service.GlobalEnvironment().WalkOutputs(viewForDir(path.Join(docsDir, "./outputs")))
	service.GlobalEnvironment().WalkProcessors(viewForDir(path.Join(docsDir, "./processors")))
	service.GlobalEnvironment().WalkRateLimits(viewForDir(path.Join(docsDir, "./rate_limits")))
	service.GlobalEnvironment().WalkTracers(viewForDir(path.Join(docsDir, "./tracers")))

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
		mdSpec, err := config.RenderDocs()
		if err != nil {
			panic(fmt.Sprintf("Failed to generate docs for '%v': %v", name, err))
		}
		create(name, path.Join(docsDir, name+".md"), mdSpec)
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

func doHTTP(dir string) {
	mdSpec, err := api.DocsMarkdown()
	if err != nil {
		panic(fmt.Sprintf("Failed to generate docs for http: %v", err))
	}

	create("http docs", filepath.Join(dir, "http", "about.md"), mdSpec)
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
