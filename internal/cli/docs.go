package cli

import (
	"os"

	"github.com/urfave/cli/v2"

	"bytes"
	"fmt"
	"path"
	"path/filepath"

	"github.com/benthosdev/benthos/v4/internal/api"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/config/test"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/template"
)

func docsCliCommand() *cli.Command {
	return &cli.Command{
		Name:  "docs",
		Usage: "Generates markdown documentation for Benthos components",
		Description: `
  benthos docs`[1:],
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "dir",
				Aliases: []string{"d"},
				Usage:   "The directory to write documentation to",
				Value:   "./website/docs/components",
			},
		},
		Action: func(c *cli.Context) error {
			generateComponentDocs(c)
			os.Exit(0)
			return nil
		},
	}
}

func generateComponentDocs(c *cli.Context) {
	docsDir := c.String("dir")

	for _, v := range bundle.AllInputs.Docs() {
		generate(path.Join(docsDir, "./inputs"), v)
	}

	for _, v := range bundle.AllBuffers.Docs() {
		generate(path.Join(docsDir, "./buffers"), v)
	}

	for _, v := range bundle.AllCaches.Docs() {
		generate(path.Join(docsDir, "./caches"), v)
	}

	for _, v := range bundle.AllMetrics.Docs() {
		generate(path.Join(docsDir, "./metrics"), v)
	}

	for _, v := range bundle.AllOutputs.Docs() {
		generate(path.Join(docsDir, "./outputs"), v)
	}

	for _, v := range bundle.AllProcessors.Docs() {
		generate(path.Join(docsDir, "./processors"), v)
	}

	for _, v := range bundle.AllRateLimits.Docs() {
		generate(path.Join(docsDir, "./rate_limits"), v)
	}

	for _, v := range bundle.AllTracers.Docs() {
		generate(path.Join(docsDir, "./tracers"), v)
	}

	for _, v := range bundle.AllScanners.Docs() {
		generate(path.Join(docsDir, "./scanners"), v)
	}

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

func generate(docsDir string, config docs.ComponentSpec) {
	_, rootOnly := map[string]struct{}{
		"cache":      {},
		"rate_limit": {},
		"processor":  {},
		"scanner":    {},
	}[string(config.Type)]

	conf := map[string]any{
		"type": config.Name,
	}
	for k, v := range docs.ReservedFieldsByType(config.Type) {
		if k == "plugin" {
			continue
		}
		if v.Default != nil {
			conf[k] = *v.Default
		}
	}

	mdSpec, err := config.AsMarkdown(bundle.GlobalEnvironment, !rootOnly, conf)

	if err != nil {
		panic(fmt.Sprintf("Failed to generate docs for '%v': %v", config.Name, err))
	}

	mkDirAll(docsDir)

	create(config.Name, path.Join(docsDir, config.Name+".md"), mdSpec)
}

func doBloblang(dir string) {
	mdSpec, err := docs.BloblangFunctionsMarkdown()
	if err != nil {
		panic(fmt.Sprintf("Failed to generate docs for bloblang functions: %v", err))
	}

	bloblangDir := filepath.Join(dir, "..", "guides", "bloblang")

	mkDirAll(bloblangDir)

	create("bloblang functions", filepath.Join(bloblangDir, "functions.md"), mdSpec)

	if mdSpec, err = docs.BloblangMethodsMarkdown(); err != nil {
		panic(fmt.Sprintf("Failed to generate docs for bloblang methods: %v", err))
	}

	create("bloblang methods", filepath.Join(bloblangDir, "methods.md"), mdSpec)
}

func doTestDocs(dir string) {
	mdSpec, err := test.DocsMarkdown()
	if err != nil {
		panic(fmt.Sprintf("Failed to generate docs for unit tests: %v", err))
	}

	testDir := filepath.Join(dir, "..", "configuration")

	mkDirAll(testDir)

	create("test docs", filepath.Join(testDir, "unit_testing.md"), mdSpec)
}

func doHTTP(dir string) {
	mdSpec, err := api.DocsMarkdown()
	if err != nil {
		panic(fmt.Sprintf("Failed to generate docs for http: %v", err))
	}

	httpDir := filepath.Join(dir, "http")

	mkDirAll(httpDir)

	create("http docs", filepath.Join(httpDir, "about.md"), mdSpec)
}

func doLogger(dir string) {
	mdSpec, err := log.DocsMarkdown()
	if err != nil {
		panic(fmt.Sprintf("Failed to generate docs for logger: %v", err))
	}

	loggerDir := filepath.Join(dir, "logger")

	mkDirAll(loggerDir)

	create("logger docs", filepath.Join(loggerDir, "about.md"), mdSpec)
}

func doTemplates(dir string) {
	mdSpec, err := template.DocsMarkdown()
	if err != nil {
		panic(fmt.Sprintf("Failed to generate docs for templates: %v", err))
	}

	templateDir := filepath.Join(dir, "..", "configuration")

	mkDirAll(templateDir)

	create("template docs", filepath.Join(templateDir, "templating.md"), mdSpec)
}

func mkDirAll(dir string) {
	if _, err := os.Stat(dir); err == nil || !os.IsNotExist(err) {
		return
	}

	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		panic(err)
	}
}
