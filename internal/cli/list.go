package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/urfave/cli/v2"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"github.com/benthosdev/benthos/v4/internal/cli/common"
	"github.com/benthosdev/benthos/v4/internal/config/schema"
	"github.com/benthosdev/benthos/v4/internal/cuegen"
)

func listCliCommand(opts *common.CLIOpts) *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: opts.ExecTemplate("List all {{.ProductName}} component types"),
		Description: opts.ExecTemplate(`
If any component types are explicitly listed then only types of those
components will be shown.

  {{.BinaryName}} list
  {{.BinaryName}} list --format json inputs output
  {{.BinaryName}} list rate-limits buffers`)[1:],
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "format",
				Value: "text",
				Usage: "Print the component list in a specific format. Options are text, json or cue.",
			},
			&cli.StringFlag{
				Name:  "status",
				Value: "",
				Usage: "Filter the component list to only those matching the given status. Options are stable, beta or experimental.",
			},
		},
		Action: func(c *cli.Context) error {
			listComponents(c, opts)
			os.Exit(0)
			return nil
		},
	}
}

func listComponents(c *cli.Context, opts *common.CLIOpts) {
	ofTypes := map[string]struct{}{}
	for _, k := range c.Args().Slice() {
		ofTypes[k] = struct{}{}
	}

	schema := schema.New(opts.Version, opts.DateBuilt)
	if status := c.String("status"); status != "" {
		schema.ReduceToStatus(status)
	}
	schema.Config = opts.MainConfigSpecCtor()

	switch c.String("format") {
	case "text":
		flat := schema.Flattened()
		i := 0
		for _, k := range []string{
			"inputs",
			"processors",
			"outputs",
			"caches",
			"rate-limits",
			"buffers",
			"metrics",
			"tracers",
			"scanners",
			"bloblang-functions",
			"bloblang-methods",
		} {
			if _, exists := ofTypes[k]; len(ofTypes) > 0 && !exists {
				continue
			}
			if i > 0 {
				fmt.Println("")
			}
			i++
			title := cases.Title(language.English).String(strings.ReplaceAll(k, "-", " "))
			fmt.Printf("%v:\n", title)
			for _, t := range flat[k] {
				fmt.Printf("  - %v\n", t)
			}
		}
	case "json":
		flat := schema.Flattened()
		if len(ofTypes) > 0 {
			for k := range flat {
				if _, exists := ofTypes[k]; !exists {
					delete(flat, k)
				}
			}
		}
		jsonBytes, err := json.Marshal(flat)
		if err != nil {
			panic(err)
		}
		fmt.Println(string(jsonBytes))
	case "json-full":
		jsonBytes, err := json.Marshal(schema)
		if err != nil {
			panic(err)
		}
		fmt.Println(string(jsonBytes))
	case "json-full-scrubbed":
		schema.Scrub()
		jsonBytes, err := json.Marshal(schema)
		if err != nil {
			panic(err)
		}
		fmt.Println(string(jsonBytes))
	case "cue":
		source, err := cuegen.GenerateSchema(schema)
		if err != nil {
			panic(err)
		}
		fmt.Println(string(source))
	}
}
