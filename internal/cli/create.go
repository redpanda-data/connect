package cli

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/config"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/stream"
)

func addExpression(conf map[string]any, expression string) error {
	var inputTypes, processorTypes, outputTypes []string
	componentTypes := strings.Split(expression, "/")
	for i, str := range componentTypes {
		for _, t := range strings.Split(str, ",") {
			if t = strings.TrimSpace(t); t != "" {
				switch i {
				case 0:
					inputTypes = append(inputTypes, t)
				case 1:
					processorTypes = append(processorTypes, t)
				case 2:
					outputTypes = append(outputTypes, t)
				default:
					return errors.New("more component separators than expected")
				}
			}
		}
	}

	if lInputs := len(inputTypes); lInputs == 1 {
		t := inputTypes[0]
		if _, exists := bundle.AllInputs.DocsFor(t); exists {
			conf["input"] = map[string]any{
				"type": t,
			}
		} else {
			return fmt.Errorf("unrecognised input type '%v'", t)
		}
	} else if lInputs > 1 {
		var inputsList []any
		for _, t := range inputTypes {
			if _, exists := bundle.AllInputs.DocsFor(t); exists {
				inputsList = append(inputsList, map[string]any{
					"type": t,
				})
			} else {
				return fmt.Errorf("unrecognised input type '%v'", t)
			}
		}
		conf["input"] = map[string]any{
			"broker": map[string]any{
				"inputs": inputsList,
			},
		}
	}

	if len(processorTypes) > 0 {
		var procsList []any
		for _, t := range processorTypes {
			if _, exists := bundle.AllProcessors.DocsFor(t); exists {
				procsList = append(procsList, map[string]any{
					"type": t,
				})
			} else {
				return fmt.Errorf("unrecognised processor type '%v'", t)
			}
		}
		conf["pipeline"] = map[string]any{
			"processors": procsList,
		}
	}

	if lOutputs := len(outputTypes); lOutputs == 1 {
		t := outputTypes[0]
		if _, exists := bundle.AllOutputs.DocsFor(t); exists {
			conf["output"] = map[string]any{
				"type": t,
			}
		} else {
			return fmt.Errorf("unrecognised output type '%v'", t)
		}
	} else if lOutputs > 1 {
		var outputsList []any
		for _, t := range outputTypes {
			if _, exists := bundle.AllOutputs.DocsFor(t); exists {
				outputsList = append(outputsList, map[string]any{
					"type": t,
				})
			} else {
				return fmt.Errorf("unrecognised output type '%v'", t)
			}
		}

		conf["output"] = map[string]any{
			"broker": map[string]any{
				"outputs": outputsList,
			},
		}
	}
	return nil
}

func createCliCommand() *cli.Command {
	return &cli.Command{
		Name:  "create",
		Usage: "Create a new Benthos config",
		Description: `
Prints a new Benthos config to stdout containing specified components
according to an expression. The expression must take the form of three
comma-separated lists of inputs, processors and outputs, divided by
forward slashes:

  benthos create stdin/bloblang,awk/nats
  benthos create file,http_server/protobuf/http_client

If the expression is omitted a default config is created.`[1:],
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:    "small",
				Aliases: []string{"s"},
				Value:   false,
				Usage:   "Print only the main components of a Benthos config (input, pipeline, output) and omit all fields marked as advanced.",
			},
		},
		Action: func(c *cli.Context) error {
			conf := map[string]any{
				"input": map[string]any{
					"stdin": map[string]any{},
				},
				"pipeline": map[string]any{
					"processors": []any{},
				},
				"output": map[string]any{
					"stdout": map[string]any{},
				},
			}
			if expression := c.Args().First(); expression != "" {
				if err := addExpression(conf, expression); err != nil {
					fmt.Fprintf(os.Stderr, "Generate error: %v\n", err)
					os.Exit(1)
				}
			}

			spec := config.Spec()
			var filter docs.FieldFilter
			if c.Bool("small") {
				spec = stream.Spec()
				filter = func(spec docs.FieldSpec, _ any) bool {
					return !spec.IsAdvanced
				}
			}

			conf, err := spec.AnyToMap(conf, docs.ToValueConfig{
				FallbackToAny: true,
			})
			if err != nil {
				fmt.Fprintf(os.Stderr, "Generate error: %v\n", err)
				os.Exit(1)
			}

			var node yaml.Node
			if err = node.Encode(conf); err == nil {
				sanitConf := docs.NewSanitiseConfig(bundle.GlobalEnvironment)
				sanitConf.RemoveTypeField = true
				sanitConf.RemoveDeprecated = true
				sanitConf.ForExample = true
				sanitConf.Filter = filter

				err = spec.SanitiseYAML(&node, sanitConf)
			}
			if err == nil {
				var configYAML []byte
				if configYAML, err = docs.MarshalYAML(node); err == nil {
					fmt.Println(string(configYAML))
				}
			}
			if err != nil {
				fmt.Fprintf(os.Stderr, "Generate error: %v\n", err)
				os.Exit(1)
			}
			return nil
		},
	}
}
