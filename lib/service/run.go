package service

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/Jeffail/benthos/v3/lib/config"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/service/blobl"
	"github.com/Jeffail/benthos/v3/lib/service/test"
	uconfig "github.com/Jeffail/benthos/v3/lib/util/config"
	"github.com/urfave/cli/v2"
)

//------------------------------------------------------------------------------

// Build stamps.
var (
	Version   string
	DateBuilt string
)

//------------------------------------------------------------------------------

// OptSetVersionStamp creates an opt func for setting the version and date built
// stamps that Benthos returns via --version and the /version endpoint. The
// traditional way of setting these values is via the build flags:
// -X github.com/Jeffail/benthos/v3/lib/service.Version=$(VERSION) and
// -X github.com/Jeffail/benthos/v3/lib/service.DateBuilt=$(DATE)
func OptSetVersionStamp(version, dateBuilt string) func() {
	return func() {
		Version = version
		DateBuilt = dateBuilt
	}
}

//------------------------------------------------------------------------------

var customFlags []cli.Flag

// OptAddStringFlag registers a custom CLI flag for the standard Benthos run
// command.
func OptAddStringFlag(name, usage string, aliases []string, value string, destination *string) func() {
	return func() {
		customFlags = append(customFlags, &cli.StringFlag{
			Name:        name,
			Aliases:     aliases,
			Value:       value,
			Usage:       usage,
			Destination: destination,
		})
	}
}

//------------------------------------------------------------------------------

func cmdVersion(version, dateBuild string) {
	fmt.Printf("Version: %v\nDate: %v\n", Version, DateBuilt)
	os.Exit(0)
}

//------------------------------------------------------------------------------

func addExpression(conf *config.Type, expression string) error {
	var inputTypes, processorTypes, outputTypes []string
	componentTypes := strings.Split(expression, "/")
	for i, str := range componentTypes {
		for _, t := range strings.Split(str, ",") {
			if t = strings.TrimSpace(t); len(t) > 0 {
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
		if _, exists := input.Constructors[t]; exists {
			conf.Input.Type = t
		} else {
			return fmt.Errorf("unrecognised input type '%v'", t)
		}
	} else if lInputs > 1 {
		conf.Input.Type = input.TypeBroker
		for _, t := range inputTypes {
			c := input.NewConfig()
			if _, exists := input.Constructors[t]; exists {
				c.Type = t
			} else {
				return fmt.Errorf("unrecognised input type '%v'", t)
			}
			conf.Input.Broker.Inputs = append(conf.Input.Broker.Inputs, c)
		}
	}

	for _, t := range processorTypes {
		c := processor.NewConfig()
		if _, exists := processor.Constructors[t]; exists {
			c.Type = t
		} else {
			return fmt.Errorf("unrecognised processor type '%v'", t)
		}
		conf.Pipeline.Processors = append(conf.Pipeline.Processors, c)
	}

	if lOutputs := len(outputTypes); lOutputs == 1 {
		t := outputTypes[0]
		if _, exists := output.Constructors[t]; exists {
			conf.Output.Type = t
		} else {
			return fmt.Errorf("unrecognised output type '%v'", t)
		}
	} else if lOutputs > 1 {
		conf.Output.Type = output.TypeBroker
		for _, t := range outputTypes {
			c := output.NewConfig()
			if _, exists := output.Constructors[t]; exists {
				c.Type = t
			} else {
				return fmt.Errorf("unrecognised output type '%v'", t)
			}
			conf.Output.Broker.Outputs = append(conf.Output.Broker.Outputs, c)
		}
	}
	return nil
}

//------------------------------------------------------------------------------

// RunWithOpts runs the Benthos service after first applying opt funcs, which
// are used for specify service customisations.
func RunWithOpts(opts ...func()) {
	for _, opt := range opts {
		opt()
	}
	Run()
}

// Run the Benthos service, if the pipeline is started successfully then this
// call blocks until either the pipeline shuts down or a termination signal is
// received.
func Run() {
	flags := []cli.Flag{
		&cli.BoolFlag{
			Name:    "version",
			Aliases: []string{"v"},
			Value:   false,
			Usage:   "display version info, then exit",
		},
		&cli.StringFlag{
			Name:  "log.level",
			Value: "",
			Usage: "override the configured log level, options are: off, error, warn, info, debug, trace.",
		},
		&cli.StringFlag{
			Name:    "config",
			Aliases: []string{"c"},
			Value:   "",
			Usage:   "a path to a configuration file",
		},
		&cli.StringSliceFlag{
			Name:    "resources",
			Aliases: []string{"r"},
			Usage:   "BETA FEATURE: parse extra resources from a file, which can be referenced the same as resources defined in the main config",
		},
		&cli.BoolFlag{
			Name:  "chilled",
			Value: false,
			Usage: "continue to execute a config containing linter errors",
		},
	}
	if len(customFlags) > 0 {
		flags = append(flags, customFlags...)
	}

	app := &cli.App{
		Name:  "benthos",
		Usage: "A stream processor for mundane tasks - https://benthos.dev",
		Description: `
   Either run Benthos as a stream processor or choose a command:

   benthos list inputs
   benthos create kafka_balanced//file > ./config.yaml
   benthos -c ./config.yaml`[4:],
		Flags: flags,
		Action: func(c *cli.Context) error {
			if c.Bool("version") {
				cmdVersion(Version, DateBuilt)
			}
			if c.Args().Len() > 0 {
				fmt.Fprintf(os.Stderr, "Unrecognised command: %v\n", c.Args().First())
				cli.ShowAppHelp(c)
				os.Exit(1)
			}
			os.Exit(cmdService(
				c.String("config"),
				c.StringSlice("resources"),
				c.String("log.level"),
				!c.Bool("chilled"),
				false,
				nil,
			))
			return nil
		},
		Commands: []*cli.Command{
			{
				Name:  "echo",
				Usage: "Parse a config file and echo back a normalised version",
				Description: `
   This simple command is useful for sanity checking a config if it isn't
   behaving as expected, as it shows you a normalised version after environment
   variables have been resolved:

   benthos -c ./config.yaml echo | less`[4:],
				Action: func(c *cli.Context) error {
					readConfig(c.String("config"), c.StringSlice("resources"))
					outConf, err := conf.Sanitised()
					if err == nil {
						var configYAML []byte
						if configYAML, err = uconfig.MarshalYAML(outConf); err == nil {
							fmt.Println(string(configYAML))
						}
					}
					if err != nil {
						fmt.Fprintln(os.Stderr, fmt.Sprintf("Echo error: %v", err))
						os.Exit(1)
					}
					return nil
				},
			},
			lintCliCommand(),
			{
				Name:  "streams",
				Usage: "Run Benthos in streams mode",
				Description: `
   Run Benthos in streams mode, where multiple pipelines can be executed in a
   single process and can be created, updated and removed via REST HTTP
   endpoints.

   benthos streams ./path/to/stream/configs ./and/some/more
   benthos -c ./root_config.yaml streams ./path/to/stream/configs
   benthos -c ./root_config.yaml streams

   In streams mode the stream fields of a root target config (input, buffer,
   pipeline, output) will be ignored. Other fields will be shared across all
   loaded streams (resources, metrics, etc).

   For more information check out the docs at:
   https://benthos.dev/docs/guides/streams_mode/about`[4:],
				Action: func(c *cli.Context) error {
					os.Exit(cmdService(
						c.String("config"),
						c.StringSlice("resources"),
						c.String("log.level"),
						!c.Bool("chilled"),
						true,
						c.Args().Slice(),
					))
					return nil
				},
			},
			{
				Name:  "list",
				Usage: "List all Benthos component types",
				Description: `
   If any component types are explicitly listed then only types of those
   components will be shown.

   benthos list
   benthos list inputs output
   benthos list rate-limits buffers`[4:],
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "format",
						Value: "text",
						Usage: "Print the component list in a specific format. Options are text or json.",
					},
				},
				Action: func(c *cli.Context) error {
					listComponents(c)
					os.Exit(0)
					return nil
				},
			},
			{
				Name:  "create",
				Usage: "Create a new Benthos config",
				Description: `
   Prints a new Benthos config to stdout containing specified components
   according to an expression. The expression must take the form of three
   comma-separated lists of inputs, processors and outputs, divided by
   forward slashes:

   benthos create stdin/jmespath,awk/nats
   benthos create file,http_server/json/http_client

   If the expression is omitted a default config is created.`[4:],
				Action: func(c *cli.Context) error {
					if expression := c.Args().First(); len(expression) > 0 {
						if err := addExpression(&conf, expression); err != nil {
							fmt.Fprintln(os.Stderr, fmt.Sprintf("Generate error: %v", err))
							os.Exit(1)
						}
					}
					outConf, err := conf.SanitisedNoDeprecated()
					if err == nil {
						var configYAML []byte
						if configYAML, err = uconfig.MarshalYAML(outConf); err == nil {
							fmt.Println(string(configYAML))
						}
					}
					if err != nil {
						fmt.Fprintln(os.Stderr, fmt.Sprintf("Generate error: %v", err))
						os.Exit(1)
					}
					return nil
				},
			},
			test.CliCommand(testSuffix),
			blobl.CliCommand(),
		},
	}

	app.OnUsageError = func(context *cli.Context, err error, isSubcommand bool) error {
		flags, notDeprecated := checkDeprecatedFlags(os.Args[1:])
		if !notDeprecated {
			fmt.Printf("Usage error: %v\n", err)
			cli.ShowAppHelp(context)
			return err
		}

		showVersion := flags.Bool(
			"version", false, "Display version info, then exit",
		)
		configPath := flags.String(
			"c", "", "Path to a configuration file",
		)

		flags.Usage = func() {
			cli.ShowAppHelp(context)
		}

		flags.Parse(os.Args[1:])
		if *showVersion {
			cmdVersion(Version, DateBuilt)
		}

		deprecatedExecute(*configPath, testSuffix)
		os.Exit(cmdService(*configPath, nil, "", false, false, nil))
		return nil
	}

	app.Run(os.Args)
}

//------------------------------------------------------------------------------
