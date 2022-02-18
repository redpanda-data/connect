package test

import (
	"fmt"
	"os"

	"github.com/Jeffail/benthos/v3/internal/filepath"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/urfave/cli/v2"
)

// CliCommand is a cli.Command definition for unit testing.
func CliCommand(testSuffix string) *cli.Command {
	return &cli.Command{
		Name:  "test",
		Usage: "Execute Benthos unit tests",
		Description: `
Execute any number of Benthos unit test definitions. If one or more tests
fail the process will report the errors and exit with a status code 1.

  benthos test ./path/to/configs/...
  benthos test ./foo_configs ./bar_configs
  benthos test ./foo.yaml

For more information check out the docs at:
https://benthos.dev/docs/configuration/unit_testing`[1:],
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "generate",
				Value: false,
				Usage: "instead of testing, detect untested Benthos configs and generate test definitions for them.",
			},
			&cli.StringFlag{
				Name:  "log",
				Value: "",
				Usage: "allow components to write logs at a provided level to stdout.",
			},
		},
		Action: func(c *cli.Context) error {
			if c.Bool("generate") {
				if err := GenerateAll(
					c.Args().Slice(), testSuffix,
				); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to generate config tests: %v\n", err)
					os.Exit(1)
				}
				os.Exit(0)
			}
			if len(c.StringSlice("set")) > 0 {
				fmt.Fprintln(os.Stderr, "Cannot override fields with --set (-s) during unit tests")
				os.Exit(1)
			}
			resourcesPaths := c.StringSlice("resources")
			var err error
			if resourcesPaths, err = filepath.Globs(resourcesPaths); err != nil {
				fmt.Printf("Failed to resolve resource glob pattern: %v\n", err)
				os.Exit(1)
			}
			if logLevel := c.String("log"); len(logLevel) > 0 {
				logConf := log.NewConfig()
				logConf.LogLevel = logLevel
				logger, err := log.NewV2(os.Stdout, logConf)
				if err != nil {
					fmt.Printf("Failed to init logger: %v\n", err)
					os.Exit(1)
				}
				if runAll(c.Args().Slice(), testSuffix, true, logger, resourcesPaths) {
					os.Exit(0)
				}
			} else if runAll(c.Args().Slice(), testSuffix, true, log.Noop(), resourcesPaths) {
				os.Exit(0)
			}
			os.Exit(1)
			return nil
		},
	}
}
