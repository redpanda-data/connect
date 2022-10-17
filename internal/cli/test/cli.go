package test

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"

	"github.com/benthosdev/benthos/v4/internal/filepath"
	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
	"github.com/benthosdev/benthos/v4/internal/log"
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
  benthos test ./foo_configs/*.yaml ./bar_configs/*.yaml
  benthos test ./foo.yaml

For more information check out the docs at:
https://benthos.dev/docs/configuration/unit_testing`[1:],
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "log",
				Value: "",
				Usage: "allow components to write logs at a provided level to stdout.",
			},
		},
		Action: func(c *cli.Context) error {
			if len(c.StringSlice("set")) > 0 {
				fmt.Fprintln(os.Stderr, "Cannot override fields with --set (-s) during unit tests")
				os.Exit(1)
			}
			resourcesPaths := c.StringSlice("resources")
			var err error
			if resourcesPaths, err = filepath.Globs(ifs.OS(), resourcesPaths); err != nil {
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
				if RunAll(c.Args().Slice(), testSuffix, true, logger, resourcesPaths) {
					os.Exit(0)
				}
			} else if RunAll(c.Args().Slice(), testSuffix, true, log.Noop(), resourcesPaths) {
				os.Exit(0)
			}
			os.Exit(1)
			return nil
		},
	}
}
