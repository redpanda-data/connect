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
func CliCommand() *cli.Command {
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
			&cli.BoolFlag{
				Name:  "legacy",
				Value: true,
				Usage: "use the legacy test runner.",
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

			logger, err := getLogger(c.String("log"))
			if err != nil {
				fmt.Printf("Failed to init Logger: %v\n", err)
				os.Exit(1)
			}

			runnerKind := RunnerKind("unknown")
			if c.Bool("legacy") {
				runnerKind = LegacyRunner
			}
			runner, err := GetRunner(runnerKind)
			if err != nil {
				fmt.Printf("Failed to init runner: %v\n", err)
				os.Exit(1)
			}

			cfg := RunConfig{
				Paths:         c.Args().Slice(),
				TestSuffix:    "_benthos_test",
				Lint:          true,
				Logger:        logger,
				ResourcePaths: resourcesPaths,
			}

			if runner.Run(cfg) {
				os.Exit(0)
			}

			os.Exit(1)
			return nil
		},
	}
}

func getLogger(logLevel string) (log.Modular, error) {
	if len(logLevel) > 0 {
		logConf := log.NewConfig()
		logConf.LogLevel = logLevel
		return log.New(os.Stdout, ifs.OS(), logConf)
	} else {
		return log.Noop(), nil
	}
}
