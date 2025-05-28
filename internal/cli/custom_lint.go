// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package cli

import (
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/fatih/color"
	"github.com/urfave/cli/v2"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/mcp/repository"
)

var (
	red    = color.New(color.FgRed).SprintFunc()
	yellow = color.New(color.FgYellow).SprintFunc()
	green  = color.New(color.FgGreen).SprintFunc()
)

func customLintCli() *cli.Command {
	flags := []cli.Flag{
		&cli.BoolFlag{
			Name:  "deprecated",
			Value: false,
			Usage: "Print linting errors for the presence of deprecated fields.",
		},
		&cli.BoolFlag{
			Name:  "labels",
			Value: false,
			Usage: "Print linting errors when components do not have labels.",
		},
		&cli.BoolFlag{
			Name:  "skip-env-var-check",
			Value: false,
			Usage: "Do not produce lint errors when environment interpolations exist without defaults within configs but aren't defined.",
		},
		&cli.BoolFlag{
			Name:  "verbose",
			Value: false,
			Usage: "Print the lint result for each target file.",
		},

		secretsFlag,
		envFileFlag,
	}

	return &cli.Command{
		Name:  "lint",
		Usage: "Parse {{.ProductName}} configs and report any linting errors",
		Flags: flags,
		Description: `
Exits with a status code 1 if any linting errors are detected in a directory:

  {{.BinaryName}} mcp-server lint . 
  {{.BinaryName}} mcp-server lint ./foo`[1:],
		Action: func(c *cli.Context) error {
			if err := applyEnvFileFlag(c); err != nil {
				return err
			}

			repositoryDir := "."
			if c.Args().Len() > 0 {
				if c.Args().Len() > 1 {
					return errors.New("a maximum of one repository directory must be specified with this command")
				}
				repositoryDir = c.Args().First()
			}

			return directoryMode(c, repositoryDir)
		},
	}
}

func directoryMode(c *cli.Context, repositoryDir string) error {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelError,
	}))

	secretLookupFn, err := parseSecretsFlag(logger, c)
	if err != nil {
		return err
	}

	env := service.NewEnvironment()

	cLinter := env.NewComponentConfigLinter()
	cLinter.SetRejectDeprecated(c.Bool("deprecated"))
	cLinter.SetRequireLabels(c.Bool("labels"))
	cLinter.SetSkipEnvVarCheck(c.Bool("skip-env-var-check"))
	cLinter.SetEnvVarLookupFunc(secretLookupFn)

	verbose := c.Bool("verbose")

	type pathLint struct {
		fileName string
		lints    []service.Lint
		err      error
	}

	var pathLints []pathLint

	reportFileLints := func(fileName string, lints []service.Lint, err error) {
		if verbose {
			if err == nil && len(lints) == 0 {
				fmt.Fprintf(os.Stdout, "%v: %v\n", fileName, green("OK"))
			} else {
				fmt.Fprintf(os.Stdout, "%v: %v\n", fileName, red("FAILED"))
			}
		}

		pathLints = append(pathLints, pathLint{
			fileName: fileName,
			lints:    lints,
			err:      err,
		})
	}

	repoScanner := repository.NewScanner(os.DirFS(repositoryDir))

	repoScanner.OnTemplateFile(func(filePath string, contents []byte) error {
		return env.RegisterTemplateYAML(string(contents))
	})

	repoScanner.OnResourceFile(func(resourceType, fileName string, contents []byte) error {
		if resourceType != "starlark" {
			lints, err := cLinter.LintYAML(resourceType, contents)
			reportFileLints(fileName, lints, err)
		}
		return nil
	})

	repoScanner.OnMetricsFile(func(fileName string, contents []byte) error {
		lints, err := cLinter.LintYAML("metrics", contents)
		reportFileLints(fileName, lints, err)
		return nil
	})

	repoScanner.OnTracerFile(func(fileName string, contents []byte) error {
		lints, err := cLinter.LintYAML("tracer", contents)
		reportFileLints(fileName, lints, err)
		return nil
	})

	if err := repoScanner.Scan("."); err != nil {
		return err
	}

	for _, pl := range pathLints {
		for _, lint := range pl.lints {
			lintText := fmt.Sprintf("%v%v\n", pl.fileName, lint.Error())
			fmt.Fprint(os.Stderr, yellow(lintText))
		}
		if pl.err != nil {
			lintText := fmt.Sprintf("%v%v\n", pl.fileName, pl.err.Error())
			fmt.Fprint(os.Stderr, red(lintText))
		}
	}

	return nil
}
