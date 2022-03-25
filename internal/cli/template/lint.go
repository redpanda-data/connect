package template

import (
	"fmt"
	"os"

	"github.com/fatih/color"
	"github.com/urfave/cli/v2"

	ifilepath "github.com/benthosdev/benthos/v4/internal/filepath"
	"github.com/benthosdev/benthos/v4/internal/template"
)

var red = color.New(color.FgRed).SprintFunc()
var yellow = color.New(color.FgYellow).SprintFunc()

type pathLint struct {
	source string
	lint   string
	err    string
}

func lintFile(path string) (pathLints []pathLint) {
	conf, lints, err := template.ReadConfig(path)
	if err != nil {
		pathLints = append(pathLints, pathLint{
			source: path,
			err:    red(err.Error()),
		})
		return
	}

	for _, l := range lints {
		pathLints = append(pathLints, pathLint{
			source: path,
			lint:   l,
		})
	}

	testErrors, err := conf.Test()
	if err != nil {
		pathLints = append(pathLints, pathLint{
			source: path,
			err:    err.Error(),
		})
		return
	}

	for _, tErr := range testErrors {
		pathLints = append(pathLints, pathLint{
			source: path,
			err:    tErr,
		})
	}
	return
}

func lintCliCommand() *cli.Command {
	return &cli.Command{
		Name:  "lint",
		Usage: "Parse Benthos templates and report any linting errors",
		Description: `
Exits with a status code 1 if any linting errors are detected:

  benthos template lint
  benthos template lint ./templates/*.yaml
  benthos template lint ./foo.yaml ./bar.yaml
  benthos template lint ./templates/...

If a path ends with '...' then Benthos will walk the target and lint any
files with the .yaml or .yml extension.`[1:],
		Action: func(c *cli.Context) error {
			targets, err := ifilepath.GlobsAndSuperPaths(c.Args().Slice(), "yaml", "yml")
			if err != nil {
				fmt.Fprintf(os.Stderr, "Lint paths error: %v\n", err)
				os.Exit(1)
			}
			var pathLints []pathLint
			for _, target := range targets {
				if target == "" {
					continue
				}
				lints := lintFile(target)
				if len(lints) > 0 {
					pathLints = append(pathLints, lints...)
				}
			}
			if len(pathLints) == 0 {
				os.Exit(0)
			}
			for _, lint := range pathLints {
				message := yellow(lint.lint)
				if len(lint.err) > 0 {
					message = red(lint.err)
				}
				fmt.Fprintf(os.Stderr, "%v: %v\n", lint.source, message)
			}
			os.Exit(1)
			return nil
		},
	}
}
