package template

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Jeffail/benthos/v3/internal/template"
	"github.com/fatih/color"
	"github.com/urfave/cli/v2"
)

var red = color.New(color.FgRed).SprintFunc()
var yellow = color.New(color.FgYellow).SprintFunc()

func resolveLintPath(path string) []string {
	recurse := false
	if path == "./..." || path == "..." {
		recurse = true
		path = "."
	}
	if strings.HasSuffix(path, "/...") {
		recurse = true
		path = strings.TrimSuffix(path, "/...")
	}
	if recurse {
		var targets []string
		if err := filepath.Walk(path, func(path string, info os.FileInfo, werr error) error {
			if werr != nil {
				return werr
			}
			if info.IsDir() {
				return nil
			}
			if strings.HasSuffix(path, ".yaml") ||
				strings.HasSuffix(path, ".yml") {
				targets = append(targets, path)
			}
			return nil
		}); err != nil {
			fmt.Fprintf(os.Stderr, "Filesystem walk error: %v\n", err)
			os.Exit(1)
		}
		return targets
	}
	return []string{path}
}

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
			var targets []string
			for _, p := range c.Args().Slice() {
				targets = append(targets, resolveLintPath(p)...)
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
