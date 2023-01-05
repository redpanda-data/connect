package cli

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path"
	"runtime"
	"sync"

	"github.com/fatih/color"
	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/config"
	"github.com/benthosdev/benthos/v4/internal/docs"
	ifilepath "github.com/benthosdev/benthos/v4/internal/filepath"
	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
)

var (
	red    = color.New(color.FgRed).SprintFunc()
	yellow = color.New(color.FgYellow).SprintFunc()
)

type pathLint struct {
	source string
	lint   docs.Lint
}

func lintFile(path string, opts config.LintOptions) (pathLints []pathLint) {
	conf := config.New()
	lints, err := config.ReadFileLinted(ifs.OS(), path, opts, &conf)
	if err != nil {
		var l docs.Lint
		if errors.As(err, &l) {
			pathLints = append(pathLints, pathLint{
				source: path,
				lint:   l,
			})
		} else {
			pathLints = append(pathLints, pathLint{
				source: path,
				lint:   docs.NewLintError(1, docs.LintFailedRead, err.Error()),
			})
		}
		return
	}
	for _, l := range lints {
		pathLints = append(pathLints, pathLint{
			source: path,
			lint:   l,
		})
	}
	return
}

func lintMDSnippets(path string, opts config.LintOptions) (pathLints []pathLint) {
	rawBytes, err := ifs.ReadFile(ifs.OS(), path)
	if err != nil {
		pathLints = append(pathLints, pathLint{
			source: path,
			lint:   docs.NewLintError(1, docs.LintFailedRead, err.Error()),
		})
		return
	}

	startTag, endTag := []byte("```yaml"), []byte("```")

	nextSnippet := bytes.Index(rawBytes, startTag)
	for nextSnippet != -1 {
		nextSnippet += len(startTag)

		snippetLine := bytes.Count(rawBytes[:nextSnippet], []byte("\n")) + 1

		endOfSnippet := bytes.Index(rawBytes[nextSnippet:], endTag)
		if endOfSnippet == -1 {
			pathLints = append(pathLints, pathLint{
				source: path,
				lint:   docs.NewLintError(snippetLine, docs.LintFailedRead, "markdown snippet not terminated"),
			})
			return
		}
		endOfSnippet = nextSnippet + endOfSnippet + len(endTag)

		conf := config.New()
		configBytes := rawBytes[nextSnippet : endOfSnippet-len(endTag)]

		if err := yaml.Unmarshal(configBytes, &conf); err != nil {
			var l docs.Lint
			if errors.As(err, &l) {
				l.Line += snippetLine - 1
				pathLints = append(pathLints, pathLint{
					source: path,
					lint:   l,
				})
			} else {
				pathLints = append(pathLints, pathLint{
					source: path,
					lint:   docs.NewLintError(snippetLine, docs.LintFailedRead, err.Error()),
				})
			}
		} else {
			lints, err := config.LintBytes(opts, configBytes)
			if err != nil {
				pathLints = append(pathLints, pathLint{
					source: path,
					lint:   docs.NewLintError(snippetLine, docs.LintFailedRead, err.Error()),
				})
			}
			for _, l := range lints {
				l.Line += snippetLine - 1
				pathLints = append(pathLints, pathLint{
					source: path,
					lint:   l,
				})
			}
		}

		if nextSnippet = bytes.Index(rawBytes[endOfSnippet:], []byte("```yaml")); nextSnippet != -1 {
			nextSnippet += endOfSnippet
		}
	}
	return
}

func lintCliCommand() *cli.Command {
	return &cli.Command{
		Name:  "lint",
		Usage: "Parse Benthos configs and report any linting errors",
		Description: `
Exits with a status code 1 if any linting errors are detected:

  benthos -c target.yaml lint
  benthos lint ./configs/*.yaml
  benthos lint ./foo.yaml ./bar.yaml
  benthos lint ./configs/...

If a path ends with '...' then Benthos will walk the target and lint any
files with the .yaml or .yml extension.`[1:],
		Flags: []cli.Flag{
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
		},
		Action: func(c *cli.Context) error {
			targets, err := ifilepath.GlobsAndSuperPaths(ifs.OS(), c.Args().Slice(), "yaml", "yml")
			if err != nil {
				fmt.Fprintf(os.Stderr, "Lint paths error: %v\n", err)
				os.Exit(1)
			}
			if conf := c.String("config"); len(conf) > 0 {
				targets = append(targets, conf)
			}

			lintOpts := config.LintOptions{
				RejectDeprecated: c.Bool("deprecated"),
				RequireLabels:    c.Bool("labels"),
			}

			var pathLintMut sync.Mutex
			var pathLints []pathLint
			threads := runtime.NumCPU()
			var wg sync.WaitGroup
			wg.Add(threads)
			for i := 0; i < threads; i++ {
				go func(threadID int) {
					defer wg.Done()
					for j, target := range targets {
						if j%threads != threadID {
							continue
						}
						if target == "" {
							continue
						}
						var lints []pathLint
						if path.Ext(target) == ".md" {
							lints = lintMDSnippets(target, lintOpts)
						} else {
							lints = lintFile(target, lintOpts)
						}
						if len(lints) > 0 {
							pathLintMut.Lock()
							pathLints = append(pathLints, lints...)
							pathLintMut.Unlock()
						}
					}
				}(i)
			}
			wg.Wait()
			if len(pathLints) == 0 {
				os.Exit(0)
			}
			for _, lint := range pathLints {
				lintText := fmt.Sprintf("%v%v\n", lint.source, lint.lint.Error())
				if lint.lint.Type == docs.LintFailedRead || lint.lint.Type == docs.LintComponentMissing {
					fmt.Fprint(os.Stderr, red(lintText))
				} else {
					fmt.Fprint(os.Stderr, yellow(lintText))
				}
			}
			os.Exit(1)
			return nil
		},
	}
}
