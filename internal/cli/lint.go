package cli

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"

	"github.com/Jeffail/benthos/v3/internal/config"
	"github.com/Jeffail/benthos/v3/internal/docs"
	ifilepath "github.com/Jeffail/benthos/v3/internal/filepath"
	"github.com/fatih/color"
	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v3"
)

var red = color.New(color.FgRed).SprintFunc()
var yellow = color.New(color.FgYellow).SprintFunc()

type pathLint struct {
	source string
	line   int
	lint   string
	err    string
}

func lintFile(path string, rejectDeprecated bool) (pathLints []pathLint) {
	conf := config.New()
	lints, err := config.ReadFileLinted(path, rejectDeprecated, &conf)
	if err != nil {
		pathLints = append(pathLints, pathLint{
			source: path,
			err:    err.Error(),
		})
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

func lintMDSnippets(path string, rejectDeprecated bool) (pathLints []pathLint) {
	rawBytes, err := os.ReadFile(path)
	if err != nil {
		pathLints = append(pathLints, pathLint{
			source: path,
			err:    err.Error(),
		})
		return
	}

	// TODO: V4, remove this dodgy work around
	if strings.HasSuffix(path, "zmq4.md") {
		return nil
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
				line:   snippetLine,
				err:    "markdown snippet not terminated",
			})
			return
		}
		endOfSnippet = nextSnippet + endOfSnippet + len(endTag)

		conf := config.New()
		configBytes := rawBytes[nextSnippet : endOfSnippet-len(endTag)]

		if err := yaml.Unmarshal(configBytes, &conf); err != nil {
			pathLints = append(pathLints, pathLint{
				source: path,
				line:   snippetLine,
				err:    err.Error(),
			})
		} else {
			lintCtx := docs.NewLintContext()
			lintCtx.RejectDeprecated = rejectDeprecated
			lints, err := config.LintBytes(lintCtx, configBytes)
			if err != nil {
				pathLints = append(pathLints, pathLint{
					source: path,
					line:   snippetLine,
					err:    err.Error(),
				})
			}
			for _, l := range lints {
				pathLints = append(pathLints, pathLint{
					source: path,
					line:   snippetLine,
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
		},
		Action: func(c *cli.Context) error {
			targets, err := ifilepath.GlobsAndSuperPaths(c.Args().Slice(), "yaml", "yml")
			if err != nil {
				fmt.Fprintf(os.Stderr, "Lint paths error: %v\n", err)
				os.Exit(1)
			}
			if conf := c.String("config"); len(conf) > 0 {
				targets = append(targets, conf)
			}

			rejectDeprecated := c.Bool("deprecated")

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
							lints = lintMDSnippets(target, rejectDeprecated)
						} else {
							lints = lintFile(target, rejectDeprecated)
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
				message := yellow(lint.lint)
				if len(lint.err) > 0 {
					message = red(lint.err)
				}
				if lint.line > 0 {
					fmt.Fprintf(os.Stderr, "%v: from snippet at line %v: %v\n", lint.source, lint.line, message)
				} else {
					fmt.Fprintf(os.Stderr, "%v: %v\n", lint.source, message)
				}
			}
			os.Exit(1)
			return nil
		},
	}
}
