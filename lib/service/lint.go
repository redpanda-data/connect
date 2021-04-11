package service

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/Jeffail/benthos/v3/lib/config"
	"github.com/fatih/color"
	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v3"
)

var red = color.New(color.FgRed).SprintFunc()
var yellow = color.New(color.FgYellow).SprintFunc()

func resolveLintPath(path string) (string, bool) {
	recurse := false
	if path == "./..." || path == "..." {
		recurse = true
		path = "."
	}
	if strings.HasSuffix(path, "/...") {
		recurse = true
		path = strings.TrimSuffix(path, "/...")
	}
	return path, recurse
}

type pathLint struct {
	source string
	line   int
	lint   string
	err    string
}

func lintFile(path string) (pathLints []pathLint) {
	conf := config.New()
	lints, err := config.Read(path, true, &conf)
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

func lintMDSnippets(path string) (pathLints []pathLint) {
	rawBytes, err := ioutil.ReadFile(path)
	if err != nil {
		pathLints = append(pathLints, pathLint{
			source: path,
			err:    err.Error(),
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
			lints, err := config.Lint(configBytes, conf)
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
   files with the .yaml or .yml extension.`[4:],
		Action: func(c *cli.Context) error {
			var targets []string
			for _, p := range c.Args().Slice() {
				var recurse bool
				if p, recurse = resolveLintPath(p); recurse {
					if err := filepath.Walk(p, func(path string, info os.FileInfo, werr error) error {
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
				} else {
					targets = append(targets, p)
				}
			}
			if conf := c.String("config"); len(conf) > 0 {
				targets = append(targets, conf)
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
						if len(target) == 0 {
							continue
						}
						var lints []pathLint
						if path.Ext(target) == ".md" {
							lints = lintMDSnippets(target)
						} else {
							lints = lintFile(target)
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
