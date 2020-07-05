package service

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Jeffail/benthos/v3/lib/config"
	"github.com/urfave/cli/v2"
)

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
			var pathLints []string
			for _, target := range targets {
				if len(target) == 0 {
					continue
				}
				var conf = config.New()
				lints, err := config.Read(target, true, &conf)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Configuration file read error '%v': %v\n", target, err)
					os.Exit(1)
				}
				for _, l := range lints {
					pathLints = append(pathLints, target+": "+l)
				}
			}
			if len(pathLints) == 0 {
				os.Exit(0)
			}
			for _, lint := range pathLints {
				fmt.Fprintln(os.Stderr, lint)
			}
			os.Exit(1)
			return nil
		},
	}
}
