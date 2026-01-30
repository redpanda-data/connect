// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/rs/xid"
	"github.com/urfave/cli/v2"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/kafka/enterprise"
	"github.com/redpanda-data/connect/v4/internal/license"
	"github.com/redpanda-data/connect/v4/internal/mcp/repository"
)

func isDirectory(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.IsDir()
}

func dryRunCli(schema *service.ConfigSchema) *cli.Command {
	flags := []cli.Flag{
		&cli.BoolFlag{
			Name:  "verbose",
			Value: false,
			Usage: "Print the lint result for each target file.",
		},

		secretsFlag,
		envFileFlag,
		licenseFlag,
	}

	return &cli.Command{
		Name:  "dry-run",
		Usage: "Parse {{.ProductName}} configs and test the connections of each plugin",
		Flags: flags,
		Description: `
Exits with a status code 1 if any connection errors are detected in a directory:

  {{.BinaryName}} dry-run ./foo.yaml`[1:],
		Action: func(c *cli.Context) error {
			if err := applyEnvFileFlag(c); err != nil {
				return err
			}

			r := dryRunner{
				schema:        schema,
				licenseConfig: defaultLicenseConfig(),
				logger: slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{
					Level: slog.LevelError,
				})),
				runLogger: &dryRunResultLogger{
					verbose: c.Bool("verbose"),
				},
			}
			applyLicenseFlag(c, &r.licenseConfig)

			var err error
			if r.secretLookupFn, err = parseSecretsFlag(r.logger, c); err != nil {
				return err
			}

			targets, err := service.Globs(service.OSFS(), c.Args().Slice()...)
			if err != nil {
				return err
			}

			for _, target := range targets {
				if isDirectory(target) {
					if err := r.dryRunDirectory(c, target); err != nil {
						return err
					}
				} else {
					if err := r.dryRunFile(c, target); err != nil {
						return err
					}
				}
			}

			if r.runLogger.Report() {
				os.Exit(1)
			}
			return nil
		},
	}
}

type pathResult struct {
	fileName string
	results  service.ConnectionTestResults
}

type dryRunResultLogger struct {
	verbose bool
	results []pathResult
}

func (d *dryRunResultLogger) Add(fileName string, results service.ConnectionTestResults) {
	var errored bool
	for _, res := range results {
		if res.Err != nil && !errors.Is(res.Err, service.ErrConnectionTestNotSupported) {
			errored = true
		}
	}

	if d.verbose {
		if errored {
			fmt.Fprintf(os.Stdout, "%v: %v\n", fileName, red("FAILED"))
		} else {
			fmt.Fprintf(os.Stdout, "%v: %v\n", fileName, green("OK"))
		}
	}

	d.results = append(d.results, pathResult{
		fileName: fileName,
		results:  results,
	})
}

func (d *dryRunResultLogger) Report() (hasRunErrors bool) {
	for _, rr := range d.results {
		for _, res := range rr.results {
			if res.Err != nil && !errors.Is(res.Err, service.ErrConnectionTestNotSupported) {
				hasRunErrors = true
			}
			if res.Err != nil {
				label := res.Label
				if label == "" {
					label = "." + strings.Join(res.Path, ".")
				}

				resText := fmt.Sprintf("[%v] %v\n", label, res.Err)
				if rr.fileName != "" {
					resText = fmt.Sprintf("%v: [%v] %v\n", rr.fileName, label, res.Err)
				}

				if errors.Is(res.Err, service.ErrConnectionTestNotSupported) {
					if d.verbose {
						fmt.Fprint(os.Stderr, yellow(resText))
					}
				} else {
					fmt.Fprint(os.Stderr, red(resText))
				}
			}
		}
	}
	return
}

type dryRunner struct {
	schema         *service.ConfigSchema
	licenseConfig  license.Config
	logger         *slog.Logger
	secretLookupFn func(context.Context, string) (string, bool)
	runLogger      *dryRunResultLogger
}

func (d *dryRunner) dryRunFile(c *cli.Context, filePath string) error {
	fileContents, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	strmBuilder := d.schema.Environment().NewStreamBuilder()
	strmBuilder.DisableLinting()
	strmBuilder.SetLogger(d.logger)
	if err := strmBuilder.SetYAML(string(fileContents)); err != nil {
		return err
	}

	strm, err := strmBuilder.Build()
	if err != nil {
		return err
	}
	resources := strm.Resources()

	license.RegisterService(resources, d.licenseConfig)

	rpMgr := enterprise.NewGlobalRedpandaManager(xid.New().String())
	rpMgr.SetFallbackLogger(service.NewLoggerFromSlog(d.logger))

	confQuerier := d.schema.NewConfigQuerier()
	confQuerier.SetResources(resources)
	confQuerier.SetEnvVarLookupFunc(d.secretLookupFn)

	qFile, err := confQuerier.ParseYAML(string(fileContents))
	if err != nil {
		return err
	}

	rpField, err := qFile.FieldAtPath("redpanda")
	if err != nil {
		return err
	}

	if err := rpMgr.InitFromParsedConfig(rpField); err != nil {
		return err
	}

	connTestResults := rpMgr.ConnectionTest(c.Context)

	if tmpTestResults, err := strm.ConnectionTest(c.Context); err != nil {
		return err
	} else {
		connTestResults = append(connTestResults, tmpTestResults...)
	}

	d.runLogger.Add(filePath, connTestResults)
	return nil
}

func (d *dryRunner) dryRunDirectory(c *cli.Context, repositoryDir string) error {
	resBuilder := d.schema.Environment().NewResourceBuilder()

	repoScanner := repository.NewScanner(os.DirFS(repositoryDir))

	repoScanner.OnTemplateFile(func(_ string, contents []byte) error {
		return d.schema.Environment().RegisterTemplateYAML(string(contents))
	})

	repoScanner.OnResourceFile(func(resourceType, _ string, contents []byte) error {
		switch resourceType {
		case "input":
			if err := resBuilder.AddInputYAML(string(contents)); err != nil {
				return err
			}
		case "cache":
			if err := resBuilder.AddCacheYAML(string(contents)); err != nil {
				return err
			}
		case "processor":
			if err := resBuilder.AddProcessorYAML(string(contents)); err != nil {
				return err
			}
		case "rate_limit":
			if err := resBuilder.AddRateLimitYAML(string(contents)); err != nil {
				return err
			}
		case "output":
			if err := resBuilder.AddOutputYAML(string(contents)); err != nil {
				return err
			}
		default:
			return fmt.Errorf("resource type '%v' is not supported yet", resourceType)
		}
		return nil
	})

	if err := repoScanner.Scan("."); err != nil {
		return err
	}

	resources, closeFn, err := resBuilder.BuildSuspended()
	if err != nil {
		return err
	}

	defer func() {
		_ = closeFn(c.Context)
	}()

	results, err := resources.ConnectionTest(c.Context)
	if err != nil {
		return err
	}

	d.runLogger.Add("", results)
	return nil
}
