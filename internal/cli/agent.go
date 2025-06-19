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
	"log/slog"
	"os"
	"path/filepath"

	"github.com/urfave/cli/v2"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/agent"
	"github.com/redpanda-data/connect/v4/internal/impl/kafka/enterprise"
)

func agentCli(rpMgr *enterprise.GlobalRedpandaManager) *cli.Command {
	flags := []cli.Flag{
		secretsFlag,
		licenseFlag,
	}
	if shouldAddChrootFlag() {
		flags = append(flags, chrootFlag)
	}

	return &cli.Command{
		Name:  "agent",
		Usage: "Redpanda Connect commands.",
		Subcommands: []*cli.Command{
			{
				Name:  "init",
				Usage: "Initialize a Redpanda Connect agent",
				// TODO: This is a junk description. Make it better.
				Flags: []cli.Flag{&cli.StringFlag{Name: "name"}},
				Description: `
!!EXPERIMENTAL!!

Initialize a template for building a Redpanda Connect agent.

  {{.BinaryName}} agent init ./repo
  
  `[1:],
				Action: func(c *cli.Context) error {
					repositoryDir := "."
					if c.Args().Len() > 0 {
						if c.Args().Len() > 1 {
							return errors.New("a maximum of one repository directory must be specified with this command")
						}
						repositoryDir = c.Args().First()
					}
					name := c.String("name")
					if name == "" {
						dir, _ := filepath.Abs(repositoryDir)
						name = filepath.Base(dir)
					}
					if name == "" || name == "." || name == string(filepath.Separator) {
						name = "my_redpanda_agent"
					}
					return agent.CreateTemplate(repositoryDir, map[string]string{
						"REDPANDA_PROJECT_NAME": name,
					})
				},
			},
			{
				Name:  "run",
				Usage: "Execute a Redpanda Connect agent as part of a pipeline that has access to tools via the MCP protocol",
				Flags: flags,
				// TODO: This is a junk description. Make it better.
				Description: `
!!EXPERIMENTAL!!

Each resource in the mcp subdirectory will create tools that can be used, then the redpanda_agents.yaml file along with python agent modules will be invoked:

  {{.BinaryName}} agent run ./repo
  
  `[1:],
				Action: func(c *cli.Context) error {
					repositoryDir := "."
					if c.Args().Len() > 0 {
						if c.Args().Len() > 1 {
							return errors.New("a maximum of one repository directory must be specified with this command")
						}
						repositoryDir = c.Args().First()
					}

					licenseConfig := defaultLicenseConfig()
					applyLicenseFlag(c, &licenseConfig)

					// It's safe to initialise a stdout logger
					fallbackLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
						Level: slog.LevelDebug,
					}))

					rpMgr.SetFallbackLogger(service.NewLoggerFromSlog(fallbackLogger))
					// TODO: rpMgr.Init...
					logger := slog.New(newTeeLogger(fallbackLogger.Handler(), rpMgr.SlogHandler()))

					secretLookupFn, err := parseSecretsFlag(logger, c)
					if err != nil {
						return err
					}
					if err := agent.RunAgent(logger, secretLookupFn, repositoryDir, licenseConfig); err != nil {
						return err
					}
					return nil
				},
			},
		},
	}
}
