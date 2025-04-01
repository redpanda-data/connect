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

	"github.com/urfave/cli/v2"

	"github.com/redpanda-data/connect/v4/internal/mcp"
)

func mcpServerCli(logger *slog.Logger) *cli.Command {
	flags := []cli.Flag{
		&cli.StringFlag{
			Name:  "base-url",
			Usage: "An optional base URL to bind the MCP server to instead of running in stdio mode.",
		},
		secretsFlag,
	}

	return &cli.Command{
		Name:  "mcp-server",
		Usage: "Execute an MCP server against a suite of Redpanda Connect resources.",
		Flags: flags,
		Description: `
Each resource will be exposed as a tool that AI can interact with:

  {{.BinaryName}} mcp-server ./repo
  
  `[1:],
		Action: func(c *cli.Context) error {
			repositoryDir := "."
			if c.Args().Len() > 0 {
				if c.Args().Len() > 1 {
					return errors.New("a maximum of one repository directory must be specified with this command")
				}
				repositoryDir = c.Args().First()
			}

			secretLookupFn, err := parseSecretsFlag(logger, c)
			if err != nil {
				return err
			}

			if err := mcp.Run(logger, secretLookupFn, repositoryDir, c.String("base-url")); err != nil {
				return err
			}
			return nil
		},
	}
}
