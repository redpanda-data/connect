// Copyright 2024 Redpanda Data, Inc.
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
	"log/slog"
	"os"

	"github.com/urfave/cli/v2"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/kafka/enterprise"
	"github.com/redpanda-data/connect/v4/internal/mcp"
)

func mcpServerCli(rpMgr *enterprise.GlobalRedpandaManager) *cli.Command {
	flags := []cli.Flag{
		&cli.StringFlag{
			Name:  "address",
			Usage: "An optional address to bind the MCP server to instead of running in stdio mode.",
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

			fallbackLogger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
				Level: slog.LevelError,
			}))

			addr := c.String("address")
			if addr != "" {
				// It's safe to initialise a stdout logger
				fallbackLogger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
					Level: slog.LevelInfo,
				}))
			}

			rpMgr.SetFallbackLogger(service.NewLoggerFromSlog(fallbackLogger))
			// TODO: rpMgr.Init...
			logger := slog.New(newTeeLogger(fallbackLogger.Handler(), rpMgr.SlogHandler()))

			secretLookupFn, err := parseSecretsFlag(logger, c)
			if err != nil {
				return err
			}

			if err := mcp.Run(logger, secretLookupFn, repositoryDir, addr); err != nil {
				return err
			}
			return nil
		},
	}
}

type teeLogger struct {
	main      slog.Handler
	secondary slog.Handler
}

func newTeeLogger(main, secondary slog.Handler) *teeLogger {
	return &teeLogger{
		main:      main,
		secondary: secondary,
	}
}

func (t *teeLogger) Enabled(ctx context.Context, level slog.Level) bool {
	return t.main.Enabled(ctx, level)
}

func (t *teeLogger) Handle(ctx context.Context, record slog.Record) error {
	if err := t.main.Handle(ctx, record); err != nil {
		return err
	}
	return t.secondary.Handle(ctx, record)
}

func (t *teeLogger) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &teeLogger{
		main:      t.main.WithAttrs(attrs),
		secondary: t.secondary.WithAttrs(attrs),
	}
}

func (t *teeLogger) WithGroup(name string) slog.Handler {
	return &teeLogger{
		main:      t.main.WithGroup(name),
		secondary: t.secondary.WithGroup(name),
	}
}
