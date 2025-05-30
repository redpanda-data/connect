/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package cli

import (
	"errors"

	"github.com/redpanda-data/connect/v4/internal/rpcplugin"
	"github.com/urfave/cli/v2"
)

func pluginInit() *cli.Command {
	flags := []cli.Flag{
		&cli.StringFlag{
			Name:    "language",
			Aliases: []string{"lang"},
			Usage:   "The programming language to use for the plugin. Supported languages are: golang, python.",
			Value:   "python",
		},
		&cli.StringFlag{
			Name:  "component",
			Usage: "The type of component to generate. Supported components are: input, output, processor.",
			Value: "processor",
		},
	}

	cmd := &cli.Command{
		Name:  "init",
		Usage: "Create the boilerplate for a RPC plugin.",
		Flags: flags,
		Description: `
!!EXPERIMENTAL!!

Generates a project on the local filesystem that can be used as a starting point for
building a custom component for Redpanda Connect. It will overwrite all files in the specified
directory (or the current directory if none is specified).
  `[1:],
		Action: func(c *cli.Context) error {
			dir := "."
			if c.Args().Len() > 0 {
				if c.Args().Len() > 1 {
					return errors.New("a maximum of one repository directory must be specified with this command")
				}
				dir = c.Args().First()
			}
			lang := rpcplugin.PluginLanguage(c.String("language"))
			comp := rpcplugin.ComponentType(c.String("component"))
			return rpcplugin.InitializeProject(lang, comp, dir)
		},
	}
	return &cli.Command{
		Name:        "plugin",
		Usage:       "Plugin management commands",
		Subcommands: []*cli.Command{cmd},
	}
}
