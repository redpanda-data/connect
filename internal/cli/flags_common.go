// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package cli

import (
	"fmt"
	"os"
	"slices"

	"github.com/urfave/cli/v2"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	cfEnvFile = "env-file"
)

var envFileFlag = &cli.StringSliceFlag{
	Name:    "env-file",
	Aliases: []string{"e"},
	Value:   cli.NewStringSlice(),
	Usage:   "import environment variables from a dotenv file",
}

func applyEnvFileFlag(c *cli.Context) error {
	dotEnvPaths, err := service.Globs(service.OSFS(), c.StringSlice(cfEnvFile)...)
	if err != nil {
		return fmt.Errorf("resolving env file glob pattern: %w", err)
	}
	for _, dotEnvFile := range dotEnvPaths {
		dotEnvBytes, err := service.ReadFile(service.OSFS(), dotEnvFile)
		if err != nil {
			return fmt.Errorf("reading dotenv file: %w", err)
		}
		vars, err := service.ParseEnvFile(dotEnvBytes)
		if err != nil {
			return fmt.Errorf("parsing dotenv file: %w", err)
		}
		for k, v := range vars {
			if err = os.Setenv(k, v); err != nil {
				return fmt.Errorf("setting env var '%v': %w", k, err)
			}
		}
	}
	return nil
}

var rpcPluginsFlag = &cli.StringSliceFlag{
	Name:  "rpc-plugins",
	Usage: "Plugins to load over the RPC interface. This flag should point to manifest files containing the plugin definitions. Globs are also supported.",
}

// rpcPluginPaths returns all plugin manifest paths and glob patterns specified
// via the --rpc-plugins flag, collected across both root and subcommand levels.
//
// Do not replace this with c.StringSlice("rpc-plugins"). Because the flag is
// defined on both the root application and individual subcommands (such as run
// and dry-run), urfave/cli reads the value from the subcommand context, also
// when the flag is not set there. As a result, flags provided before the
// subcommand (e.g., `connect --rpc-plugins <path> run`) would be ignored. This
// helper traverses c.Lineage() to collect flags set at every level.
//
// Paths are ordered from the subcommand up to the root. When manifests are
// registered sequentially, later registrations override earlier ones, so
// root-level definitions take precedence over subcommand-level definitions in
// case of duplicate plugin names.
func rpcPluginPaths(c *cli.Context) []string {
	var paths []string
	for _, cc := range c.Lineage() {
		if slices.Contains(cc.LocalFlagNames(), rpcPluginsFlag.Name) {
			paths = append(paths, cc.StringSlice(rpcPluginsFlag.Name)...)
		}
	}
	return paths
}
