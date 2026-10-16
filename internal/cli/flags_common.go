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

// rpcPluginsFromLineage returns the rpc-plugins values from each context in
// the lineage of c where the flag is set, without duplicates.
//
// Do not replace this with c.StringSlice("rpc-plugins"). The flag is defined
// on the root app and on subcommands such as run. urfave/cli returns the value
// of the first flag set in the lineage that defines the flag, also when the
// flag is not set there. Thus `connect --rpc-plugins P run` loses P.
func rpcPluginsFromLineage(c *cli.Context) []string {
	var paths []string
	for _, cc := range c.Lineage() {
		if slices.Contains(cc.LocalFlagNames(), rpcPluginsFlag.Name) {
			paths = append(paths, cc.StringSlice(rpcPluginsFlag.Name)...)
		}
	}
	slices.Sort(paths)
	return slices.Compact(paths)
}
