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

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/urfave/cli/v2"
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
		return fmt.Errorf("failed to resolve env file glob pattern: %w", err)
	}
	for _, dotEnvFile := range dotEnvPaths {
		dotEnvBytes, err := service.ReadFile(service.OSFS(), dotEnvFile)
		if err != nil {
			return fmt.Errorf("failed to read dotenv file: %w", err)
		}
		vars, err := service.ParseEnvFile(dotEnvBytes)
		if err != nil {
			return fmt.Errorf("failed to parse dotenv file: %w", err)
		}
		for k, v := range vars {
			if err = os.Setenv(k, v); err != nil {
				return fmt.Errorf("failed to set env var '%v': %w", k, err)
			}
		}
	}
	return nil
}
