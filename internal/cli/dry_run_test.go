// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package cli

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"

	"github.com/redpanda-data/connect/v4/public/schema"
)

func TestDryRunRPCPlugins(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping test in CI: the plugin starts with go run")
	}

	const pluginPath = "../rpcplugin/testdata/catshout/plugin.yaml"

	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(cfgPath, []byte(`
input:
  generate:
    count: 1
    mapping: root = "meow"
pipeline:
  processors:
    - catshout:
        suffix: "!"
output:
  drop: {}
`), 0o644))

	tests := []struct {
		name string
		args []string
	}{
		{
			name: "flag before dry-run",
			args: []string{"connect", "--rpc-plugins", pluginPath, "dry-run", cfgPath},
		},
		{
			name: "flag after dry-run",
			args: []string{"connect", "dry-run", "--rpc-plugins", pluginPath, cfgPath},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := &cli.App{
				Flags: []cli.Flag{
					&cli.StringSliceFlag{Name: "rpc-plugins"},
				},
				Commands: []*cli.Command{
					dryRunCli(schema.Standard("", "")),
				},
			}
			require.NoError(t, app.RunContext(t.Context(), test.args))
		})
	}
}
