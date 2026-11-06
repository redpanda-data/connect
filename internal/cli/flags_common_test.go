// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package cli

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
)

func TestRPCPluginPaths(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want []string
	}{
		{
			name: "root only",
			args: []string{"connect", "--rpc-plugins", "a.yaml", "run"},
			want: []string{"a.yaml"},
		},
		{
			name: "command only",
			args: []string{"connect", "run", "--rpc-plugins", "b.yaml"},
			want: []string{"b.yaml"},
		},
		{
			name: "both",
			args: []string{"connect", "--rpc-plugins", "a.yaml", "run", "--rpc-plugins", "b.yaml"},
			want: []string{"b.yaml", "a.yaml"},
		},
		{
			name: "none",
			args: []string{"connect", "run"},
			want: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var got []string
			app := &cli.App{
				Flags: []cli.Flag{
					&cli.StringSliceFlag{Name: "rpc-plugins"},
				},
				Commands: []*cli.Command{
					{
						Name: "run",
						Flags: []cli.Flag{
							&cli.StringSliceFlag{Name: "rpc-plugins"},
						},
						Action: func(c *cli.Context) error {
							got = rpcPluginPaths(c)
							return nil
						},
					},
				},
			}
			require.NoError(t, app.RunContext(t.Context(), test.args))
			assert.Equal(t, test.want, got)
		})
	}
}
