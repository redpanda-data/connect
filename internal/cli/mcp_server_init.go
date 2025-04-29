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
	"fmt"
	"os"
	"path/filepath"

	"github.com/urfave/cli/v2"

	"github.com/redpanda-data/connect/v4/internal/impl/kafka/enterprise"
)

func mcpServerInitCli(rpMgr *enterprise.GlobalRedpandaManager) *cli.Command {
	flags := []cli.Flag{}

	return &cli.Command{
		Name:  "init",
		Usage: "Create the basic folder structure of an MCP server.",
		Flags: flags,
		Description: `
!!EXPERIMENTAL!!

Files that already exist will not be overwritten.
  `[1:],
		Action: func(c *cli.Context) error {
			repositoryDir := "."
			if c.Args().Len() > 0 {
				if c.Args().Len() > 1 {
					return errors.New("a maximum of one repository directory must be specified with this command")
				}
				repositoryDir = c.Args().First()
			}

			for k, v := range initStructure {
				fpath := filepath.Join(repositoryDir, k)
				if _, err := os.Stat(fpath); err == nil {
					// File already exists, carry on
					continue
				}

				folderPath := filepath.Dir(fpath)
				if err := os.MkdirAll(folderPath, 0o755); err != nil {
					return fmt.Errorf("failed to create folder %v: %w", folderPath, err)
				}

				if err := os.WriteFile(fpath, []byte(v), 0o644); err != nil {
					return fmt.Errorf("failed to write file %v: %w", fpath, err)
				}
			}
			return nil
		},
	}
}

var initStructure = map[string]string{
	"o11y/metrics.yaml": `prometheus: {}
`,
	"o11y/tracer.yaml": `open_telemetry_collector:
  service: rpcn-mcp
  grpc: []
  http: []
`,
	"resources/caches/example-cache.yaml": `label: example-cache
memory: {}
meta:
  tags: [ example ]
  mcp:
    enabled: true
    description: An example cache for saving information.
`,
	"resources/processors/example-processor.yaml": `label: example-processor
try:
  - mapping: 'root = content().uppercase()' 
meta:
  tags: [ example ]
  mcp:
    enabled: true
    description: An example processor that uppercases text.
`,
	"resources/outputs/example-output.yaml": `label: example-output
file:
  path: "/tmp/${! uuid_v4() }.txt"
meta:
  tags: [ example ]
  mcp:
    enabled: true
    description: An example output that writes data to a temporary folder.
`,
	"resources/inputs/example-input.yaml": `label: example-input
generate:
  interval: 1s
  mapping: |
    root.id = uuid_v4()
    root.name = fake("name")
    root.email = fake("email")
    root.message = fake("paragraph")
meta:
  tags: [ example ]
  mcp:
    enabled: true
    description: An example input that generates JSON messages.
`,
}
