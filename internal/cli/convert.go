// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package cli

import (
	"fmt"
	"io"
	"os"

	"github.com/urfave/cli/v2"

	"github.com/redpanda-data/connect/v4/internal/cli/convertserver"
	connectconverter "github.com/redpanda-data/connect/v4/internal/connect_converter"
)

func convertCli() *cli.Command {
	return &cli.Command{
		Name:      "convert",
		Usage:     "Convert a Kafka Connect connector config into a Redpanda Connect config",
		ArgsUsage: "[path]",
		Description: `
Reads a Kafka Connect connector configuration (REST-wrapped or flat JSON) from a
file argument or stdin and prints an equivalent Redpanda Connect pipeline YAML.
Sections that could not be fully mapped are annotated with # TODO markers.

  {{.BinaryName}} convert ./s3-sink.json -o ./s3-sink.yaml
  cat ./connector.json | {{.BinaryName}} convert`[1:],
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
				Usage:   "Write the converted config to a file instead of stdout.",
			},
		},
		Subcommands: []*cli.Command{convertserver.Command()},
		Action: func(c *cli.Context) error {
			var (
				input []byte
				err   error
			)
			if path := c.Args().First(); path != "" {
				if input, err = os.ReadFile(path); err != nil {
					return fmt.Errorf("read input: %w", err)
				}
			} else {
				if input, err = io.ReadAll(c.App.Reader); err != nil {
					return fmt.Errorf("read stdin: %w", err)
				}
			}

			res, err := connectconverter.Convert(input)
			if err != nil {
				return err
			}

			if out := c.String("output"); out != "" {
				if err := os.WriteFile(out, res.YAML, 0o644); err != nil {
					return fmt.Errorf("write output: %w", err)
				}
			} else {
				if _, err := c.App.Writer.Write(res.YAML); err != nil {
					return err
				}
			}

			if len(res.Warnings) > 0 {
				fmt.Fprintf(c.App.ErrWriter, "conversion completed with %d warning(s); review # TODO markers\n", len(res.Warnings))
			}
			return nil
		},
	}
}
