package studio

import (
	"github.com/urfave/cli/v2"
)

// CliCommand is a cli.Command definition for interacting with Benthos studio.
func CliCommand(version, dateBuilt string) *cli.Command {
	return &cli.Command{
		Name:  "studio",
		Usage: "Interact with Benthos studio (https://studio.benthos.dev)",
		Description: `
EXPERIMENTAL: This subcommand is experimental and therefore are subject to
change outside of major version releases.`[1:],
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "endpoint",
				Aliases: []string{"e"},
				Value:   "https://studio.benthos.dev",
				Usage:   "Specify the URL of the Benthos studio server to connect to.",
			},
		},
		Subcommands: []*cli.Command{
			syncSchemaCommand(version, dateBuilt),
			pullCommand(version, dateBuilt),
		},
	}
}
