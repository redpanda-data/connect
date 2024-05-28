package template

import (
	"github.com/urfave/cli/v2"

	"github.com/benthosdev/benthos/v4/internal/cli/common"
)

// CliCommand is a cli.Command definition for interacting with templates.
func CliCommand(opts *common.CLIOpts) *cli.Command {
	return &cli.Command{
		Name:  "template",
		Usage: opts.ExecTemplate("Interact and generate {{.ProductName}} templates"),
		Description: opts.ExecTemplate(`
EXPERIMENTAL: This subcommand, and templates in general, are experimental and
therefore are subject to change outside of major version releases.

Allows linting and generating {{.ProductName}} templates.

  {{.BinaryName}} template lint ./path/to/templates/...

For more information check out the docs at:
{{.DocumentationURL}}/configuration/templating`)[1:],
		Subcommands: []*cli.Command{
			lintCliCommand(opts),
		},
	}
}
