package service

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/cli"
)

// RunCLI executes Benthos as a CLI, allowing users to specify a configuration
// file path(s) and execute subcommands for linting configs, testing configs,
// etc. This is how a standard distribution of Benthos operates.
//
// This call blocks until either:
//
// 1. The service shuts down gracefully due to the inputs closing
// 2. A termination signal is received
// 3. The provided context is cancelled
//
// This function must only be called once during the entire lifecycle of your
// program, as it interacts with singleton state. In order to manage multiple
// Benthos stream lifecycles in a program use the StreamBuilder API instead.
func RunCLI(ctx context.Context) {
	cli.RunWithOpts(cli.OptUseContext(ctx))
}
