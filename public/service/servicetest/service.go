// Package servicetest provides functions and utilities that might be useful for
// testing custom Benthos builds.
package servicetest

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/cli"
)

// RunCLIWithArgs executes Benthos as a CLI with an explicit set of arguments.
// This is useful for testing commands without needing to modify os.Args.
//
// This call blocks until either:
//
// 1. The service shuts down gracefully due to the inputs closing
// 2. A termination signal is received
// 3. The provided context has a deadline that is reached, triggering graceful termination
// 4. The provided context is cancelled (WARNING, this prevents graceful termination)
func RunCLIWithArgs(ctx context.Context, args ...string) {
	_ = cli.App().RunContext(ctx, args)
}
