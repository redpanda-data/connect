package service

import (
	"github.com/Jeffail/benthos/v3/lib/service"
)

// RunCLI executes Benthos as a CLI, allowing users to specify a configuration
// file path(s) and execute subcommands for linting configs, testing configs,
// etc. This is how a standard distribution of Benthos operates.
//
// This call blocks until either the service shuts down or a termination signal
// is received.
func RunCLI() {
	service.Run()
}
