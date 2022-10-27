package main

import (
	"github.com/benthosdev/benthos/v4/internal/cli"

	// Import all plugins defined within the repo.
	_ "github.com/benthosdev/benthos/v4/public/components/all"
	_ "github.com/benthosdev/benthos/v4/public/components/pulsar"
)

func main() {
	cli.Run()
}
