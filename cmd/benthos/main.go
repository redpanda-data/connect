package main

import (
	"github.com/benthosdev/benthos/v4/internal/cli"

	// Import all plugins defined within the repo.
	_ "github.com/benthosdev/benthos/v4/public/components/all"
)

func main() {
	cli.Run()
}
