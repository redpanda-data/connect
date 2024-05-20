package main

import (
	"context"

	"github.com/benthosdev/benthos/v4/public/service"

	// Import all plugins defined within the repo.
	_ "github.com/benthosdev/benthos/v4/public/components/all"
)

func main() {
	service.RunCLI(context.Background())
}
