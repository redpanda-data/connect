package main

import (
	"github.com/redpanda-data/connect/v4/internal/impl/aws"

	// Import all plugins defined within the repo.
	_ "github.com/redpanda-data/connect/v4/public/components/all"
)

func main() {
	aws.RunLambda()
}
