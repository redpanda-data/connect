package main

import (
	"github.com/Jeffail/benthos/v3/lib/serverless/lambda"

	// Import all plugins defined within the repo.
	_ "github.com/Jeffail/benthos/v3/public/components/all"
)

func main() {
	lambda.Run()
}
