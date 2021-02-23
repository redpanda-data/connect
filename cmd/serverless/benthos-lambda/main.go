package main

import (
	"github.com/Jeffail/benthos/v3/lib/serverless/lambda"

	_ "github.com/Jeffail/benthos/v3/public/components/all"
)

//------------------------------------------------------------------------------

func main() {
	lambda.Run()
}

//------------------------------------------------------------------------------
