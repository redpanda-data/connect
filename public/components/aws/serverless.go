package aws

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/serverless/lambda"
)

// RunLambda executes Benthos as an AWS Lambda function. Configuration can be
// stored within the environment variable BENTHOS_CONFIG.
func RunLambda(ctx context.Context) {
	lambda.Run()
}
