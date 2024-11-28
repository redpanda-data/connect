// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aws

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service/integration"

	_ "github.com/redpanda-data/connect/v4/public/components/pure"
)

func sqsIntegrationSuite(t *testing.T, lsPort string) {
	template := `
output:
  aws_sqs:
    url: http://localhost:$PORT/000000000000/queue-$ID
    endpoint: http://localhost:$PORT
    region: eu-west-1
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
    max_in_flight: $MAX_IN_FLIGHT
    batching:
      count: $OUTPUT_BATCH_COUNT

input:
  aws_sqs:
    url: http://localhost:$PORT/000000000000/queue-$ID
    endpoint: http://localhost:$PORT
    region: eu-west-1
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
`
	integration.StreamTests(
		integration.StreamTestOpenClose(),
		integration.StreamTestSendBatch(10),
		integration.StreamTestStreamSequential(50),
		integration.StreamTestStreamParallel(50),
		integration.StreamTestStreamParallelLossy(50),
		integration.StreamTestStreamParallelLossyThroughReconnect(50),
	).Run(
		t, template,
		integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
			require.NoError(t, createBucketQueue(ctx, "", lsPort, vars.ID))
		}),
		integration.StreamTestOptPort(lsPort),
	)

	t.Run("batch_limited", func(t *testing.T) {
		template := `
output:
  aws_sqs:
    url: http://localhost:$PORT/000000000000/queue-$ID
    endpoint: http://localhost:$PORT
    region: eu-west-1
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
    max_in_flight: $MAX_IN_FLIGHT
    batching:
      count: $OUTPUT_BATCH_COUNT
    max_records_per_request: 1

input:
  aws_sqs:
    url: http://localhost:$PORT/000000000000/queue-$ID
    endpoint: http://localhost:$PORT
    region: eu-west-1
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
`
		integration.StreamTests(
			integration.StreamTestOpenClose(),
			integration.StreamTestSendBatch(10),
			integration.StreamTestStreamSequential(50),
			integration.StreamTestStreamParallel(50),
			integration.StreamTestStreamParallelLossy(50),
			integration.StreamTestStreamParallelLossyThroughReconnect(50),
		).Run(
			t, template,
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				require.NoError(t, createBucketQueue(ctx, "", lsPort, vars.ID))
			}),
			integration.StreamTestOptPort(lsPort),
		)
	})

}
