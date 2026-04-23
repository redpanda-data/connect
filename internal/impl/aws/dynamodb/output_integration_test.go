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

package dynamodb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationDynamoDBOutput(t *testing.T) {
	integration.CheckSkip(t)

	ctr, err := testcontainers.Run(t.Context(), "amazon/dynamodb-local:latest",
		testcontainers.WithExposedPorts("8000/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("8000/tcp").WithStartupTimeout(30*time.Second),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mp, err := ctr.MappedPort(t.Context(), "8000/tcp")
	require.NoError(t, err)
	dynamoPort := mp.Port()

	const tableName = "output_batch_table"
	require.Eventually(t, func() bool {
		return createTable(t.Context(), t, dynamoPort, tableName) == nil
	}, 30*time.Second, time.Second)

	endpoint := fmt.Sprintf("http://localhost:%s", dynamoPort)

	pConf, err := ddboOutputSpec().ParseYAML(fmt.Sprintf(`
table: %s
string_columns:
  id: ${! json("id") }
  content: ${! json("content") }
region: us-east-1
endpoint: %s
credentials:
  id: xxxxx
  secret: xxxxx
  token: xxxxx
`, tableName, endpoint), nil)
	require.NoError(t, err)

	dConf, err := ddboConfigFromParsed(pConf)
	require.NoError(t, err)

	writer, err := newDynamoDBWriter(dConf, service.MockResources())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, writer.Close(context.Background()))
	})

	require.NoError(t, writer.Connect(t.Context()))

	const numMessages = 60
	batch := make(service.MessageBatch, numMessages)
	for i := range numMessages {
		batch[i] = service.NewMessage(fmt.Appendf(nil, `{"id":"id-%d","content":"content-%d"}`, i, i))
	}

	require.NoError(t, writer.WriteBatch(t.Context(), batch))

	awsCfg, err := config.LoadDefaultConfig(t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
	)
	require.NoError(t, err)
	awsCfg.BaseEndpoint = &endpoint
	client := dynamodb.NewFromConfig(awsCfg)

	seenIDs := make(map[string]bool, numMessages)
	var lastKey map[string]types.AttributeValue
	for {
		scanOut, err := client.Scan(t.Context(), &dynamodb.ScanInput{
			TableName:         aws.String(tableName),
			ExclusiveStartKey: lastKey,
		})
		require.NoError(t, err)
		for _, item := range scanOut.Items {
			idAttr, ok := item["id"].(*types.AttributeValueMemberS)
			require.True(t, ok, "item missing string 'id' attribute")
			seenIDs[idAttr.Value] = true
		}
		if len(scanOut.LastEvaluatedKey) == 0 {
			break
		}
		lastKey = scanOut.LastEvaluatedKey
	}

	assert.Len(t, seenIDs, numMessages, "expected all %d items to be written", numMessages)
	for i := range numMessages {
		expectedID := fmt.Sprintf("id-%d", i)
		assert.True(t, seenIDs[expectedID], "expected item %q to be present in table", expectedID)
	}
}
