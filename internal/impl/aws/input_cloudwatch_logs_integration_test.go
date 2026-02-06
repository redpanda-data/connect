// Copyright 2026 Redpanda Data, Inc.
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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service/integration"

	_ "github.com/redpanda-data/connect/v4/public/components/pure"
)

// createLogGroupWithEvents creates a CloudWatch Log Group with a log stream and test events
func createLogGroupWithEvents(ctx context.Context, t testing.TB, cwlPort, logGroupName string, numEvents int) error {
	endpoint := fmt.Sprintf("http://localhost:%v", cwlPort)

	conf, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
		config.WithRegion("us-east-1"),
	)
	require.NoError(t, err)

	conf.BaseEndpoint = &endpoint
	client := cloudwatchlogs.NewFromConfig(conf)

	// Create log group
	t.Logf("Creating log group: %v", logGroupName)
	_, err = client.CreateLogGroup(ctx, &cloudwatchlogs.CreateLogGroupInput{
		LogGroupName: aws.String(logGroupName),
	})
	if err != nil {
		// Check if already exists
		var alreadyExists *types.ResourceAlreadyExistsException
		if !errors.As(err, &alreadyExists) {
			return fmt.Errorf("creating log group: %w", err)
		}
	}

	// Create log stream
	streamName := "test-stream"
	t.Logf("Creating log stream: %v", streamName)
	_, err = client.CreateLogStream(ctx, &cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  aws.String(logGroupName),
		LogStreamName: aws.String(streamName),
	})
	if err != nil {
		var alreadyExists *types.ResourceAlreadyExistsException
		if !errors.As(err, &alreadyExists) {
			return fmt.Errorf("creating log stream: %w", err)
		}
	}

	// Put log events
	if numEvents > 0 {
		events := make([]types.InputLogEvent, numEvents)
		baseTime := time.Now().Add(-1 * time.Hour).UnixMilli()
		for i := 0; i < numEvents; i++ {
			events[i] = types.InputLogEvent{
				Message:   aws.String(fmt.Sprintf("test message %d", i)),
				Timestamp: aws.Int64(baseTime + int64(i*1000)),
			}
		}

		t.Logf("Putting %d log events", numEvents)
		_, err = client.PutLogEvents(ctx, &cloudwatchlogs.PutLogEventsInput{
			LogGroupName:  aws.String(logGroupName),
			LogStreamName: aws.String(streamName),
			LogEvents:     events,
		})
		if err != nil {
			return fmt.Errorf("putting log events: %w", err)
		}
	}

	return nil
}

func cloudWatchLogsIntegrationSuite(t *testing.T, lsPort string) {
	t.Run("basic_consumption", func(t *testing.T) {
		logGroupName := "test-log-group-" + t.Name()
		ctx := context.Background()

		// Create log group with events
		require.NoError(t, createLogGroupWithEvents(ctx, t, lsPort, logGroupName, 10))

		// Give LocalStack a moment to process
		time.Sleep(500 * time.Millisecond)

		template := fmt.Sprintf(`
input:
  aws_cloudwatch_logs:
    log_group_name: %s
    endpoint: http://localhost:$PORT
    region: us-east-1
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
    poll_interval: 1s
    structured_log: false

output:
  drop: {}
`, logGroupName)

		integration.StreamTests(
			integration.StreamTestOpenClose(),
		).Run(
			t, template,
			integration.StreamTestOptPort(lsPort),
		)
	})

	t.Run("with_filter_pattern", func(t *testing.T) {
		logGroupName := "test-log-group-filter-" + t.Name()
		ctx := context.Background()

		// Create log group and stream
		endpoint := fmt.Sprintf("http://localhost:%v", lsPort)
		conf, err := config.LoadDefaultConfig(ctx,
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
			config.WithRegion("us-east-1"),
		)
		require.NoError(t, err)

		conf.BaseEndpoint = &endpoint
		client := cloudwatchlogs.NewFromConfig(conf)

		_, err = client.CreateLogGroup(ctx, &cloudwatchlogs.CreateLogGroupInput{
			LogGroupName: aws.String(logGroupName),
		})
		require.NoError(t, err)

		streamName := "test-stream"
		_, err = client.CreateLogStream(ctx, &cloudwatchlogs.CreateLogStreamInput{
			LogGroupName:  aws.String(logGroupName),
			LogStreamName: aws.String(streamName),
		})
		require.NoError(t, err)

		// Put events with different log levels
		baseTime := time.Now().Add(-1 * time.Hour).UnixMilli()
		events := []types.InputLogEvent{
			{Message: aws.String("[ERROR] error message 1"), Timestamp: aws.Int64(baseTime)},
			{Message: aws.String("[INFO] info message 1"), Timestamp: aws.Int64(baseTime + 1000)},
			{Message: aws.String("[ERROR] error message 2"), Timestamp: aws.Int64(baseTime + 2000)},
			{Message: aws.String("[INFO] info message 2"), Timestamp: aws.Int64(baseTime + 3000)},
		}

		_, err = client.PutLogEvents(ctx, &cloudwatchlogs.PutLogEventsInput{
			LogGroupName:  aws.String(logGroupName),
			LogStreamName: aws.String(streamName),
			LogEvents:     events,
		})
		require.NoError(t, err)

		time.Sleep(500 * time.Millisecond)

		template := fmt.Sprintf(`
input:
  aws_cloudwatch_logs:
    log_group_name: %s
    filter_pattern: "[ERROR]"
    endpoint: http://localhost:$PORT
    region: us-east-1
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
    poll_interval: 1s
    structured_log: false

output:
  drop: {}
`, logGroupName)

		integration.StreamTests(
			integration.StreamTestOpenClose(),
		).Run(
			t, template,
			integration.StreamTestOptPort(lsPort),
		)
	})

	t.Run("structured_log_output", func(t *testing.T) {
		logGroupName := "test-log-group-structured-" + t.Name()
		ctx := context.Background()

		require.NoError(t, createLogGroupWithEvents(ctx, t, lsPort, logGroupName, 5))
		time.Sleep(500 * time.Millisecond)

		template := fmt.Sprintf(`
input:
  aws_cloudwatch_logs:
    log_group_name: %s
    endpoint: http://localhost:$PORT
    region: us-east-1
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
    poll_interval: 1s
    structured_log: true

output:
  drop: {}
`, logGroupName)

		integration.StreamTests(
			integration.StreamTestOpenClose(),
		).Run(
			t, template,
			integration.StreamTestOptPort(lsPort),
		)
	})
}
