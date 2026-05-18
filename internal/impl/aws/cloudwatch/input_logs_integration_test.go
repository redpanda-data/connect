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

package cloudwatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	_ "github.com/redpanda-data/connect/v4/public/components/pure"

	"github.com/redpanda-data/connect/v4/internal/impl/aws/awstest"
)

func TestIntegrationCloudWatch(t *testing.T) {
	integration.CheckSkip(t)

	servicePort := awstest.GetLocalStack(t)
	cloudWatchLogsIntegrationSuite(t, servicePort)
}

// createLogGroupWithEvents creates a CloudWatch Log Group with a log stream and test events.
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
		for i := range numEvents {
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

// newTestCWLClient creates a CloudWatch Logs client pointed at the localstack endpoint.
func newTestCWLClient(t testing.TB, cwlPort string) cloudWatchLogsAPI {
	t.Helper()
	endpoint := fmt.Sprintf("http://localhost:%v", cwlPort)

	conf, err := config.LoadDefaultConfig(context.Background(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
		config.WithRegion("us-east-1"),
	)
	require.NoError(t, err)

	conf.BaseEndpoint = &endpoint
	return cloudwatchlogs.NewFromConfig(conf)
}

// collectMessages reads batches from the input until at least wantCount messages
// are collected or the context expires.
func collectMessages(t testing.TB, input *cloudWatchLogsInput, wantCount int, timeout time.Duration) []*service.Message {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var all []*service.Message
	for len(all) < wantCount {
		batch, _, err := input.ReadBatch(ctx)
		if err != nil {
			break
		}
		all = append(all, batch...)
	}
	return all
}

func cloudWatchLogsIntegrationSuite(t *testing.T, lsPort string) {
	t.Run("basic_consumption", func(t *testing.T) {
		logGroupName := "test-log-group-" + t.Name()
		ctx := context.Background()

		// Create log group with events
		require.NoError(t, createLogGroupWithEvents(ctx, t, lsPort, logGroupName, 10))
		time.Sleep(500 * time.Millisecond)

		input := &cloudWatchLogsInput{
			conf: cloudWatchLogsInputConfig{
				LogGroupName:  logGroupName,
				PollInterval:  1 * time.Second,
				Limit:         1000,
				StructuredLog: false,
				APITimeout:    30 * time.Second,
			},
			log:    service.MockResources().Logger(),
			client: newTestCWLClient(t, lsPort),
		}

		require.NoError(t, input.Connect(ctx))
		t.Cleanup(func() { _ = input.Close(ctx) })

		msgs := collectMessages(t, input, 10, 30*time.Second)
		require.Len(t, msgs, 10)
	})

	t.Run("with_filter_pattern", func(t *testing.T) {
		logGroupName := "test-log-group-filter-" + t.Name()
		ctx := context.Background()

		// Create log group and stream with mixed log levels
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

		filterPattern := "[ERROR]"
		input := &cloudWatchLogsInput{
			conf: cloudWatchLogsInputConfig{
				LogGroupName:  logGroupName,
				FilterPattern: &filterPattern,
				PollInterval:  1 * time.Second,
				Limit:         1000,
				StructuredLog: false,
				APITimeout:    30 * time.Second,
			},
			log:    service.MockResources().Logger(),
			client: newTestCWLClient(t, lsPort),
		}

		require.NoError(t, input.Connect(ctx))
		t.Cleanup(func() { _ = input.Close(ctx) })

		// LocalStack may not support filter_pattern, so accept 2..4 messages
		msgs := collectMessages(t, input, 2, 30*time.Second)
		assert.GreaterOrEqual(t, len(msgs), 2)
		assert.LessOrEqual(t, len(msgs), 4)
	})

	t.Run("structured_log_output", func(t *testing.T) {
		logGroupName := "test-log-group-structured-" + t.Name()
		ctx := context.Background()

		require.NoError(t, createLogGroupWithEvents(ctx, t, lsPort, logGroupName, 5))
		time.Sleep(500 * time.Millisecond)

		input := &cloudWatchLogsInput{
			conf: cloudWatchLogsInputConfig{
				LogGroupName:  logGroupName,
				PollInterval:  1 * time.Second,
				Limit:         1000,
				StructuredLog: true,
				APITimeout:    30 * time.Second,
			},
			log:    service.MockResources().Logger(),
			client: newTestCWLClient(t, lsPort),
		}

		require.NoError(t, input.Connect(ctx))
		t.Cleanup(func() { _ = input.Close(ctx) })

		msgs := collectMessages(t, input, 5, 30*time.Second)
		require.Len(t, msgs, 5)

		// Verify structured JSON output
		for _, msg := range msgs {
			raw, err := msg.AsBytes()
			require.NoError(t, err)

			var obj map[string]any
			require.NoError(t, json.Unmarshal(raw, &obj), "message should be valid JSON: %s", string(raw))
			assert.Contains(t, obj, "message")
			assert.Contains(t, obj, "log_group")
			assert.Contains(t, obj, "timestamp")
			assert.Equal(t, logGroupName, obj["log_group"])
		}
	})
}
