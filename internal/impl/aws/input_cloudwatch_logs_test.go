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
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestCloudWatchLogsInputConfig(t *testing.T) {
	tests := []struct {
		name        string
		config      string
		errContains string
	}{
		{
			name: "minimal config",
			config: `
log_group_name: my-app-logs
`,
		},
		{
			name: "with log stream names",
			config: `
log_group_name: my-app-logs
log_stream_names:
  - stream-1
  - stream-2
`,
		},
		{
			name: "with log stream prefix",
			config: `
log_group_name: my-app-logs
log_stream_prefix: prod-
`,
		},
		{
			name: "cannot use both stream names and prefix",
			config: `
log_group_name: my-app-logs
log_stream_names:
  - stream-1
log_stream_prefix: prod-
`,
			errContains: "cannot specify both log_stream_names and log_stream_prefix",
		},
		{
			name: "with filter pattern",
			config: `
log_group_name: my-app-logs
filter_pattern: "[ERROR]"
`,
		},
		{
			name: "with start time RFC3339",
			config: `
log_group_name: my-app-logs
start_time: "2024-01-01T00:00:00Z"
`,
		},
		{
			name: "with start time now",
			config: `
log_group_name: my-app-logs
start_time: now
`,
		},
		{
			name: "with custom poll interval",
			config: `
log_group_name: my-app-logs
poll_interval: 10s
`,
		},
		{
			name: "missing log_group_name",
			config: `
poll_interval: 5s
`,
			errContains: "log_group_name",
		},
	}

	spec := cloudWatchLogsInputSpec()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := service.NewEnvironment()
			parsedConf, err := spec.ParseYAML(tt.config, env)
			// Handle errors from ParseYAML (e.g., required fields)
			if err != nil {
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
					return
				}
				require.NoError(t, err)
			}

			// Parse the config
			conf, err := cloudWatchLogsInputConfigFromParsed(parsedConf)
			if tt.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, conf.LogGroupName)
		})
	}
}

func TestCloudWatchLogsInputConfigFromParsed(t *testing.T) {
	t.Run("parses all fields", func(t *testing.T) {
		config := `
log_group_name: my-app-logs
log_stream_names:
  - stream-1
  - stream-2
filter_pattern: "[ERROR]"
start_time: "2024-01-01T00:00:00Z"
poll_interval: 10s
`
		env := service.NewEnvironment()
		spec := cloudWatchLogsInputSpec()
		parsedConf, err := spec.ParseYAML(config, env)
		require.NoError(t, err)

		conf, err := cloudWatchLogsInputConfigFromParsed(parsedConf)
		require.NoError(t, err)

		assert.Equal(t, "my-app-logs", conf.LogGroupName)
		assert.Equal(t, []string{"stream-1", "stream-2"}, conf.LogStreamNames)
		assert.Equal(t, "[ERROR]", *conf.FilterPattern)
		assert.NotNil(t, conf.StartTime)
		expectedTime, _ := time.Parse(time.RFC3339, "2024-01-01T00:00:00Z")
		assert.Equal(t, expectedTime.Unix(), conf.StartTime.Unix())
		assert.Equal(t, 10*time.Second, conf.PollInterval)
	})

	t.Run("parses start_time as now", func(t *testing.T) {
		config := `
log_group_name: my-app-logs
start_time: now
`
		env := service.NewEnvironment()
		spec := cloudWatchLogsInputSpec()
		parsedConf, err := spec.ParseYAML(config, env)
		require.NoError(t, err)

		before := time.Now()
		conf, err := cloudWatchLogsInputConfigFromParsed(parsedConf)
		after := time.Now()

		require.NoError(t, err)
		require.NotNil(t, conf.StartTime)
		assert.True(t, conf.StartTime.After(before.Add(-time.Second)))
		assert.True(t, conf.StartTime.Before(after.Add(time.Second)))
	})

	t.Run("parses with log_stream_prefix", func(t *testing.T) {
		config := `
log_group_name: my-app-logs
log_stream_prefix: prod-
`
		env := service.NewEnvironment()
		spec := cloudWatchLogsInputSpec()
		parsedConf, err := spec.ParseYAML(config, env)
		require.NoError(t, err)

		conf, err := cloudWatchLogsInputConfigFromParsed(parsedConf)
		require.NoError(t, err)

		assert.Equal(t, "my-app-logs", conf.LogGroupName)
		require.NotNil(t, conf.LogStreamPrefix)
		assert.Equal(t, "prod-", *conf.LogStreamPrefix)
	})

	t.Run("defaults poll_interval", func(t *testing.T) {
		config := `
log_group_name: my-app-logs
`
		env := service.NewEnvironment()
		spec := cloudWatchLogsInputSpec()
		parsedConf, err := spec.ParseYAML(config, env)
		require.NoError(t, err)

		conf, err := cloudWatchLogsInputConfigFromParsed(parsedConf)
		require.NoError(t, err)

		assert.Equal(t, 5*time.Second, conf.PollInterval)
	})

	t.Run("invalid start_time format", func(t *testing.T) {
		config := `
log_group_name: my-app-logs
start_time: "not-a-timestamp"
`
		env := service.NewEnvironment()
		spec := cloudWatchLogsInputSpec()
		parsedConf, err := spec.ParseYAML(config, env)
		require.NoError(t, err)

		_, err = cloudWatchLogsInputConfigFromParsed(parsedConf)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "parsing start_time")
	})
}

// Mock CloudWatch Logs client for unit testing
type mockCloudWatchLogsClient struct {
	mu sync.Mutex

	// Captured calls
	filterLogEventsCalls   []mockFilterLogEventsCall
	describeLogGroupsCalls []mockDescribeLogGroupsCall

	// Response queues
	filterLogEventsResponses   []mockFilterLogEventsResponse
	describeLogGroupsResponses []mockDescribeLogGroupsResponse

	// Response indices
	filterLogEventsIndex   int
	describeLogGroupsIndex int
}

type mockFilterLogEventsCall struct {
	input *cloudwatchlogs.FilterLogEventsInput
}

type mockFilterLogEventsResponse struct {
	output *cloudwatchlogs.FilterLogEventsOutput
	err    error
}

type mockDescribeLogGroupsCall struct {
	input *cloudwatchlogs.DescribeLogGroupsInput
}

type mockDescribeLogGroupsResponse struct {
	output *cloudwatchlogs.DescribeLogGroupsOutput
	err    error
}

func (m *mockCloudWatchLogsClient) FilterLogEvents(_ context.Context, input *cloudwatchlogs.FilterLogEventsInput, _ ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.FilterLogEventsOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.filterLogEventsCalls = append(m.filterLogEventsCalls, mockFilterLogEventsCall{input: input})

	if m.filterLogEventsIndex >= len(m.filterLogEventsResponses) {
		return nil, errors.New("mock: no more FilterLogEvents responses configured")
	}

	resp := m.filterLogEventsResponses[m.filterLogEventsIndex]
	m.filterLogEventsIndex++
	return resp.output, resp.err
}

func (m *mockCloudWatchLogsClient) DescribeLogGroups(_ context.Context, input *cloudwatchlogs.DescribeLogGroupsInput, _ ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.DescribeLogGroupsOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.describeLogGroupsCalls = append(m.describeLogGroupsCalls, mockDescribeLogGroupsCall{input: input})

	if m.describeLogGroupsIndex >= len(m.describeLogGroupsResponses) {
		return nil, errors.New("mock: no more DescribeLogGroups responses configured")
	}

	resp := m.describeLogGroupsResponses[m.describeLogGroupsIndex]
	m.describeLogGroupsIndex++
	return resp.output, resp.err
}

func TestCloudWatchLogsInputEventToMessage(t *testing.T) {
	t.Run("structured log output", func(t *testing.T) {
		input := &cloudWatchLogsInput{
			conf: cloudWatchLogsInputConfig{
				LogGroupName:  "/aws/lambda/my-function",
				StructuredLog: true,
			},
		}

		event := types.FilteredLogEvent{
			EventId:       aws.String("event-123"),
			IngestionTime: aws.Int64(2000),
			LogStreamName: aws.String("stream-1"),
			Message:       aws.String("test message"),
			Timestamp:     aws.Int64(1000),
		}

		msg := input.eventToMessage(event)
		require.NotNil(t, msg)

		msgBytes, err := msg.AsBytes()
		require.NoError(t, err)
		assert.Contains(t, string(msgBytes), "test message")
		assert.Contains(t, string(msgBytes), "/aws/lambda/my-function")
		assert.Contains(t, string(msgBytes), "stream-1")
	})

	t.Run("plain text output with metadata", func(t *testing.T) {
		input := &cloudWatchLogsInput{
			conf: cloudWatchLogsInputConfig{
				LogGroupName:  "/aws/lambda/my-function",
				StructuredLog: false,
			},
		}

		event := types.FilteredLogEvent{
			EventId:       aws.String("event-123"),
			IngestionTime: aws.Int64(2000),
			LogStreamName: aws.String("stream-1"),
			Message:       aws.String("test message"),
			Timestamp:     aws.Int64(1000),
		}

		msg := input.eventToMessage(event)
		require.NotNil(t, msg)

		msgBytes, err := msg.AsBytes()
		require.NoError(t, err)
		assert.Equal(t, "test message", string(msgBytes))

		// Check metadata
		stream, _ := msg.MetaGet("cloudwatch_log_stream")
		assert.Equal(t, "stream-1", stream)
		group, _ := msg.MetaGet("cloudwatch_log_group")
		assert.Equal(t, "/aws/lambda/my-function", group)
		ts, _ := msg.MetaGet("cloudwatch_timestamp")
		assert.Equal(t, "1000", ts)
		ingestion, _ := msg.MetaGet("cloudwatch_ingestion_time")
		assert.Equal(t, "2000", ingestion)
		eventID, _ := msg.MetaGet("cloudwatch_event_id")
		assert.Equal(t, "event-123", eventID)
	})

	t.Run("handles nil fields", func(t *testing.T) {
		input := &cloudWatchLogsInput{
			conf: cloudWatchLogsInputConfig{
				LogGroupName:  "/aws/lambda/my-function",
				StructuredLog: false,
			},
		}

		event := types.FilteredLogEvent{
			Message: aws.String("test message"),
			// All other fields nil
		}

		msg := input.eventToMessage(event)
		require.NotNil(t, msg)

		msgBytes, err := msg.AsBytes()
		require.NoError(t, err)
		assert.Equal(t, "test message", string(msgBytes))
	})
}

func TestCloudWatchLogsInputCheckpointAdvancement(t *testing.T) {
	t.Run("advances checkpoint on events", func(t *testing.T) {
		mock := &mockCloudWatchLogsClient{
			describeLogGroupsResponses: []mockDescribeLogGroupsResponse{
				{
					output: &cloudwatchlogs.DescribeLogGroupsOutput{
						LogGroups: []types.LogGroup{
							{LogGroupName: aws.String("my-log-group")},
						},
					},
				},
			},
			filterLogEventsResponses: []mockFilterLogEventsResponse{
				{
					output: &cloudwatchlogs.FilterLogEventsOutput{
						Events: []types.FilteredLogEvent{
							{
								EventId:       aws.String("event1"),
								IngestionTime: aws.Int64(1000),
								Message:       aws.String("msg1"),
								Timestamp:     aws.Int64(1000),
							},
							{
								EventId:       aws.String("event2"),
								IngestionTime: aws.Int64(2000),
								Message:       aws.String("msg2"),
								Timestamp:     aws.Int64(2000),
							},
						},
					},
				},
			},
		}

		input := &cloudWatchLogsInput{
			conf: cloudWatchLogsInputConfig{
				LogGroupName: "my-log-group",
				PollInterval: 100 * time.Millisecond,
				APITimeout:   30 * time.Second,
			},
			log:    service.MockResources().Logger(),
			client: mock,
		}

		// Connect
		require.NoError(t, input.Connect(context.Background()))

		// Read the batch
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		batch, _, err := input.ReadBatch(ctx)
		require.NoError(t, err)
		assert.Len(t, batch, 2)

		// Checkpoint should be advanced to 2001 (last ingestion time + 1)
		assert.Equal(t, int64(2001), input.startTime)

		// Clean up
		require.NoError(t, input.Close(context.Background()))
	})

	t.Run("advances to now when no events", func(t *testing.T) {
		mock := &mockCloudWatchLogsClient{
			describeLogGroupsResponses: []mockDescribeLogGroupsResponse{
				{
					output: &cloudwatchlogs.DescribeLogGroupsOutput{
						LogGroups: []types.LogGroup{
							{LogGroupName: aws.String("my-log-group")},
						},
					},
				},
			},
			filterLogEventsResponses: []mockFilterLogEventsResponse{
				{
					output: &cloudwatchlogs.FilterLogEventsOutput{
						Events: []types.FilteredLogEvent{},
					},
				},
			},
		}

		input := &cloudWatchLogsInput{
			conf: cloudWatchLogsInputConfig{
				LogGroupName: "my-log-group",
				PollInterval: 100 * time.Millisecond,
				APITimeout:   30 * time.Second,
			},
			log:       service.MockResources().Logger(),
			client:    mock,
			startTime: 500, // Set initial checkpoint
		}

		before := time.Now()

		// Connect then close to wait for pollLoop to complete
		require.NoError(t, input.Connect(context.Background()))
		time.Sleep(150 * time.Millisecond)
		require.NoError(t, input.Close(context.Background()))

		// Checkpoint should be advanced to ~now since no events were returned
		assert.Greater(t, input.startTime, before.UnixMilli()-1000)
		assert.LessOrEqual(t, input.startTime, time.Now().UnixMilli())
	})
}

func TestCloudWatchLogsInputShutdownBehavior(t *testing.T) {
	t.Run("graceful shutdown", func(t *testing.T) {
		mock := &mockCloudWatchLogsClient{
			describeLogGroupsResponses: []mockDescribeLogGroupsResponse{
				{
					output: &cloudwatchlogs.DescribeLogGroupsOutput{
						LogGroups: []types.LogGroup{
							{LogGroupName: aws.String("my-log-group")},
						},
					},
				},
			},
			filterLogEventsResponses: []mockFilterLogEventsResponse{
				{
					output: &cloudwatchlogs.FilterLogEventsOutput{
						Events: []types.FilteredLogEvent{
							{
								EventId:       aws.String("event1"),
								IngestionTime: aws.Int64(1000),
								Message:       aws.String("msg1"),
								Timestamp:     aws.Int64(1000),
							},
						},
					},
				},
			},
		}

		input := &cloudWatchLogsInput{
			conf: cloudWatchLogsInputConfig{
				LogGroupName: "my-log-group",
				PollInterval: 50 * time.Millisecond,
				APITimeout:   30 * time.Second,
			},
			log:    service.MockResources().Logger(),
			client: mock,
		}

		// Connect
		require.NoError(t, input.Connect(context.Background()))

		// Read the batch so pollLoop isn't blocked on send
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, _, _ = input.ReadBatch(ctx)

		// Close should complete quickly
		start := time.Now()
		require.NoError(t, input.Close(context.Background()))
		duration := time.Since(start)

		// Should complete promptly
		assert.Less(t, duration, 1*time.Second, "Close should complete quickly")
	})
}

func TestCloudWatchLogsInputConnectGuard(t *testing.T) {
	t.Run("prevents duplicate goroutines on multiple Connect calls", func(t *testing.T) {
		mock := &mockCloudWatchLogsClient{
			describeLogGroupsResponses: []mockDescribeLogGroupsResponse{
				{
					output: &cloudwatchlogs.DescribeLogGroupsOutput{
						LogGroups: []types.LogGroup{
							{LogGroupName: aws.String("my-log-group")},
						},
					},
				},
			},
		}

		input := &cloudWatchLogsInput{
			conf: cloudWatchLogsInputConfig{
				LogGroupName: "my-log-group",
				PollInterval: 1 * time.Second,
				APITimeout:   30 * time.Second,
			},
			log:    service.MockResources().Logger(),
			client: mock,
		}

		// First Connect
		require.NoError(t, input.Connect(context.Background()))
		assert.NotNil(t, input.shutSig)

		// Second Connect should be no-op
		require.NoError(t, input.Connect(context.Background()))
		assert.NotNil(t, input.shutSig)

		// Clean up
		require.NoError(t, input.Close(context.Background()))
	})

	t.Run("can reconnect after close", func(t *testing.T) {
		mock := &mockCloudWatchLogsClient{
			describeLogGroupsResponses: []mockDescribeLogGroupsResponse{
				{
					output: &cloudwatchlogs.DescribeLogGroupsOutput{
						LogGroups: []types.LogGroup{
							{LogGroupName: aws.String("my-log-group")},
						},
					},
				},
				{
					output: &cloudwatchlogs.DescribeLogGroupsOutput{
						LogGroups: []types.LogGroup{
							{LogGroupName: aws.String("my-log-group")},
						},
					},
				},
			},
		}

		input := &cloudWatchLogsInput{
			conf: cloudWatchLogsInputConfig{
				LogGroupName: "my-log-group",
				PollInterval: 1 * time.Second,
				APITimeout:   30 * time.Second,
			},
			log:    service.MockResources().Logger(),
			client: mock,
		}

		// Connect, close, then reconnect
		require.NoError(t, input.Connect(context.Background()))
		require.NoError(t, input.Close(context.Background()))
		require.NoError(t, input.Connect(context.Background()))

		assert.NotNil(t, input.shutSig)
		assert.NotNil(t, input.msgChan)

		// Clean up
		require.NoError(t, input.Close(context.Background()))
	})
}
