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
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/shutdown"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/aws/config"
)

const (
	cwlFieldLogGroupName    = "log_group_name"
	cwlFieldLogStreamNames  = "log_stream_names"
	cwlFieldLogStreamPrefix = "log_stream_prefix"
	cwlFieldFilterPattern   = "filter_pattern"
	cwlFieldStartTime       = "start_time"
	cwlFieldPollInterval    = "poll_interval"
	cwlFieldLimit           = "limit"
	cwlFieldStructuredLog   = "structured_log"
	cwlFieldAPITimeout      = "api_timeout"
)

func cloudWatchLogsInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Version("4.81.0").
		Categories("Services", "AWS").
		Summary("Consumes log events from AWS CloudWatch Logs.").
		Description(`
Polls CloudWatch Log Groups for log events. Supports filtering by log streams, CloudWatch filter patterns, and configurable start times.

Each log event becomes a separate message with metadata including the log group name, log stream name, timestamp, and ingestion time.

IMPORTANT: This input tracks its position in memory only. If the process restarts, it will resume from the configured start_time (or the beginning if not set). For exactly-once processing, you should configure an appropriate start_time or implement idempotent downstream processing.

## Credentials

By default Redpanda Connect will use a shared credentials file when connecting to AWS services. It's also possible to set them explicitly at the component level, allowing you to transfer data across accounts. You can find out more in xref:guides:cloud/aws.adoc[].

## Metadata

This input adds the following metadata fields to each message:

- `+"`cloudwatch_log_group`"+` - The name of the log group
- `+"`cloudwatch_log_stream`"+` - The name of the log stream
- `+"`cloudwatch_timestamp`"+` - The timestamp of the log event (Unix milliseconds)
- `+"`cloudwatch_ingestion_time`"+` - The ingestion timestamp (Unix milliseconds)
- `+"`cloudwatch_event_id`"+` - The unique event ID

You can access these metadata fields using xref:guides:bloblang/about.adoc[Bloblang].
`).
		Fields(
			service.NewStringField(cwlFieldLogGroupName).
				Description("The name of the CloudWatch Log Group to consume from.").
				Example("my-app-logs"),
			service.NewStringListField(cwlFieldLogStreamNames).
				Description("An optional list of log stream names to consume from. If not set, events from all streams in the log group will be consumed.").
				Optional().
				Example([]string{"stream-1", "stream-2"}),
			service.NewStringField(cwlFieldLogStreamPrefix).
				Description("An optional log stream name prefix to filter streams. Only streams starting with this prefix will be consumed.").
				Optional().
				Example("prod-"),
			service.NewStringField(cwlFieldFilterPattern).
				Description("An optional CloudWatch Logs filter pattern to apply when querying log events. See AWS documentation for filter pattern syntax.").
				Optional().
				Example("[ERROR]"),
			service.NewStringField(cwlFieldStartTime).
				Description("The time to start consuming log events from. Can be an RFC3339 timestamp (e.g., `2024-01-01T00:00:00Z`) or the string `now` to start consuming from the current time. If not set, starts from the beginning of available logs.").
				Optional().
				Example("2024-01-01T00:00:00Z").
				Example("now"),
			service.NewDurationField(cwlFieldPollInterval).
				Description("The interval at which to poll for new log events.").
				Default("5s"),
			service.NewIntField(cwlFieldLimit).
				Description("The maximum number of log events to return in a single API call. Valid range: 1-10000.").
				Default(1000).
				LintRule(`root = if this < 1 || this > 10000 { ["limit must be between 1 and 10000"] }`).
				Advanced(),
			service.NewBoolField(cwlFieldStructuredLog).
				Description("Whether to output log events as structured JSON objects with all metadata fields, or as plain text messages with metadata in message metadata.").
				Default(true).
				Advanced(),
			service.NewDurationField(cwlFieldAPITimeout).
				Description("The maximum time to wait for an API request to complete.").
				Default("30s").
				Advanced(),
			service.NewAutoRetryNacksToggleField(),
		).
		Fields(config.SessionFields()...).
		LintRule(`
root = if this.log_stream_names.or([]).length() > 0 && this.exists("log_stream_prefix") {
  "cannot specify both log_stream_names and log_stream_prefix"
}
`)
}

func init() {
	service.MustRegisterBatchInput("aws_cloudwatch_logs", cloudWatchLogsInputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			i, err := newCloudWatchLogsInputFromConfig(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksBatchedToggled(conf, i)
		})
}

// cloudWatchLogsAPI defines the CloudWatch Logs API operations used by this input.
type cloudWatchLogsAPI interface {
	FilterLogEvents(ctx context.Context, input *cloudwatchlogs.FilterLogEventsInput, opts ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.FilterLogEventsOutput, error)
	DescribeLogGroups(ctx context.Context, input *cloudwatchlogs.DescribeLogGroupsInput, opts ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.DescribeLogGroupsOutput, error)
}

type cloudWatchLogsInputConfig struct {
	LogGroupName    string
	LogStreamNames  []string
	LogStreamPrefix *string
	FilterPattern   *string
	StartTime       *time.Time
	PollInterval    time.Duration
	Limit           int
	StructuredLog   bool
	APITimeout      time.Duration
}

func cloudWatchLogsInputConfigFromParsed(pConf *service.ParsedConfig) (conf cloudWatchLogsInputConfig, err error) {
	if conf.LogGroupName, err = pConf.FieldString(cwlFieldLogGroupName); err != nil {
		return
	}

	if pConf.Contains(cwlFieldLogStreamNames) {
		if conf.LogStreamNames, err = pConf.FieldStringList(cwlFieldLogStreamNames); err != nil {
			return
		}
	}

	if pConf.Contains(cwlFieldLogStreamPrefix) {
		var prefix string
		if prefix, err = pConf.FieldString(cwlFieldLogStreamPrefix); err != nil {
			return
		}
		conf.LogStreamPrefix = &prefix
	}

	if pConf.Contains(cwlFieldFilterPattern) {
		var pattern string
		if pattern, err = pConf.FieldString(cwlFieldFilterPattern); err != nil {
			return
		}
		conf.FilterPattern = &pattern
	}

	if pConf.Contains(cwlFieldStartTime) {
		var startTimeStr string
		if startTimeStr, err = pConf.FieldString(cwlFieldStartTime); err != nil {
			return
		}
		startTimeStr = strings.TrimSpace(startTimeStr)
		if startTimeStr == "now" {
			now := time.Now()
			conf.StartTime = &now
		} else {
			var parsedTime time.Time
			if parsedTime, err = time.Parse(time.RFC3339, startTimeStr); err != nil {
				return conf, fmt.Errorf("parsing start_time: %w", err)
			}
			conf.StartTime = &parsedTime
		}
	}

	if conf.PollInterval, err = pConf.FieldDuration(cwlFieldPollInterval); err != nil {
		return
	}

	if conf.Limit, err = pConf.FieldInt(cwlFieldLimit); err != nil {
		return
	}

	if conf.StructuredLog, err = pConf.FieldBool(cwlFieldStructuredLog); err != nil {
		return
	}

	if conf.APITimeout, err = pConf.FieldDuration(cwlFieldAPITimeout); err != nil {
		return
	}

	// Validate mutual exclusion
	if len(conf.LogStreamNames) > 0 && conf.LogStreamPrefix != nil {
		return conf, errors.New("cannot specify both log_stream_names and log_stream_prefix")
	}

	// Validate limit range
	if conf.Limit < 1 || conf.Limit > 10000 {
		return conf, errors.New("limit must be between 1 and 10000")
	}

	return
}

type cloudWatchLogsInput struct {
	conf   cloudWatchLogsInputConfig
	log    *service.Logger
	client cloudWatchLogsAPI

	nextToken *string
	startTime int64
	endTime   int64
	msgChan   chan asyncMessage

	connMu  sync.Mutex
	shutSig *shutdown.Signaller
}

func newCloudWatchLogsInputFromConfig(pConf *service.ParsedConfig, mgr *service.Resources) (*cloudWatchLogsInput, error) {
	conf, err := cloudWatchLogsInputConfigFromParsed(pConf)
	if err != nil {
		return nil, err
	}

	sess, err := GetSession(context.Background(), pConf)
	if err != nil {
		return nil, err
	}

	client := cloudwatchlogs.NewFromConfig(sess)

	var startTime int64
	if conf.StartTime != nil {
		startTime = conf.StartTime.UnixMilli()
	}

	return &cloudWatchLogsInput{
		conf:      conf,
		log:       mgr.Logger(),
		client:    client,
		startTime: startTime,
		endTime:   0,
	}, nil
}

func (c *cloudWatchLogsInput) Connect(ctx context.Context) error {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.shutSig != nil {
		return nil
	}

	if err := c.verifyLogGroup(ctx); err != nil {
		return err
	}

	c.msgChan = make(chan asyncMessage)
	c.shutSig = shutdown.NewSignaller()

	go c.pollLoop()

	c.log.Infof("Connected to CloudWatch Logs group: %s", c.conf.LogGroupName)
	return nil
}

func (c *cloudWatchLogsInput) verifyLogGroup(ctx context.Context) error {
	in := &cloudwatchlogs.DescribeLogGroupsInput{
		LogGroupNamePrefix: aws.String(c.conf.LogGroupName),
		Limit:              aws.Int32(1),
	}

	out, err := c.client.DescribeLogGroups(ctx, in)
	if err != nil {
		return fmt.Errorf("describing log groups: %w", err)
	}

	for _, lg := range out.LogGroups {
		if lg.LogGroupName != nil && *lg.LogGroupName == c.conf.LogGroupName {
			return nil
		}
	}

	return fmt.Errorf("log group %q not found", c.conf.LogGroupName)
}

func (c *cloudWatchLogsInput) pollLoop() {
	shutSig := c.shutSig
	msgChan := c.msgChan

	defer func() {
		c.connMu.Lock()
		shutSig.TriggerHasStopped()
		close(msgChan)
		c.shutSig = nil
		c.msgChan = nil
		c.connMu.Unlock()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-shutSig.SoftStopChan():
			cancel()
		case <-ctx.Done():
		}
	}()

	ticker := time.NewTicker(c.conf.PollInterval)
	defer ticker.Stop()

	// Poll immediately on startup
	hasMore := c.poll(ctx, shutSig, msgChan)

	for {
		// If we have more data (pagination), poll immediately without waiting
		if hasMore {
			select {
			case <-shutSig.SoftStopChan():
				return
			default:
			}
			ticker.Reset(c.conf.PollInterval)
			hasMore = c.poll(ctx, shutSig, msgChan)
			continue
		}

		select {
		case <-shutSig.SoftStopChan():
			return
		case <-ticker.C:
			hasMore = c.poll(ctx, shutSig, msgChan)
		}
	}
}

func (c *cloudWatchLogsInput) poll(ctx context.Context, shutSig *shutdown.Signaller, msgChan chan asyncMessage) bool {
	ctx, cancel := context.WithTimeout(ctx, c.conf.APITimeout)
	defer cancel()

	in := &cloudwatchlogs.FilterLogEventsInput{
		LogGroupName: aws.String(c.conf.LogGroupName),
		Limit:        aws.Int32(int32(c.conf.Limit)),
	}

	if len(c.conf.LogStreamNames) > 0 {
		in.LogStreamNames = c.conf.LogStreamNames
	} else if c.conf.LogStreamPrefix != nil {
		in.LogStreamNamePrefix = c.conf.LogStreamPrefix
	}

	if c.conf.FilterPattern != nil {
		in.FilterPattern = c.conf.FilterPattern
	}

	if c.startTime > 0 {
		in.StartTime = aws.Int64(c.startTime)
	}
	if c.endTime > 0 {
		in.EndTime = aws.Int64(c.endTime)
	}

	if c.nextToken != nil {
		in.NextToken = c.nextToken
	}

	out, err := c.client.FilterLogEvents(ctx, in)
	if err != nil {
		c.log.Errorf("Polling CloudWatch Logs: %v", err)
		return false
	}

	// Build batch from events
	var batch service.MessageBatch
	for _, event := range out.Events {
		batch = append(batch, c.eventToMessage(event))

		// Update checkpoint - use ingestion time as it's monotonically increasing
		if event.IngestionTime != nil {
			if t := *event.IngestionTime; t > c.startTime {
				c.startTime = t + 1 // Add 1ms to avoid re-reading the same event
			}
		}
	}

	// Send the batch
	if len(batch) > 0 {
		select {
		case msgChan <- asyncMessage{msg: batch, ackFn: func(context.Context, error) error { return nil }}:
		case <-shutSig.SoftStopChan():
			return false
		}
		c.log.Debugf("Processed %d log events from CloudWatch Logs", len(batch))
	}

	// Update pagination token
	c.nextToken = out.NextToken

	// If we've exhausted this page and have no next token, update the time window
	if c.nextToken == nil {
		if len(out.Events) == 0 {
			c.startTime = time.Now().UnixMilli()
			c.endTime = 0 // Reset end time for live tailing
		}
	}

	return c.nextToken != nil
}

func (c *cloudWatchLogsInput) eventToMessage(event types.FilteredLogEvent) *service.Message {
	var msg *service.Message

	if c.conf.StructuredLog {
		structured := map[string]any{
			"message":        aws.ToString(event.Message),
			"log_group":      c.conf.LogGroupName,
			"timestamp":      event.Timestamp,
			"ingestion_time": event.IngestionTime,
		}

		if event.LogStreamName != nil {
			structured["log_stream"] = *event.LogStreamName
		}

		if event.EventId != nil {
			structured["event_id"] = *event.EventId
		}

		jsonBytes, _ := json.Marshal(structured)
		msg = service.NewMessage(jsonBytes)
	} else {
		msg = service.NewMessage([]byte(aws.ToString(event.Message)))

		if event.LogStreamName != nil {
			msg.MetaSetMut("cloudwatch_log_stream", *event.LogStreamName)
		}

		msg.MetaSetMut("cloudwatch_log_group", c.conf.LogGroupName)

		if event.Timestamp != nil {
			msg.MetaSetMut("cloudwatch_timestamp", strconv.FormatInt(*event.Timestamp, 10))
		}

		if event.IngestionTime != nil {
			msg.MetaSetMut("cloudwatch_ingestion_time", strconv.FormatInt(*event.IngestionTime, 10))
		}

		if event.EventId != nil {
			msg.MetaSetMut("cloudwatch_event_id", *event.EventId)
		}
	}

	return msg
}

func (c *cloudWatchLogsInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	c.connMu.Lock()
	msgChan := c.msgChan
	shutSig := c.shutSig
	c.connMu.Unlock()

	if msgChan == nil || shutSig == nil {
		return nil, nil, service.ErrNotConnected
	}

	select {
	case m, open := <-msgChan:
		if !open {
			return nil, nil, service.ErrNotConnected
		}
		return m.msg, m.ackFn, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (c *cloudWatchLogsInput) Close(_ context.Context) error {
	c.connMu.Lock()
	shutSig := c.shutSig
	c.connMu.Unlock()

	if shutSig == nil {
		return nil
	}

	shutSig.TriggerSoftStop()
	select {
	case <-shutSig.HasStoppedChan():
	case <-time.After(5 * time.Second):
		shutSig.TriggerHardStop()
	}

	return nil
}
