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

package kinesis

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestStreamIDParser(t *testing.T) {
	tests := []struct {
		name        string
		id          string
		remaining   string
		shard       string
		errContains string
	}{
		{
			name:      "no shards stream name",
			id:        "foo-bar",
			remaining: "foo-bar",
		},
		{
			name:      "no shards stream arn",
			id:        "arn:aws:kinesis:region:account-id:stream/stream-name",
			remaining: "arn:aws:kinesis:region:account-id:stream/stream-name",
		},
		{
			name:      "sharded stream name",
			id:        "foo-bar:baz",
			remaining: "foo-bar",
			shard:     "baz",
		},
		{
			name:      "sharded stream arn",
			id:        "arn:aws:kinesis:region:account-id:stream/stream-name:baz",
			remaining: "arn:aws:kinesis:region:account-id:stream/stream-name",
			shard:     "baz",
		},
		{
			name:        "multiple shards stream name",
			id:          "foo-bar:baz:buz",
			errContains: "only one shard should be specified",
		},
		{
			name:        "multiple shards stream arn",
			id:          "arn:aws:kinesis:region:account-id:stream/stream-name:baz:buz",
			errContains: "only one shard should be specified",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rem, shard, err := parseStreamID(test.id)
			if test.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.remaining, rem)
				assert.Equal(t, test.shard, shard)
			}
		})
	}
}

func TestPollIntervalConfigParsing(t *testing.T) {
	tests := []struct {
		name         string
		conf         string
		pollInterval string
	}{
		{
			name: "explicit poll interval",
			conf: `
streams: [foo]
poll_interval: 2s
`,
			pollInterval: "2s",
		},
		{
			name: "default poll interval",
			conf: `
streams: [foo]
`,
			pollInterval: "0s",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pConf, err := kinesisInputSpec().ParseYAML(test.conf, nil)
			require.NoError(t, err)

			conf, err := kinesisInputConfigFromParsed(pConf)
			require.NoError(t, err)
			assert.Equal(t, test.pollInterval, conf.PollInterval)
		})
	}
}

func TestNewKinesisReaderFromConfigPollInterval(t *testing.T) {
	baseConf := func(pollInterval string) kiConfig {
		return kiConfig{
			Streams:          []string{"foo"},
			CommitPeriod:     "5s",
			StealGracePeriod: "2s",
			LeasePeriod:      "30s",
			RebalancePeriod:  "30s",
			PollInterval:     pollInterval,
		}
	}

	tests := []struct {
		name         string
		pollInterval string
		expected     time.Duration
		errContains  string
	}{
		{
			name:         "empty poll interval defaults to zero",
			pollInterval: "",
			expected:     0,
		},
		{
			name:         "valid poll interval",
			pollInterval: "750ms",
			expected:     750 * time.Millisecond,
		},
		{
			name:         "invalid poll interval",
			pollInterval: "nope",
			errContains:  "poll interval",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader, err := newKinesisReaderFromConfig(baseConf(test.pollInterval), service.BatchPolicy{}, aws.Config{}, aws.Config{}, service.MockResources())
			if test.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expected, reader.pollInterval)
			}
		})
	}
}

func TestNextPullDelay(t *testing.T) {
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name         string
		pollInterval time.Duration
		lastPull     time.Time
		backoff      time.Duration
		expected     time.Duration
	}{
		{
			name:         "zero poll interval zero backoff",
			pollInterval: 0,
			lastPull:     now,
			backoff:      0,
			expected:     0,
		},
		{
			name:         "zero poll interval with backoff",
			pollInterval: 0,
			lastPull:     now,
			backoff:      300 * time.Millisecond,
			expected:     300 * time.Millisecond,
		},
		{
			name:         "poll interval just started zero backoff",
			pollInterval: time.Second,
			lastPull:     now,
			backoff:      0,
			expected:     time.Second,
		},
		{
			name:         "poll interval partially elapsed zero backoff",
			pollInterval: time.Second,
			lastPull:     now.Add(-400 * time.Millisecond),
			backoff:      0,
			expected:     600 * time.Millisecond,
		},
		{
			name:         "poll interval fully elapsed zero backoff",
			pollInterval: time.Second,
			lastPull:     now.Add(-2 * time.Second),
			backoff:      0,
			expected:     0,
		},
		{
			name:         "backoff dominates remaining interval",
			pollInterval: time.Second,
			lastPull:     now.Add(-900 * time.Millisecond),
			backoff:      5 * time.Second,
			expected:     5 * time.Second,
		},
		{
			name:         "remaining interval dominates backoff",
			pollInterval: 5 * time.Second,
			lastPull:     now.Add(-time.Second),
			backoff:      300 * time.Millisecond,
			expected:     4 * time.Second,
		},
		{
			name:         "never pulled before",
			pollInterval: time.Second,
			lastPull:     time.Time{},
			backoff:      0,
			expected:     0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, nextPullDelay(test.pollInterval, test.lastPull, now, test.backoff))
		})
	}
}
