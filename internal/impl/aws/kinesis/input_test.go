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

func TestKinesisInputPollPeriodConfig(t *testing.T) {
	pConf, err := kinesisInputSpec().ParseYAML(`
streams: [ foo ]
poll_period: 250ms
`, nil)
	require.NoError(t, err)

	conf, err := kinesisInputConfigFromParsed(pConf)
	require.NoError(t, err)
	assert.Equal(t, 250*time.Millisecond, conf.PollPeriod)
}

func TestKinesisInputPollPeriodDefault(t *testing.T) {
	pConf, err := kinesisInputSpec().ParseYAML(`
streams: [ foo ]
`, nil)
	require.NoError(t, err)

	conf, err := kinesisInputConfigFromParsed(pConf)
	require.NoError(t, err)
	assert.Equal(t, time.Duration(0), conf.PollPeriod)
}

func TestKinesisInputPollPeriodExceedsLeasePeriodFails(t *testing.T) {
	pConf, err := kinesisInputSpec().ParseYAML(`
streams: [ foo ]
poll_period: 60s
lease_period: 30s
`, nil)
	require.NoError(t, err)

	conf, err := kinesisInputConfigFromParsed(pConf)
	require.NoError(t, err)

	_, err = newKinesisReaderFromConfig(conf, service.BatchPolicy{}, aws.Config{}, aws.Config{}, service.MockResources())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "lease_period")
}

func TestKinesisInputPollPeriodBetweenCommitAndLeasePeriodSucceeds(t *testing.T) {
	pConf, err := kinesisInputSpec().ParseYAML(`
streams: [ foo ]
poll_period: 10s
commit_period: 5s
lease_period: 30s
`, nil)
	require.NoError(t, err)

	conf, err := kinesisInputConfigFromParsed(pConf)
	require.NoError(t, err)

	_, err = newKinesisReaderFromConfig(conf, service.BatchPolicy{}, aws.Config{}, aws.Config{}, service.MockResources())
	require.NoError(t, err)
}

func TestKinesisInputEFOConfig(t *testing.T) {
	pConf, err := kinesisInputSpec().ParseYAML(`
streams: [ foo ]
enhanced_fan_out:
  enabled: true
  consumer_name: my-app
`, nil)
	require.NoError(t, err)

	conf, err := kinesisInputConfigFromParsed(pConf)
	require.NoError(t, err)
	assert.True(t, conf.EFOEnabled)
	assert.Equal(t, "my-app", conf.EFOConsumerName)
}

func TestKinesisInputEFODisabledByDefault(t *testing.T) {
	pConf, err := kinesisInputSpec().ParseYAML(`
streams: [ foo ]
`, nil)
	require.NoError(t, err)

	conf, err := kinesisInputConfigFromParsed(pConf)
	require.NoError(t, err)
	assert.False(t, conf.EFOEnabled)
}

func TestKinesisInputEFOActivationTimeoutDefault(t *testing.T) {
	pConf, err := kinesisInputSpec().ParseYAML(`
streams: [ foo ]
enhanced_fan_out:
  enabled: true
  consumer_name: my-app
`, nil)
	require.NoError(t, err)

	conf, err := kinesisInputConfigFromParsed(pConf)
	require.NoError(t, err)
	assert.Equal(t, time.Minute, conf.EFOActivationTimeout)
}

func TestKinesisInputEFOActivationTimeoutCustom(t *testing.T) {
	pConf, err := kinesisInputSpec().ParseYAML(`
streams: [ foo ]
enhanced_fan_out:
  enabled: true
  consumer_name: my-app
  consumer_activation_timeout: 90s
`, nil)
	require.NoError(t, err)

	conf, err := kinesisInputConfigFromParsed(pConf)
	require.NoError(t, err)
	assert.Equal(t, 90*time.Second, conf.EFOActivationTimeout)
}

func TestKinesisInputEFOActivationTimeoutZeroFails(t *testing.T) {
	pConf, err := kinesisInputSpec().ParseYAML(`
streams: [ foo ]
enhanced_fan_out:
  enabled: true
  consumer_name: my-app
  consumer_activation_timeout: 0s
`, nil)
	require.NoError(t, err)

	_, err = kinesisInputConfigFromParsed(pConf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "consumer_activation_timeout")
}

func TestKinesisInputEFOMaxResubscribeIntervalDefault(t *testing.T) {
	pConf, err := kinesisInputSpec().ParseYAML(`
streams: [ foo ]
enhanced_fan_out:
  enabled: true
  consumer_name: my-app
`, nil)
	require.NoError(t, err)

	conf, err := kinesisInputConfigFromParsed(pConf)
	require.NoError(t, err)
	assert.Equal(t, 30*time.Second, conf.EFOMaxResubscribeInterval)
}

func TestKinesisInputEFOMaxResubscribeIntervalCustom(t *testing.T) {
	pConf, err := kinesisInputSpec().ParseYAML(`
streams: [ foo ]
enhanced_fan_out:
  enabled: true
  consumer_name: my-app
  max_resubscribe_interval: 90s
`, nil)
	require.NoError(t, err)

	conf, err := kinesisInputConfigFromParsed(pConf)
	require.NoError(t, err)
	assert.Equal(t, 90*time.Second, conf.EFOMaxResubscribeInterval)
}

func TestKinesisInputEFOMaxResubscribeIntervalTooLowFails(t *testing.T) {
	pConf, err := kinesisInputSpec().ParseYAML(`
streams: [ foo ]
enhanced_fan_out:
  enabled: true
  consumer_name: my-app
  max_resubscribe_interval: 500ms
`, nil)
	require.NoError(t, err)

	_, err = kinesisInputConfigFromParsed(pConf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max_resubscribe_interval")
}

func TestKinesisInputEFOConsumerNameLintRule(t *testing.T) {
	tests := []struct {
		name        string
		conf        string
		lintPresent bool
	}{
		{
			name: "efo disabled",
			conf: `
aws_kinesis:
  streams: [ foo ]
`,
		},
		{
			name: "efo enabled with consumer name",
			conf: `
aws_kinesis:
  streams: [ foo ]
  enhanced_fan_out:
    enabled: true
    consumer_name: my-app
`,
		},
		{
			name: "efo enabled without consumer name",
			conf: `
aws_kinesis:
  streams: [ foo ]
  enhanced_fan_out:
    enabled: true
`,
			lintPresent: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			linter := service.NewEnvironment().NewComponentConfigLinter()

			lints, err := linter.LintInputYAML([]byte(test.conf))
			require.NoError(t, err)
			if test.lintPresent {
				require.Len(t, lints, 1)
				assert.Contains(t, lints[0].Error(), "consumer_name is required when enabled is true")
			} else {
				assert.Empty(t, lints)
			}
		})
	}
}

func TestKinesisInputEFORequiresConsumerName(t *testing.T) {
	pConf, err := kinesisInputSpec().ParseYAML(`
streams: [ foo ]
enhanced_fan_out:
  enabled: true
`, nil)
	require.NoError(t, err)

	_, err = kinesisInputConfigFromParsed(pConf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "consumer_name")
}

func TestShardFetchWaitBound(t *testing.T) {
	tests := []struct {
		name         string
		commitPeriod time.Duration
		batchPeriod  time.Duration
		want         time.Duration
	}{
		{
			name:         "default commit period no batch period",
			commitPeriod: 5 * time.Second,
			want:         time.Second,
		},
		{
			name:         "short commit period halves the bound",
			commitPeriod: time.Second,
			want:         500 * time.Millisecond,
		},
		{
			name:         "batch period tightens the bound",
			commitPeriod: 5 * time.Second,
			batchPeriod:  100 * time.Millisecond,
			want:         50 * time.Millisecond,
		},
		{
			name:         "batch period below the floor is clamped",
			commitPeriod: 5 * time.Second,
			batchPeriod:  time.Millisecond,
			want:         5 * time.Millisecond,
		},
		{
			name:         "zero batch period is ignored",
			commitPeriod: 5 * time.Second,
			batchPeriod:  0,
			want:         time.Second,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, shardFetchWaitBound(test.commitPeriod, test.batchPeriod))
		})
	}
}
