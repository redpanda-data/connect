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
		test := test
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

func TestKinesisInputConfig(t *testing.T) {
	tests := []struct {
		name          string
		config        string
		expectedDelay time.Duration
		expectError   bool
		errorContains string
	}{
		{
			name: "default record_pull_delay",
			config: `
streams: ["test-stream"]
dynamodb:
  table: "test-table"
`,
			expectedDelay: 0,
			expectError:   false,
		},
		{
			name: "explicit record_pull_delay",
			config: `
streams: ["test-stream"]
dynamodb:
  table: "test-table"
record_pull_delay: "250ms"
`,
			expectedDelay: 250 * time.Millisecond,
			expectError:   false,
		},
		{
			name: "zero record_pull_delay",
			config: `
streams: ["test-stream"]
dynamodb:
  table: "test-table"
record_pull_delay: "0s"
`,
			expectedDelay: 0,
			expectError:   false,
		},
		{
			name: "one second record_pull_delay",
			config: `
streams: ["test-stream"]
dynamodb:
  table: "test-table"
record_pull_delay: "1s"
`,
			expectedDelay: time.Second,
			expectError:   false,
		},
		{
			name: "invalid record_pull_delay",
			config: `
streams: ["test-stream"]
dynamodb:
  table: "test-table"
record_pull_delay: "invalid"
`,
			expectError:   true,
			errorContains: "failed to parse record pull delay",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			pConf, err := kinesisInputSpec().ParseYAML(test.config, nil)
			require.NoError(t, err)

			conf, err := kinesisInputConfigFromParsed(pConf)
			require.NoError(t, err)

			// Create a mock batch policy and sessions for testing
			batcher := service.BatchPolicy{}
			sess := aws.Config{Region: "us-east-1"}
			ddbSess := aws.Config{Region: "us-east-1"}

			reader, err := newKinesisReaderFromConfig(conf, batcher, sess, ddbSess, service.MockResources())

			if test.expectError {
				require.Error(t, err)
				if test.errorContains != "" {
					assert.Contains(t, err.Error(), test.errorContains)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedDelay, reader.recordPullDelay)
			}
		})
	}
}
