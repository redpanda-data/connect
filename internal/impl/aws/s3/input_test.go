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

package s3

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIdlePollPeriodConfigParsing exercises the full parse path for the
// sqs.idle_poll_period field: the default (empty-queue throttle unchanged),
// valid durations, and the negative-duration guard in s3iSQSConfigFromParsed.
func TestIdlePollPeriodConfigParsing(t *testing.T) {
	const baseSQS = `
bucket: foo
region: eu-west-1
credentials:
  id: xxxxx
  secret: xxxxx
sqs:
  url: http://example.com/queue
`

	tests := []struct {
		name          string
		yaml          string
		expectedValue time.Duration
		expectErr     bool
		errContains   string
	}{
		{
			name:          "field omitted defaults to zero",
			yaml:          baseSQS,
			expectedValue: 0,
		},
		{
			name:          "explicit minutes",
			yaml:          baseSQS + "  idle_poll_period: 10m\n",
			expectedValue: 10 * time.Minute,
		},
		{
			name:          "explicit millis",
			yaml:          baseSQS + "  idle_poll_period: 500ms\n",
			expectedValue: 500 * time.Millisecond,
		},
		{
			name:        "negative is rejected",
			yaml:        baseSQS + "  idle_poll_period: -10m\n",
			expectErr:   true,
			errContains: "cannot be negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := s3InputSpec().ParseYAML(tt.yaml, nil)
			require.NoError(t, err)

			conf, err := s3iConfigFromParsed(parsed)
			if tt.expectErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expectedValue, conf.SQS.IdlePollPeriod)
		})
	}
}
