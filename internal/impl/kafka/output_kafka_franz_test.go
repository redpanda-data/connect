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

package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestKafkaFranzOutputBadParams(t *testing.T) {
	testCases := []struct {
		name        string
		conf        string
		errContains string
	}{
		{
			name: "manual partitioner with a partition",
			conf: `
kafka_franz:
  seed_brokers: [ foo:1234 ]
  topic: foo
  partitioner: manual
  partition: '${! meta("foo") }'
`,
		},
		{
			name: "non manual partitioner without a partition",
			conf: `
kafka_franz:
  seed_brokers: [ foo:1234 ]
  topic: foo
`,
		},
		{
			name: "manual partitioner with no partition",
			conf: `
kafka_franz:
  seed_brokers: [ foo:1234 ]
  topic: foo
  partitioner: manual
`,
			errContains: "a partition must be specified when the partitioner is set to manual",
		},
		{
			name: "partition without manual partitioner",
			conf: `
kafka_franz:
  seed_brokers: [ foo:1234 ]
  topic: foo
  partition: '${! meta("foo") }'
`,
			errContains: "a partition cannot be specified unless the partitioner is set to manual",
		},
		{
			name: "idempotent write with acks all",
			conf: `
kafka_franz:
  seed_brokers: [ foo:1234 ]
  topic: foo
  idempotent_write: true
  acks: all
`,
		},
		{
			name: "idempotent write with acks leader",
			conf: `
kafka_franz:
  seed_brokers: [ foo:1234 ]
  topic: foo
  idempotent_write: true
  acks: leader
`,
			errContains: "idempotent_write requires acks to be set to all",
		},
		{
			name: "idempotent write with acks none",
			conf: `
kafka_franz:
  seed_brokers: [ foo:1234 ]
  topic: foo
  idempotent_write: true
  acks: none
`,
			errContains: "idempotent_write requires acks to be set to all",
		},
		{
			name: "non-idempotent with acks leader",
			conf: `
kafka_franz:
  seed_brokers: [ foo:1234 ]
  topic: foo
  idempotent_write: false
  acks: leader
`,
		},
		{
			name: "non-idempotent with acks none",
			conf: `
kafka_franz:
  seed_brokers: [ foo:1234 ]
  topic: foo
  idempotent_write: false
  acks: none
`,
		},
		{
			name: "custom producer limits",
			conf: `
kafka_franz:
  seed_brokers: [ foo:1234 ]
  topic: foo
  idempotent_write: false
  max_buffered_records: 50000
  max_buffered_bytes: "128MB"
  max_in_flight_requests: 5
  record_retries: 10
  record_delivery_timeout: "30s"
`,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			err := service.NewStreamBuilder().AddOutputYAML(test.conf)
			if test.errContains == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			}
		})
	}
}
