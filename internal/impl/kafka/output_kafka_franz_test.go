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
		{
			name: "idempotent write with max_in_flight_requests above one",
			conf: `
kafka_franz:
  seed_brokers: [ foo:1234 ]
  topic: foo
  idempotent_write: true
  max_in_flight_requests: 5
`,
			errContains: "idempotent_write requires max_in_flight_requests to be 1",
		},
		{
			// idempotent_write defaults to true, so omitting it must still be
			// caught. This is the shape users actually write.
			name: "default idempotent write with max_in_flight_requests above one",
			conf: `
kafka_franz:
  seed_brokers: [ foo:1234 ]
  topic: foo
  max_in_flight_requests: 5
`,
			errContains: "idempotent_write requires max_in_flight_requests to be 1",
		},
		{
			name: "idempotent write with max_in_flight_requests of one",
			conf: `
kafka_franz:
  seed_brokers: [ foo:1234 ]
  topic: foo
  idempotent_write: true
  max_in_flight_requests: 1
`,
		},
		{
			name: "non-idempotent write with max_in_flight_requests above one",
			conf: `
kafka_franz:
  seed_brokers: [ foo:1234 ]
  topic: foo
  idempotent_write: false
  max_in_flight_requests: 5
`,
		},
		{
			// Same latent gap as above, on the pre-existing acks rule.
			name: "default idempotent write with acks leader",
			conf: `
kafka_franz:
  seed_brokers: [ foo:1234 ]
  topic: foo
  acks: leader
`,
			errContains: "idempotent_write requires acks to be set to all",
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

// The linter rules above are the first line of defence, but they can be bypassed
// (--chilled, or configs built programmatically). Assert that opt construction
// also rejects the combination, so it fails there rather than surfacing from
// franz-go inside Connect() as an endlessly retried connection error.
func TestFranzProducerOptsIdempotencyLimits(t *testing.T) {
	spec := service.NewConfigSpec().Fields(FranzProducerFields()...)

	testCases := []struct {
		name        string
		conf        string
		errContains string
	}{
		{
			name:        "default idempotent write with max_in_flight_requests above one",
			conf:        `max_in_flight_requests: 5`,
			errContains: "idempotent_write requires max_in_flight_requests to be 1, got 5",
		},
		{
			name: "explicit idempotent write with max_in_flight_requests above one",
			conf: `
idempotent_write: true
max_in_flight_requests: 2
`,
			errContains: "idempotent_write requires max_in_flight_requests to be 1, got 2",
		},
		{
			name: "idempotent write with the default max_in_flight_requests",
			conf: `idempotent_write: true`,
		},
		{
			name: "non-idempotent write with max_in_flight_requests above one",
			conf: `
idempotent_write: false
max_in_flight_requests: 5
`,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			conf, err := spec.ParseYAML(test.conf, nil)
			require.NoError(t, err)

			_, err = FranzProducerOptsFromConfig(conf)
			if test.errContains == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			}
		})
	}
}
