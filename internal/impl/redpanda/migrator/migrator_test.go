// Copyright 2025 Redpanda Data, Inc.
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

package migrator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestRedpandaMigratorOutputLintRules(t *testing.T) {
	tests := []struct {
		name    string
		conf    string
		lintErr string
	}{
		{
			name: "valid_config_without_schema_registry",
			conf: `
input:
  redpanda_migrator:
    seed_brokers: ["source:9092"]
    topics: ["orders"]
    consumer_group: "migration"

output:
  redpanda_migrator:
    seed_brokers: ["destination:9092"]
    topic: ${! metadata("kafka_topic") }
`,
			lintErr: "",
		},
		{
			name: "valid_config_with_different_schema_registry_urls",
			conf: `
input:
  redpanda_migrator:
    seed_brokers: ["source:9092"]
    topics: ["orders"]
    consumer_group: "migration"
    schema_registry:
      url: "http://source-registry:8081"

output:
  redpanda_migrator:
    seed_brokers: ["destination:9092"]
    topic: ${! metadata("kafka_topic") }
    schema_registry:
      url: "http://destination-registry:8081"
`,
			lintErr: "",
		},
		{
			name: "valid_config_with_only_output_schema_registry",
			conf: `
input:
  redpanda_migrator:
    seed_brokers: ["source:9092"]
    topics: ["orders"]
    consumer_group: "migration"

output:
  redpanda_migrator:
    seed_brokers: ["destination:9092"]
    topic: ${! metadata("kafka_topic") }
    schema_registry:
      url: "http://destination-registry:8081"
`,
			lintErr: "",
		},
		{
			name: "valid_config_with_only_input_schema_registry",
			conf: `
input:
  redpanda_migrator:
    seed_brokers: ["source:9092"]
    topics: ["orders"]
    consumer_group: "migration"
    schema_registry:
      url: "http://source-registry:8081"

output:
  redpanda_migrator:
    seed_brokers: ["destination:9092"]
    topic: ${! metadata("kafka_topic") }
`,
			lintErr: "",
		},
		{
			name: "key_field_set",
			conf: `
input:
  redpanda_migrator:
    seed_brokers: ["source:9092"]
    topics: ["orders"]
    consumer_group: "migration"

output:
  redpanda_migrator:
    seed_brokers: ["destination:9092"]
    topic: ${! metadata("kafka_topic") }
    key: ${! content() }
`,
			lintErr: "key field is not supported by migrator",
		},
		{
			name: "partitioner_field_set",
			conf: `
input:
  redpanda_migrator:
    seed_brokers: ["source:9092"]
    topics: ["orders"]
    consumer_group: "migration"

output:
  redpanda_migrator:
    seed_brokers: ["destination:9092"]
    topic: ${! metadata("kafka_topic") }
    partitioner: manual
`,
			lintErr: "partitioner field is not supported by migrator",
		},
		{
			name: "partition_field_set",
			conf: `
input:
  redpanda_migrator:
    seed_brokers: ["source:9092"]
    topics: ["orders"]
    consumer_group: "migration"

output:
  redpanda_migrator:
    seed_brokers: ["destination:9092"]
    topic: ${! metadata("kafka_topic") }
    partition: ${! metadata("kafka_partition") }
`,
			lintErr: "partition field is not supported by migrator",
		},
		{
			name: "timestamp_field_set",
			conf: `
input:
  redpanda_migrator:
    seed_brokers: ["source:9092"]
    topics: ["orders"]
    consumer_group: "migration"

output:
  redpanda_migrator:
    seed_brokers: ["destination:9092"]
    topic: ${! metadata("kafka_topic") }
    timestamp: ${! timestamp_unix() }
`,
			lintErr: "timestamp field is not supported by migrator",
		},
		{
			name: "timestamp_ms_field_set",
			conf: `
input:
  redpanda_migrator:
    seed_brokers: ["source:9092"]
    topics: ["orders"]
    consumer_group: "migration"

output:
  redpanda_migrator:
    seed_brokers: ["destination:9092"]
    topic: ${! metadata("kafka_topic") }
    timestamp_ms: ${! timestamp_unix_milli() }
`,
			lintErr: "timestamp_ms field is not supported by migrator",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder := service.NewStreamBuilder()
			err := builder.SetYAML(test.conf)
			if test.lintErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.lintErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
