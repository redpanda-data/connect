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

func TestRedpandaInputFranzConsumerFieldLintRules(t *testing.T) {
	tests := []struct {
		name    string
		conf    string
		lintErr string
	}{
		{
			name: "valid_config_with_topics",
			conf: `
redpanda:
  seed_brokers: ["localhost:9092"]
  topics:
    - foo
    - bar
  consumer_group: test
`,
			lintErr: "",
		},
		{
			name: "valid_config_with_regexp_topics_include",
			conf: `
redpanda:
  seed_brokers: ["localhost:9092"]
  regexp_topics_include:
    - "logs_.*"
  consumer_group: test
`,
			lintErr: "",
		},
		{
			name: "valid_config_with_topic_partitions",
			conf: `
redpanda:
  seed_brokers: ["localhost:9092"]
  topics:
    - foo:0
    - bar:1
`,
			lintErr: "",
		},
		{
			name: "valid_config_with_regexp_topics_exclude",
			conf: `
redpanda:
  seed_brokers: ["localhost:9092"]
  regexp_topics_include:
    - "logs_.*"
  regexp_topics_exclude:
    - "logs_debug_.*"
  consumer_group: test
`,
			lintErr: "",
		},
		{
			name: "both_topics_and_regexp_topics_include",
			conf: `
redpanda:
  seed_brokers: ["localhost:9092"]
  topics:
    - foo
    - bar
  regexp_topics_include:
    - "logs_.*"
  consumer_group: test
`,
			lintErr: "(3,1) cannot specify both topics and regexp_topics_include, use one or the other",
		},
		{
			name: "topic_partitions_with_consumer_group",
			conf: `
redpanda:
  seed_brokers: ["localhost:9092"]
  topics:
    - foo:0
    - bar:1
  consumer_group: test
`,
			lintErr: "(3,1) this input does not support both a consumer group and explicit topic partitions",
		},
		{
			name: "topic_partitions_with_regexp_topics",
			conf: `
redpanda:
  seed_brokers: ["localhost:9092"]
  topics:
    - foo:0
    - bar:1
  regexp_topics: true
`,
			lintErr: "(3,1) this input does not support both regular expression topics and explicit topic partitions",
		},
		{
			name: "no_consumer_group_without_topic_partitions",
			conf: `
redpanda:
  seed_brokers: ["localhost:9092"]
  topics:
    - foo
    - bar
`,
			lintErr: "(3,1) a consumer group is mandatory when not using explicit topic partitions",
		},
		{
			name: "neither_topics_nor_regexp_topics_include",
			conf: `
redpanda:
  seed_brokers: ["localhost:9092"]
  consumer_group: test
`,
			lintErr: "(3,1) either topics or regexp_topics_include must be specified",
		},
		{
			name: "regexp_topics_exclude_without_regex_mode",
			conf: `
redpanda:
  seed_brokers: ["localhost:9092"]
  topics:
    - foo
  regexp_topics_exclude:
    - "bar_.*"
  consumer_group: test
`,
			lintErr: "(3,1) regexp_topics_exclude can only be used when regexp_topics is set to true or regexp_topics_include is specified",
		},
		{
			name: "start_from_oldest_false_with_start_offset_earliest",
			conf: `
redpanda:
  seed_brokers: ["localhost:9092"]
  topics:
    - foo
  consumer_group: test
  start_from_oldest: false
  start_offset: earliest
`,
			lintErr: "(3,1) start_from_oldest cannot be set to false when start_offset is set to earliest",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			env := service.NewEnvironment()
			linter := env.NewComponentConfigLinter()

			lints, err := linter.LintInputYAML([]byte(test.conf))
			require.NoError(t, err)
			if test.lintErr != "" {
				assert.Len(t, lints, 1)
				assert.Equal(t, test.lintErr, lints[0].Error())
			} else {
				assert.Empty(t, lints)
			}
		})
	}
}
