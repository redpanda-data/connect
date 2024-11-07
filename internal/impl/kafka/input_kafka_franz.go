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
	"slices"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/twmb/franz-go/pkg/kgo"
)

func franzKafkaInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Version("3.61.0").
		Summary(`A Kafka input using the https://github.com/twmb/franz-go[Franz Kafka client library^].`).
		Description(`
When a consumer group is specified this input consumes one or more topics where partitions will automatically balance across any other connected clients with the same consumer group. When a consumer group is not specified topics can either be consumed in their entirety or with explicit partitions.

This input often out-performs the traditional ` + "`kafka`" + ` input as well as providing more useful logs and error messages.

== Metadata

This input adds the following metadata fields to each message:

` + "```text" + `
- kafka_key
- kafka_topic
- kafka_partition
- kafka_offset
- kafka_timestamp
- kafka_timestamp_unix
- kafka_tombstone_message
- All record headers
` + "```" + `
`).
		Fields(FranzKafkaInputConfigFields()...).
		LintRule(`
let has_topic_partitions = this.topics.any(t -> t.contains(":"))
root = if $has_topic_partitions {
  if this.consumer_group.or("") != "" {
    "this input does not support both a consumer group and explicit topic partitions"
  } else if this.regexp_topics {
    "this input does not support both regular expression topics and explicit topic partitions"
  }
} else {
  if this.consumer_group.or("") == "" {
    "a consumer group is mandatory when not using explicit topic partitions"
  }
}
`)
}

// FranzKafkaInputConfigFields returns the full suite of config fields for a
// kafka input using the franz-go client library.
func FranzKafkaInputConfigFields() []*service.ConfigField {
	return slices.Concat(
		FranzConnectionFields(),
		FranzConsumerFields(),
		FranzReaderUnorderedConfigFields(),
		[]*service.ConfigField{
			service.NewAutoRetryNacksToggleField(),
		},
	)
}

func init() {
	err := service.RegisterBatchInput("kafka_franz", franzKafkaInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			tmpOpts, err := FranzConnectionOptsFromConfig(conf, mgr.Logger())
			if err != nil {
				return nil, err
			}
			clientOpts := append([]kgo.Opt{}, tmpOpts...)

			if tmpOpts, err = FranzConsumerOptsFromConfig(conf); err != nil {
				return nil, err
			}
			clientOpts = append(clientOpts, tmpOpts...)

			rdr, err := NewFranzReaderUnorderedFromConfig(conf, mgr, clientOpts...)
			if err != nil {
				return nil, err
			}

			return service.AutoRetryNacksBatchedToggled(conf, rdr)
		})
	if err != nil {
		panic(err)
	}
}
