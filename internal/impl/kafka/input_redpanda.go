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

func redpandaInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Summary(`A Kafka input using the https://github.com/twmb/franz-go[Franz Kafka client library^].`).
		Description(`
When a consumer group is specified this input consumes one or more topics where partitions will automatically balance across any other connected clients with the same consumer group. When a consumer group is not specified topics can either be consumed in their entirety or with explicit partitions.

== Delivery Guarantees

When using consumer groups the offsets of "delivered" records will be committed automatically and continuously, and in the event of restarts these committed offsets will be used in order to resume from where the input left off. Redpanda Connect guarantees at least once delivery by ensuring that records are only considerd to be delivered when all configured outputs that the record is routed to have confirmed delivery.

== Ordering

In order to preserve ordering of topic partitions, records consumed from each partition are processed and delivered in the order that they are received, and only one batch of records of a given partition will ever be processed at a time. This means that parallel processing can only occur when multiple topic partitions are being consumed, but ensures that data is processed in a sequential order as determined from the source partition.

However, one way in which the order of records can be mixed is when delivery errors occur and error handling mechanisms kick in. Redpanda Connect always leans towards at least once delivery unless instructed otherwise, and this includes reattempting delivery of data when the ordering of that data can no longer be guaranteed.

For example, a batch of records may have been sent to an output broker and only a subset of records were delivered, in this case Redpanda Connect by default will reattempt to deliver the records that failed, even though these failed records may have come before records that were previously delivered successfully.

In order to avoid this scenario you must specify in your configuration an alternative way to handle delivery errors in the form of a ` + "xref:components:outputs/fallback.adoc[`fallback`] output" + `. It is good practice to also disable the field ` + "`auto_retry_nacks` by setting it to `false`" + ` when you've added an explicit fallback output as this will improve the throughput of your pipeline. For example, the following config avoids ordering issues by specifying a fallback output into a DLQ topic, which is also retried indefinitely as a way to apply back pressure during connectivity issues:

` + "```yaml" + `
output:
  fallback:
    - redpanda:
        seed_brokers: [ localhost:9092 ]
        topic: foo
    - retry:
        output:
          redpanda:
            seed_brokers: [ localhost:9092 ]
            topic: foo_dlq
` + "```" + `

== Batching

Records are processed and delivered from each partition in batches as received from brokers. These batch sizes are therefore dynamically sized in order to optimise throughput, but can be tuned with the config fields ` + "`fetch_max_partition_bytes` and `fetch_max_bytes`" + `. Batches can be further broken down using the ` + "xref:components:processors/split.adoc[`split`] processor" + `.

== Metadata

This input adds the following metadata fields to each message:

` + "```text" + `
- kafka_key
- kafka_topic
- kafka_partition
- kafka_offset
- kafka_timestamp_ms
- kafka_timestamp_unix
- kafka_tombstone_message
- All record headers
` + "```" + `
`).
		Fields(redpandaInputConfigFields()...).
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

func redpandaInputConfigFields() []*service.ConfigField {
	return slices.Concat(
		FranzConnectionFields(),
		FranzConsumerFields(),
		FranzReaderOrderedConfigFields(),
		[]*service.ConfigField{
			service.NewAutoRetryNacksToggleField(),
		},
	)
}

func init() {
	err := service.RegisterBatchInput("redpanda", redpandaInputConfig(),
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

			rdr, err := NewFranzReaderOrderedFromConfig(conf, mgr, func() ([]kgo.Opt, error) {
				return clientOpts, nil
			})
			if err != nil {
				return nil, err
			}

			return service.AutoRetryNacksBatchedToggled(conf, rdr)
		})
	if err != nil {
		panic(err)
	}
}
