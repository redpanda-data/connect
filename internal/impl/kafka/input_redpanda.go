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
	"fmt"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// RedpandaInputDescription returns the description shared by the Redpanda
// inputs that consume with the franz-go readers. The fallbackExample is a YAML
// config that routes failed deliveries to a DLQ topic. When cloudFallbackExample
// is not empty, the docs show it in Redpanda Cloud instead of fallbackExample,
// for an input whose Cloud config needs different connection fields.
// batchTuningFields names
// the config fields that tune batch sizes, and orderingNote, when not empty,
// is a paragraph added to the Ordering section for an input whose ordering
// can be turned off.
func RedpandaInputDescription(fallbackExample, cloudFallbackExample, batchTuningFields, orderingNote string) string {
	if orderingNote != "" {
		orderingNote += "\n\n"
	}
	example := "```yaml" + fallbackExample + "```"
	if cloudFallbackExample != "" {
		example = "ifndef::env-cloud[]\n" + example + "\nendif::[]\n\nifdef::env-cloud[]\n```yaml" + cloudFallbackExample + "```\nendif::[]"
	}
	return `
When a consumer group is specified this input consumes one or more topics, and partitions automatically balance across any other connected clients with the same consumer group. When a consumer group is not specified, topics can either be consumed in their entirety or with explicit partitions.

== Delivery Guarantees

When using consumer groups, the offsets of "delivered" records are committed automatically and continuously, and in the event of restarts these committed offsets are used to resume from where the input left off. Redpanda Connect guarantees at least once delivery by ensuring that records are only considered to be delivered when all configured outputs that the record is routed to have confirmed delivery.

== Ordering

To preserve ordering of topic partitions, records consumed from each partition are processed and delivered in the order that they are received, and only one batch of records of a given partition is ever processed at a time. This means that parallel processing can only occur when multiple topic partitions are being consumed, but ensures that data is processed in a sequential order as determined from the source partition.

` + orderingNote + `However, one way in which the order of records can be mixed is when delivery errors occur and error handling mechanisms kick in. Redpanda Connect always leans towards at least once delivery unless instructed otherwise, and this includes reattempting delivery of data when the ordering of that data can no longer be guaranteed.

For example, a batch of records may have been sent to an output broker and only a subset of records were delivered. In this case Redpanda Connect by default reattempts to deliver the records that failed, even though these failed records may have come before records that were previously delivered successfully.

To avoid this scenario you must specify in your configuration an alternative way to handle delivery errors in the form of a ` + "xref:components:outputs/fallback.adoc[`fallback`] output" + `. It is good practice to also disable the field ` + "`auto_retry_nacks` by setting it to `false`" + ` when you've added an explicit fallback output as this improves the throughput of your pipeline. For example, the following config avoids ordering issues by specifying a fallback output into a DLQ topic, which is also retried indefinitely as a way to apply back pressure during connectivity issues:

` + example + `

== Batching

Records are processed and delivered from each partition in batches as received from brokers. These batch sizes are therefore dynamically sized in order to optimise throughput, but can be tuned with the config fields ` + batchTuningFields + `. Batches can be further broken down using the ` + "xref:components:processors/split.adoc[`split`] processor" + `.

== Metrics

When a consumer group is set, this input emits a ` + "`redpanda_lag`" + ` metric with ` + "`topic`" + ` and ` + "`partition`" + ` labels for each consumed topic. The metric records the number of produced messages that the consumer group has not yet read from each topic partition.

== Metadata

This input adds the following metadata fields to each message:

` + "```text" + `
- kafka_key
- kafka_topic
- kafka_partition
- kafka_offset
- kafka_lag
- kafka_timestamp_ms
- kafka_timestamp_unix
- kafka_tombstone_message
- All record headers
` + "```" + `
`
}

func redpandaInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Version("4.39.0").
		Stable().
		Categories("Services").
		Summary(`A Kafka input using the https://github.com/twmb/franz-go[Franz Kafka client library^].`).
		Description(RedpandaInputDescription(`
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
`, `
output:
  fallback:
    - redpanda:
        seed_brokers: [ "${REDPANDA_BROKERS}" ]
        topic: foo
        tls:
          enabled: true
        sasl:
          - mechanism: SCRAM-SHA-256
            username: "<username>"
            password: "${secrets.PASSWORD}"
    - retry:
        output:
          redpanda:
            seed_brokers: [ "${REDPANDA_BROKERS}" ]
            topic: foo_dlq
            tls:
              enabled: true
            sasl:
              - mechanism: SCRAM-SHA-256
                username: "<username>"
                password: "${secrets.PASSWORD}"
`, "`fetch_max_bytes`, `fetch_max_partition_bytes`, and `max_yield_batch_bytes`, or `unordered_processing.batching` when unordered processing is enabled", "If `unordered_processing.enabled` is `true`, this does not apply. Messages of the same partition are processed in parallel, up to `unordered_processing.checkpoint_limit`, and can be delivered out of order.")).
		Fields(redpandaInputConfigFields()...).
		LintRule(FranzConsumerFieldLintRules)
}

const rpiFieldPartitionAssignment = "partition_assignment_strategy"

func redpandaInputConfigFields() []*service.ConfigField {
	return slices.Concat(
		FranzConnectionOptionalFields(),
		FranzConsumerFields(),
		FranzReaderToggledConfigFields(),
		[]*service.ConfigField{
			service.NewStringListField(rpiFieldPartitionAssignment).
				Description(`
When using a consumer group, the strategies this client supports for assigning partitions to group members, in order of preference. This is the equivalent to the Java partition.assignment.strategy setting. Supported values:

- ` + "`cooperative-sticky`" + `: Balances the total partition count per member while moving as few partitions as possible, and rebalances incrementally so that members keep consuming unaffected partitions. It does not balance partitions of each topic individually.
- ` + "`sticky`" + `: The same assignment as ` + "`cooperative-sticky`" + `, using the eager protocol.
- ` + "`range`" + `: Divides the partitions of each topic evenly across the members subscribed to it. Remainder partitions go to the first members by sort order, so when consuming many topics the earlier members can own more partitions overall.
- ` + "`roundrobin`" + `: Assigns all partitions across members one at a time, which balances both each topic and the total when all members subscribe to the same topics.

The eager strategies (` + "`sticky`" + `, ` + "`range`" + ` and ` + "`roundrobin`" + `) revoke every partition from every member on each rebalance, such as when a member joins, leaves or restarts. Records that are in flight but not yet committed are then reprocessed from the last committed offsets, so frequent rebalances cause repeated duplicates.

To change strategies on a running group, first roll out a list with the old strategy followed by the new one, then a list with only the new one. Members that share no strategy with the group are rejected. This is safe between eager strategies and from an eager strategy to ` + "`cooperative-sticky`" + `. Once every member uses only ` + "`cooperative-sticky`" + `, do not roll out an eager strategy: an eager member joining a cooperative group can be elected leader and break rebalancing. Instead, stop all members and start them with the new strategy.`).
				ShortDescription("The strategies used to assign partitions to consumer group members, in order of preference.").
				Example([]string{"range"}).
				Example([]string{"cooperative-sticky", "roundrobin"}).
				Default([]any{"cooperative-sticky"}).
				Advanced().
				Version("4.114.0"),
			service.NewAutoRetryNacksToggleField(),
			service.NewForceTimelyNacksField(),
			service.NewExtractTracingSpanMappingField().Version("4.85.0"),
		},
	)
}

func init() {
	service.MustRegisterBatchInput("redpanda", redpandaInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			connDetails, err := FranzConnectionDetailsFromConfig(conf, mgr.Logger())
			if err != nil {
				return nil, err
			}

			consumerOpts, err := FranzConsumerOptsFromConfig(conf)
			if err != nil {
				return nil, err
			}

			balancers, err := redpandaPartitionAssignmentFromConfig(conf)
			if err != nil {
				return nil, err
			}
			consumerOpts = append(consumerOpts, kgo.Balancers(balancers...))

			var rdr service.BatchInput
			if connDetails.IsConfigured() {
				// We're using a custom connection from config.
				clientOpts := append(connDetails.FranzOpts(), consumerOpts...)
				if rdr, err = NewFranzReaderToggledFromConfig(conf, mgr, func() ([]kgo.Opt, error) {
					return clientOpts, nil
				}); err != nil {
					return nil, err
				}
			} else {
				mgr.Logger().Info("Connection fields omitted, falling back to common redpanda config.")

				// We're using a common redpanda block to determine the connection.
				if rdr, err = NewFranzReaderToggledFromConfig(conf, mgr, func() (clientOpts []kgo.Opt, err error) {
					// Make multiple attempts here just to allow the redpanda logger
					// to initialise in the background. Otherwise we get an annoying
					// log.
					for range 20 {
						if err = FranzSharedClientUse(SharedGlobalRedpandaClientKey, mgr, func(details *FranzSharedClientInfo) error {
							clientOpts = append(clientOpts, details.ConnDetails.FranzOpts()...)
							return nil
						}); err == nil {
							clientOpts = append(clientOpts, consumerOpts...)
							return
						}
						time.Sleep(time.Millisecond * 100)
					}
					return
				}); err != nil {
					return nil, err
				}
			}

			if rdr, err = service.AutoRetryNacksBatchedToggled(conf, rdr); err != nil {
				return nil, err
			}

			if rdr, err = service.ForceTimelyNacksBatched(conf, rdr); err != nil {
				return nil, err
			}

			if rdr, err = conf.WrapBatchInputExtractTracingSpanMapping("redpanda", rdr); err != nil {
				return nil, err
			}

			return rdr, nil
		})
}

func redpandaPartitionAssignmentFromConfig(conf *service.ParsedConfig) ([]kgo.GroupBalancer, error) {
	names, err := conf.FieldStringList(rpiFieldPartitionAssignment)
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("field %v must list at least one strategy", rpiFieldPartitionAssignment)
	}
	seen := map[string]struct{}{}
	balancers := make([]kgo.GroupBalancer, 0, len(names))
	for _, name := range names {
		if _, exists := seen[name]; exists {
			return nil, fmt.Errorf("field %v lists strategy %q more than once", rpiFieldPartitionAssignment, name)
		}
		seen[name] = struct{}{}

		switch name {
		case "cooperative-sticky":
			balancers = append(balancers, kgo.CooperativeStickyBalancer())
		case "sticky":
			balancers = append(balancers, kgo.StickyBalancer())
		case "range":
			balancers = append(balancers, kgo.RangeBalancer())
		case "roundrobin":
			balancers = append(balancers, kgo.RoundRobinBalancer())
		default:
			return nil, fmt.Errorf("field %v contains unsupported strategy %q, expected one of: cooperative-sticky, sticky, range, roundrobin", rpiFieldPartitionAssignment, name)
		}
	}
	return balancers, nil
}
