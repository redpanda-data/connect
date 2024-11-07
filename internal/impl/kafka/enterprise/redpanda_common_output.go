// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package enterprise

import (
	"context"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/kafka"
)

func redpandaCommonOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Summary("Sends data to a Redpanda (Kafka) broker, using credentials defined in a common top-level `redpanda` config block.").
		Fields(kafka.FranzWriterConfigFields()...).
		Fields(
			service.NewOutputMaxInFlightField().
				Default(10),
			service.NewBatchPolicyField(roFieldBatching),
		).
		LintRule(`
root = if this.partitioner == "manual" {
if this.partition.or("") == "" {
"a partition must be specified when the partitioner is set to manual"
}
} else if this.partition.or("") != "" {
"a partition cannot be specified unless the partitioner is set to manual"
}`).
		Example("Simple Output", "Data is generated and written to a topic bar, targetting the cluster configured within the redpanda block at the bottom. This is useful as it allows us to configure TLS and SASL only once for potentially multiple inputs and outputs.", `
input:
  generate:
    interval: 1s
    mapping: 'root.name = fake("name")'

pipeline:
  processors:
    - mutation: |
        root.id = uuid_v4()
        root.loud_name = this.name.uppercase()

output:
  redpanda_common:
    topic: bar
    key: ${! @id }

redpanda:
  seed_brokers: [ "127.0.0.1:9092" ]
  tls:
    enabled: true
  sasl:
    - mechanism: SCRAM-SHA-512
      password: bar
      username: foo
`)
}

const (
	roFieldBatching = "batching"
)

func init() {
	err := service.RegisterBatchOutput("redpanda_common", redpandaCommonOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (
			output service.BatchOutput,
			batchPolicy service.BatchPolicy,
			maxInFlight int,
			err error,
		) {
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy(roFieldBatching); err != nil {
				return
			}
			output, err = kafka.NewFranzWriterFromConfig(conf, func(fn kafka.FranzSharedClientUseFn) error {
				return kafka.FranzSharedClientUse(sharedGlobalRedpandaClientKey, mgr, fn)
			}, func(context.Context) error { return nil })
			return
		})
	if err != nil {
		panic(err)
	}
}
