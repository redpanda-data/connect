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
	"github.com/redpanda-data/connect/v4/internal/license"
)

func redpandaCommonOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Deprecated().
		Version("4.39.0").
		Categories("Services").
		Summary("Sends data to a Redpanda (Kafka) broker, using credentials defined in a common top-level `redpanda` config block.").
		Description("This output is deprecated as of version 4.68.0. Use the xref:components:outputs/redpanda.adoc[`redpanda` output] instead. When you omit its `seed_brokers` field, the `redpanda` output takes its connection configuration from the top-level `redpanda` block, as this output does.").
		Fields(kafka.FranzWriterConfigFields()...).
		Fields(
			service.NewOutputMaxInFlightField().
				Default(10),
			service.NewBatchPolicyField(roFieldBatching),
		).
		LintRule(kafka.FranzWriterConfigLints())
}

const (
	roFieldBatching = "batching"
)

func init() {
	service.MustRegisterBatchOutput("redpanda_common", redpandaCommonOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (
			output service.BatchOutput,
			batchPolicy service.BatchPolicy,
			maxInFlight int,
			err error,
		) {
			if err = license.CheckRunningEnterprise(mgr); err != nil {
				return
			}

			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy(roFieldBatching); err != nil {
				return
			}
			output, err = kafka.NewFranzWriterFromConfig(
				conf,
				kafka.NewFranzWriterHooks(
					func(_ context.Context, fn kafka.FranzSharedClientUseFn) error {
						return kafka.FranzSharedClientUse(kafka.SharedGlobalRedpandaClientKey, mgr, fn)
					}).
					WithYieldClientFn(
						func(context.Context) error { return nil }),
			)
			return
		})
}
