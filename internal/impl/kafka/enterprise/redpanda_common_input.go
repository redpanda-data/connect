// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package enterprise

import (
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/kafka"
	"github.com/redpanda-data/connect/v4/internal/license"
)

func redpandaCommonInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Deprecated().
		Categories("Services").
		Summary("Consumes data from a Redpanda (Kafka) broker, using credentials defined in a common top-level `redpanda` config block.").
		Fields(
			slices.Concat(
				kafka.FranzConsumerFields(),
				kafka.FranzReaderOrderedConfigFields(),
				[]*service.ConfigField{
					service.NewAutoRetryNacksToggleField(),
					service.NewForceTimelyNacksField(),
				},
			)...,
		).
		Description(kafka.RedpandaInputDescription(`
output:
  fallback:
    - redpanda_common:
        topic: foo
    - retry:
        output:
          redpanda_common:
            topic: foo_dlq
`, "`fetch_max_bytes`, `fetch_max_partition_bytes`, and `max_yield_batch_bytes`")).
		LintRule(kafka.FranzConsumerFieldLintRules)
}

func init() {
	service.MustRegisterBatchInput("redpanda_common", redpandaCommonInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			if err := license.CheckRunningEnterprise(mgr); err != nil {
				return nil, err
			}

			tmpOpts, err := kafka.FranzConsumerOptsFromConfig(conf)
			if err != nil {
				return nil, err
			}

			var rdr service.BatchInput
			if rdr, err = kafka.NewFranzReaderOrderedFromConfig(conf, mgr, func() (clientOpts []kgo.Opt, err error) {
				// Make multiple attempts here just to allow the redpanda logger
				// to initialise in the background. Otherwise we get an annoying
				// log.
				for range 20 {
					if err = kafka.FranzSharedClientUse(kafka.SharedGlobalRedpandaClientKey, mgr, func(details *kafka.FranzSharedClientInfo) error {
						clientOpts = append(clientOpts, details.ConnDetails.FranzOpts()...)
						return nil
					}); err == nil {
						clientOpts = append(clientOpts, tmpOpts...)
						return
					}
					time.Sleep(time.Millisecond * 100)
				}
				return
			}); err != nil {
				return nil, err
			}

			if rdr, err = service.AutoRetryNacksBatchedToggled(conf, rdr); err != nil {
				return nil, err
			}

			if rdr, err = service.ForceTimelyNacksBatched(conf, rdr); err != nil {
				return nil, err
			}

			return rdr, nil
		})
}
