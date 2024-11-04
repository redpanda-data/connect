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
	"context"
	"slices"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	roFieldMaxInFlight = "max_in_flight"
)

func redpandaOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Summary("A Kafka output using the https://github.com/twmb/franz-go[Franz Kafka client library^].").
		Description(`
Writes a batch of messages to Kafka brokers and waits for acknowledgement before propagating it back to the input.
`).
		Fields(redpandaOutputConfigFields()...).
		LintRule(`
root = if this.partitioner == "manual" {
if this.partition.or("") == "" {
"a partition must be specified when the partitioner is set to manual"
}
} else if this.partition.or("") != "" {
"a partition cannot be specified unless the partitioner is set to manual"
}`)
}

func redpandaOutputConfigFields() []*service.ConfigField {
	return slices.Concat(
		FranzConnectionFields(),
		FranzWriterConfigFields(),
		[]*service.ConfigField{
			service.NewIntField(roFieldMaxInFlight).
				Description("The maximum number of batches to be sending in parallel at any given time.").
				Default(256),
		},
		FranzProducerFields(),
	)
}

func init() {
	err := service.RegisterBatchOutput("redpanda", redpandaOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (
			output service.BatchOutput,
			batchPolicy service.BatchPolicy,
			maxInFlight int,
			err error,
		) {
			if maxInFlight, err = conf.FieldInt(roFieldMaxInFlight); err != nil {
				return
			}

			var tmpOpts, clientOpts []kgo.Opt

			var connDetails *FranzConnectionDetails
			if connDetails, err = FranzConnectionDetailsFromConfig(conf, mgr.Logger()); err != nil {
				return
			}
			clientOpts = append(clientOpts, connDetails.FranzOpts()...)

			if tmpOpts, err = FranzProducerOptsFromConfig(conf); err != nil {
				return
			}
			clientOpts = append(clientOpts, tmpOpts...)

			clientOpts = append(clientOpts, kgo.AllowAutoTopicCreation()) // TODO: Configure this?

			var client *kgo.Client

			output, err = NewFranzWriterFromConfig(conf, func(fn FranzSharedClientUseFn) error {
				if client == nil {
					var err error
					if client, err = kgo.NewClient(clientOpts...); err != nil {
						return err
					}
				}
				return fn(&FranzSharedClientInfo{
					Client:      client,
					ConnDetails: connDetails,
				})
			}, func(context.Context) error {
				if client == nil {
					return nil
				}
				client.Close()
				client = nil
				return nil
			})
			return
		})
	if err != nil {
		panic(err)
	}
}
