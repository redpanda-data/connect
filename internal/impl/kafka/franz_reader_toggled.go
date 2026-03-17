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
	"github.com/Jeffail/shutdown"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	krtFieldUnordered                = "unordered_processing"
	krtFieldUnorderedEnabled         = "enabled"
	krtFieldUnorderedCheckpointLimit = "checkpoint_limit"
	krtFieldUnorderedBatching        = "batching"
)

// FranzReaderToggledConfigFields returns config fields for customising the
// behaviour of kafka reader with a toggle between ordered and unordered
// processing.
func FranzReaderToggledConfigFields() []*service.ConfigField {
	return append(
		FranzReaderOrderedConfigFields(),
		service.NewObjectField(krtFieldUnordered,
			service.NewBoolField(krtFieldUnorderedEnabled).
				Description("Whether to enable the unordered processing of messages from a given partition.").
				Default(false),
			service.NewIntField(krtFieldUnorderedCheckpointLimit).
				Description("Determines how many messages of the same partition can be processed in parallel before applying back pressure. When a message of a given offset is delivered to the output the offset is only allowed to be committed when all messages of prior offsets have also been delivered, this ensures at-least-once delivery guarantees. However, this mechanism also increases the likelihood of duplicates in the event of crashes or server faults, reducing the checkpoint limit will mitigate this.").
				Default(1024),
			service.NewBatchPolicyField(krtFieldUnorderedBatching).
				Description("Allows you to configure a xref:configuration:batching.adoc[batching policy] that applies to individual topic partitions in order to batch messages together before flushing them for processing. Batching can be beneficial for performance as well as useful for windowed processing, and doing so this way preserves the ordering of topic partitions."),
		).
			Description("Configures partition consumers to allow parallel and therefore unordered processing of messages of any given partition. This allows for better utilization of processing threads and asynchronous publishing at the output level. The maximum parallelization of each partition is determined by the checkpoint_limit field.").
			Advanced(),
	)
}

// NewFranzReaderToggledFromConfig attempts to instantiate a new franz reader
// from a parsed config using fields that allow for toggling between ordered
// and unordered modes.
func NewFranzReaderToggledFromConfig(conf *service.ParsedConfig, res *service.Resources, optsFn func() ([]kgo.Opt, error)) (service.BatchInput, error) {
	unorderedConf := conf.Namespace(krtFieldUnordered)

	unordered, err := unorderedConf.FieldBool(krtFieldUnorderedEnabled)
	if err != nil {
		return nil, err
	}
	if unordered {
		f := FranzReaderUnordered{
			res:     res,
			log:     res.Logger(),
			shutSig: shutdown.NewSignaller(),

			clientOpts:         optsFn,
			franzRecordToMsgFn: FranzRecordToMessageV1,
		}

		var err error
		if f.checkpointLimit, err = unorderedConf.FieldInt(krtFieldUnorderedCheckpointLimit); err != nil {
			return nil, err
		}

		if f.batchPolicy, err = unorderedConf.FieldBatchPolicy(krtFieldUnorderedBatching); err != nil {
			return nil, err
		}

		f.consumerGroup, _ = conf.FieldString(kroFieldConsumerGroup)

		if f.commitPeriod, err = conf.FieldDuration(kroFieldCommitPeriod); err != nil {
			return nil, err
		}

		if f.topicLagRefreshPeriod, err = conf.FieldDuration(kroFieldTopicLagRefreshPeriod); err != nil {
			return nil, err
		}

		return &f, nil
	}

	return NewFranzReaderOrderedFromConfig(conf, res, optsFn)
}
