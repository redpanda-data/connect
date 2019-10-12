// Copyright (c) 2017 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package input

import (
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/tls"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeKafkaBalanced] = TypeSpec{
		constructor:                  NewKafkaBalanced,
		constructorHasBatchProcessor: newKafkaBalancedHasBatchProcessor,
		description: `
Connects to a kafka (0.9+) server. Offsets are managed within kafka as per the
consumer group (set via config), and partitions are automatically balanced
across any members of the consumer group.

Partitions consumed by this input can be processed in parallel allowing it to
utilise <= N pipeline processing threads and parallel outputs where N is the
number of partitions allocated to this consumer.

The ` + "`batching`" + ` fields allow you to configure a
[batching policy](../batching.md#batch-policy) which will be applied per
partition. It is not currently possible to use
[broker based batching](../batching.md#combined-batching) with this input type.

The field ` + "`max_processing_period`" + ` should be set above the maximum
estimated time taken to process a message.

` + tls.Documentation + `

### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
- kafka_key
- kafka_topic
- kafka_partition
- kafka_offset
- kafka_lag
- kafka_timestamp_unix
- All existing message headers (version 0.11+)
` + "```" + `

The field ` + "`kafka_lag`" + ` is the calculated difference between the high
water mark offset of the partition at the time of ingestion and the current
message offset.

You can access these metadata fields using
[function interpolation](../config_interpolation.md#metadata).`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return sanitiseWithBatch(conf.KafkaBalanced, conf.KafkaBalanced.Batching)
		},
	}
}

//------------------------------------------------------------------------------

// NewKafkaBalanced creates a new KafkaBalanced input type.
func NewKafkaBalanced(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	// TODO: V4 Remove this.
	if conf.KafkaBalanced.MaxBatchCount > 1 {
		log.Warnf("Field '%v.max_batch_count' is deprecated, use '%v.batching.count' instead.\n", conf.Type, conf.Type)
		conf.KafkaBalanced.Batching.Count = conf.KafkaBalanced.MaxBatchCount
	}
	k, err := reader.NewKafkaCG(conf.KafkaBalanced, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	preserved := reader.NewAsyncPreserver(k)
	return NewAsyncReader("kafka_balanced", true, preserved, log, stats)
}

// DEPRECATED: This is a hack for until the batch processor is removed.
// TODO: V4 Remove this.
func newKafkaBalancedHasBatchProcessor(
	hasBatchProc bool,
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (Type, error) {
	if !hasBatchProc {
		return NewKafkaBalanced(conf, mgr, log, stats)
	}

	log.Warnln("Detected presence of a 'batch' processor, kafka_balanced input falling back to single threaded mode. To fix this use the 'batching' fields instead.")
	k, err := reader.NewKafkaBalanced(conf.KafkaBalanced, log, stats)
	if err != nil {
		return nil, err
	}
	return NewReader("kafka_balanced", reader.NewPreserver(k), log, stats)
}

//------------------------------------------------------------------------------
