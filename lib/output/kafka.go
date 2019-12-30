// Copyright (c) 2014 Ashley Jeffs
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

package output

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/tls"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeKafka] = TypeSpec{
		constructor: NewKafka,
		Description: `
The kafka output type writes a batch of messages to Kafka brokers and waits for
acknowledgement before propagating it back to the input. The config field
` + "`ack_replicas`" + ` determines whether we wait for acknowledgement from all
replicas or just a single broker.

It is possible to specify a compression codec to use out of the following
options: ` + "`none`, `snappy`, `lz4` and `gzip`" + `.

Both the ` + "`key` and `topic`" + ` fields can be dynamically set using
function interpolations described [here](../config_interpolation.md#functions).
When sending batched messages these interpolations are performed per message
part.

The ` + "`partitioner`" + ` field determines how messages are delegated to a
partition. Options are ` + "`fnv1a_hash`, `murmur2_hash`, `random`" + ` and
` + "`round_robin`" + `. When a hash partitioner is selected but a message key
is empty then a random partition is chosen.

The field ` + "`round_robin_partitions`" + ` is deprecated.

` + tls.Documentation + ``,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return sanitiseWithBatch(conf.Kafka, conf.Kafka.Batching)
		},
		Async:   true,
		Batches: true,
	}
}

//------------------------------------------------------------------------------

// NewKafka creates a new Kafka output type.
func NewKafka(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	k, err := writer.NewKafka(conf.Kafka, log, stats)
	if err != nil {
		return nil, err
	}
	var w Type
	if conf.Kafka.MaxInFlight == 1 {
		w, err = NewWriter(
			TypeKafka, k, log, stats,
		)
	} else {
		w, err = NewAsyncWriter(
			TypeKafka, conf.Kafka.MaxInFlight, k, log, stats,
		)
	}
	if bconf := conf.Kafka.Batching; err == nil && !bconf.IsNoop() {
		policy, err := batch.NewPolicy(bconf, mgr, log.NewModule(".batching"), metrics.Namespaced(stats, "batching"))
		if err != nil {
			return nil, fmt.Errorf("failed to construct batch policy: %v", err)
		}
		w = NewBatcher(policy, w, log, stats)
	}
	return w, err
}

//------------------------------------------------------------------------------
