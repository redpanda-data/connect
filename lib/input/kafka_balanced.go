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
	"github.com/Jeffail/benthos/lib/input/reader"
	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/tls"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeKafkaBalanced] = TypeSpec{
		constructor: NewKafkaBalanced,
		description: `
Connects to a kafka (0.9+) server. Offsets are managed within kafka as per the
consumer group (set via config), and partitions are automatically balanced
across any members of the consumer group.

The field ` + "`max_batch_size`" + ` specifies the maximum number of prefetched
messages to be batched together. When more than one message is batched they can
be split into individual messages with the ` + "`split`" + ` processor.

` + tls.Documentation + `

### Metadata

This input adds the following metadata fields to each message:

` + "```" + `
- kafka_key
- kafka_topic
- kafka_partition
- kafka_offset
- kafka_timestamp_unix
- All existing message headers (version 0.11+)
` + "```" + `

You can access these metadata fields using
[function interpolation](../config_interpolation.md#metadata).`,
	}
}

//------------------------------------------------------------------------------

// NewKafkaBalanced creates a new KafkaBalanced input type.
func NewKafkaBalanced(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	k, err := reader.NewKafkaBalanced(conf.KafkaBalanced, log, stats)
	if err != nil {
		return nil, err
	}
	return NewReader("kafka_balanced", reader.NewPreserver(k), log, stats)
}

//------------------------------------------------------------------------------
