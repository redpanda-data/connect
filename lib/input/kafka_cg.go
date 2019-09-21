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
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/tls"
	"gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeKafkaCG] = TypeSpec{
		constructor: NewKafkaCG,
		description: `
EXPERIMENTAL: This input is considered experimental and is therefore subject to
change outside of major version releases.

Connects to a Kafka (0.9+) server. Offsets are managed within kafka as per the
consumer group (set via config), and partitions are automatically balanced
across any members of the consumer group.

Partitions consumed by this client can be processed in parallel, meaning a
single instance of this input can utilise any number of threads within a
` + "`pipeline`" + ` section of a config.

WARNING: It is NOT safe to use a ` + "`batch`" + ` processor with this input, and it
will shut down if that is the case. Instead, configure an appropriate
[batch policy](../batching.md#batch_policy).

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
			batchSanit, err := batch.SanitisePolicyConfig(conf.KafkaCG.Batching)
			if err != nil {
				return nil, err
			}

			cBytes, err := yaml.Marshal(conf.KafkaCG)
			if err != nil {
				return nil, err
			}

			hashMap := map[string]interface{}{}
			if err = yaml.Unmarshal(cBytes, &hashMap); err != nil {
				return nil, err
			}

			hashMap["batching"] = batchSanit
			return hashMap, nil
		},
	}
}

//------------------------------------------------------------------------------

// NewKafkaCG creates a new KafkaCG input type.
func NewKafkaCG(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	k, err := reader.NewKafkaCG(conf.KafkaCG, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	return NewAsyncReader(TypeKafkaCG, false, reader.NewAsyncPreserver(k), log, stats)
}

//------------------------------------------------------------------------------
