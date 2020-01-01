package input

import (
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeKinesis] = TypeSpec{
		constructor: NewKinesis,
		Description: `
Receive messages from a Kinesis stream.

It's possible to use DynamoDB for persisting shard iterators by setting the
table name. Offsets will then be tracked per ` + "`client_id`" + ` per
` + "`shard_id`" + `. When using this mode you should create a table with
` + "`namespace`" + ` as the primary key and ` + "`shard_id`" + ` as a sort key.

Use the ` + "`batching`" + ` fields to configure an optional
[batching policy](../batching.md#batch-policy). It is not currently possible to
use [broker based batching](../batching.md#combined-batching) with this input
type.

This input currently provides a single continuous feed of data, and therefore
by default will only utilise a single processing thread and parallel output.
Take a look at the
[pipelines documentation](../pipeline.md#single-consumer-without-buffer) for
guides on how to work around this.

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](../aws.md).`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return sanitiseWithBatch(conf.Kinesis, conf.Kinesis.Batching)
		},
	}
}

//------------------------------------------------------------------------------

// NewKinesis creates a new AWS Kinesis input type.
func NewKinesis(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	k, err := reader.NewKinesis(conf.Kinesis, log, stats)
	if err != nil {
		return nil, err
	}
	var kb reader.Type = k
	if !conf.Kinesis.Batching.IsNoop() {
		if kb, err = reader.NewSyncBatcher(conf.Kinesis.Batching, k, mgr, log, stats); err != nil {
			return nil, err
		}
	}
	return NewReader(
		TypeKinesis,
		reader.NewPreserver(kb),
		log, stats,
	)
}

//------------------------------------------------------------------------------
