package input

import (
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/aws/session"
	"github.com/Jeffail/benthos/v3/lib/x/docs"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeKinesisBalanced] = TypeSpec{
		constructor: NewKinesisBalanced,
		Summary: `
BETA: This input is a beta component and is subject to change outside of major
version releases.

Receives messages from a Kinesis stream and automatically balances shards across
consumers.`,
		Description: `
Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
` + "`pipeline`" + ` section of a config.

Use the ` + "`batching`" + ` fields to configure an optional
[batching policy](/docs/configuration/batching#batch-policy).

### Metadata

This input adds the following metadata fields to each message:

` + "```text" + `
- kinesis_shard
- kinesis_partition_key
- kinesis_sequence_number
` + "```" + `

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#metadata).`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return sanitiseWithBatch(conf.KinesisBalanced, conf.KinesisBalanced.Batching)
		},
		FieldSpecs: append(
			append(docs.FieldSpecs{
				docs.FieldCommon("stream", "The Kinesis stream to consume from."),
				docs.FieldCommon("dynamodb_table", "A DynamoDB table to use for offset storage."),
				docs.FieldAdvanced("dynamodb_billing_mode", "A billing mode to set for the offset DynamoDB table."),
				docs.FieldAdvanced("dynamodb_read_provision", "The read capacity of the offset DynamoDB table."),
				docs.FieldAdvanced("dynamodb_write_provision", "The write capacity of the offset DynamoDB table."),
				docs.FieldCommon("start_from_oldest", "Whether to consume from the oldest message when an offset does not yet exist for the stream."),
			}, session.FieldSpecs()...),
			batch.FieldSpec(),
			docs.FieldDeprecated("max_batch_count"),
		),
	}
}

//------------------------------------------------------------------------------

// NewKinesisBalanced creates a new AWS KinesisBalanced input type.
func NewKinesisBalanced(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	// TODO: V4 Remove this.
	if conf.KinesisBalanced.MaxBatchCount > 1 {
		log.Warnf("Field '%v.max_batch_count' is deprecated, use '%v.batching.count' instead.\n", conf.Type, conf.Type)
		conf.KinesisBalanced.Batching.Count = conf.KinesisBalanced.MaxBatchCount
	}
	var k reader.Async
	var err error
	if k, err = reader.NewKinesisBalanced(conf.KinesisBalanced, log, stats); err != nil {
		return nil, err
	}
	if k, err = reader.NewAsyncBatcher(conf.KinesisBalanced.Batching, k, mgr, log, stats); err != nil {
		return nil, err
	}
	k = reader.NewAsyncBundleUnacks(reader.NewAsyncPreserver(k))
	return NewAsyncReader(TypeKinesisBalanced, true, k, log, stats)
}

//------------------------------------------------------------------------------
