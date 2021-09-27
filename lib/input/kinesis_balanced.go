package input

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/aws/session"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeKinesisBalanced] = TypeSpec{
		constructor: fromSimpleConstructor(NewKinesisBalanced),
		Status:      docs.StatusDeprecated,
		Summary: `
Receives messages from a Kinesis stream and automatically balances shards across
consumers.`,
		Description: `
## Alternatives

This input is being replaced with the shiny new ` + "[`aws_kinesis` input](/docs/components/inputs/aws_kinesis)" + `, which has improved features, consider trying it out instead.

### Metadata

This input adds the following metadata fields to each message:

` + "```text" + `
- kinesis_shard
- kinesis_partition_key
- kinesis_sequence_number
` + "```" + `

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#metadata).`,
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
		Categories: []Category{
			CategoryServices,
			CategoryAWS,
		},
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
