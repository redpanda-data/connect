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
	Constructors[TypeKinesis] = TypeSpec{
		constructor: NewKinesis,
		Summary: `
Receive messages from a Kinesis stream.`,
		Description: `
It's possible to use DynamoDB for persisting shard iterators by setting the
table name. Offsets will then be tracked per ` + "`client_id`" + ` per
` + "`shard_id`" + `. When using this mode you should create a table with
` + "`namespace`" + ` as the primary key and ` + "`shard_id`" + ` as a sort key.

Use the ` + "`batching`" + ` fields to configure an optional
[batching policy](/docs/configuration/batching#batch-policy). Any other batching
mechanism will stall with this input due its sequential transaction model.`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return sanitiseWithBatch(conf.Kinesis, conf.Kinesis.Batching)
		},
		FieldSpecs: append(
			append(docs.FieldSpecs{
				docs.FieldCommon("stream", "The Kinesis stream to consume from."),
				docs.FieldCommon("shard", "The shard to consume from."),
				docs.FieldCommon("client_id", "The client identifier to assume."),
				docs.FieldCommon("commit_period", "The rate at which offset commits should be sent."),
				docs.FieldCommon("dynamodb_table", "A DynamoDB table to use for offset storage."),
				docs.FieldCommon("start_from_oldest", "Whether to consume from the oldest message when an offset does not yet exist for the stream."),
			}, session.FieldSpecs()...),
			docs.FieldAdvanced("timeout", "The period of time to wait before abandoning a request and trying again."),
			docs.FieldAdvanced("limit", "The maximum number of messages to consume from each request."),
			batch.FieldSpec(),
		),
		Categories: []Category{
			CategoryServices,
			CategoryAWS,
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
