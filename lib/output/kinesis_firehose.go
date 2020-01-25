package output

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/aws/session"
	"github.com/Jeffail/benthos/v3/lib/util/retries"
	"github.com/Jeffail/benthos/v3/lib/x/docs"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeKinesisFirehose] = TypeSpec{
		constructor: NewKinesisFirehose,
		Description: `
Sends messages to a Kinesis Firehose delivery stream.

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](/docs/guides/aws).`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return sanitiseWithBatch(conf.KinesisFirehose, conf.KinesisFirehose.Batching)
		},
		Async:   true,
		Batches: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("stream", "The stream to publish messages to."),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			batch.FieldSpec(),
		}.Merge(session.FieldSpecs()).Merge(retries.FieldSpecs()),
	}
}

//------------------------------------------------------------------------------

// NewKinesisFirehose creates a new KinesisFirehose output type.
func NewKinesisFirehose(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	kin, err := writer.NewKinesisFirehose(conf.KinesisFirehose, log, stats)
	if err != nil {
		return nil, err
	}
	var w Type
	if conf.KinesisFirehose.MaxInFlight == 1 {
		w, err = NewWriter(
			TypeKinesisFirehose, kin, log, stats,
		)
	} else {
		w, err = NewAsyncWriter(
			TypeKinesisFirehose, conf.KinesisFirehose.MaxInFlight, kin, log, stats,
		)
	}
	if bconf := conf.KinesisFirehose.Batching; err == nil && !bconf.IsNoop() {
		policy, err := batch.NewPolicy(bconf, mgr, log.NewModule(".batching"), metrics.Namespaced(stats, "batching"))
		if err != nil {
			return nil, fmt.Errorf("failed to construct batch policy: %v", err)
		}
		w = NewBatcher(policy, w, log, stats)
	}
	return w, err
}

//------------------------------------------------------------------------------
