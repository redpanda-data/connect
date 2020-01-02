package output

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSQS] = TypeSpec{
		constructor: NewAmazonSQS,
		Description: `
Sends messages to an SQS queue. Metadata values are sent along with the payload
as attributes with the data type String. If the number of metadata values in a
message exceeds the message attribute limit (10) then the top ten keys ordered
alphabetically will be selected.

The fields ` + "`message_group_id` and `message_deduplication_id`" + ` can be
set dynamically using
[function interpolations](../config_interpolation.md#functions), which are
resolved individually for each message of a batch.

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](../aws.md).`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return sanitiseWithBatch(conf.SQS, conf.SQS.Batching)
		},
		Async:   true,
		Batches: true,
	}
}

//------------------------------------------------------------------------------

// NewAmazonSQS creates a new AmazonSQS output type.
func NewAmazonSQS(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	s, err := writer.NewAmazonSQS(conf.SQS, log, stats)
	if err != nil {
		return nil, err
	}
	var w Type
	if conf.SQS.MaxInFlight == 1 {
		w, err = NewWriter(
			TypeSQS, s, log, stats,
		)
	} else {
		w, err = NewAsyncWriter(
			TypeSQS, conf.SQS.MaxInFlight, s, log, stats,
		)
	}
	if bconf := conf.SQS.Batching; err == nil && !bconf.IsNoop() {
		policy, err := batch.NewPolicy(bconf, mgr, log.NewModule(".batching"), metrics.Namespaced(stats, "batching"))
		if err != nil {
			return nil, fmt.Errorf("failed to construct batch policy: %v", err)
		}
		w = NewBatcher(policy, w, log, stats)
	}
	return w, err
}

//------------------------------------------------------------------------------
