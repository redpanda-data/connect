package input

import (
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSQS] = TypeSpec{
		constructor: NewAmazonSQS,
		Description: `
Receive messages from an Amazon SQS URL, only the body is extracted into
messages.

Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
` + "`pipeline`" + ` section of a config.

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](../aws.md).

### Metadata

This input adds the following metadata fields to each message:

` + "```text" + `
- sqs_message_id
- sqs_receipt_handle
- sqs_approximate_receive_count
- All message attributes
` + "```" + `

You can access these metadata fields using
[function interpolation](../config_interpolation.md#metadata).`,
	}
}

//------------------------------------------------------------------------------

// NewAmazonSQS creates a new AWS SQS input type.
func NewAmazonSQS(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	s, err := reader.NewAmazonSQS(conf.SQS, log, stats)
	if err != nil {
		return nil, err
	}
	return NewAsyncReader(TypeSQS, true, reader.NewAsyncBundleUnacks(s), log, stats)
}

//------------------------------------------------------------------------------
