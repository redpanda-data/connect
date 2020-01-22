package input

import (
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/aws/session"
	"github.com/Jeffail/benthos/v3/lib/x/docs"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSQS] = TypeSpec{
		constructor: NewAmazonSQS,
		Summary: `
Receive messages from an Amazon SQS URL.`,
		Description: `
### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](/docs/guides/aws).

### Metadata

This input adds the following metadata fields to each message:

` + "```text" + `
- sqs_message_id
- sqs_receipt_handle
- sqs_approximate_receive_count
- All message attributes
` + "```" + `

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#metadata).`,
		FieldSpecs: append(
			append(docs.FieldSpecs{
				docs.FieldCommon("url", "The SQS URL to consume from."),
				docs.FieldAdvanced("delete_message", "Whether to delete the consumed message once it is acked. Disabling allows you to handle the deletion using a different mechanism."),
			}, session.FieldSpecs()...),
			docs.FieldAdvanced("timeout", "The period of time to wait before abandoning a request and trying again."),
			docs.FieldAdvanced("max_number_of_messages", "The maximum number of messages to consume from each request."),
		),
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
