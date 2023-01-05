package pure

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/transaction"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(func(c output.Config, nm bundle.NewManagement) (output.Streamed, error) {
		return output.NewAsyncWriter("sync_response", 1, SyncResponseWriter{}, nm)
	}), docs.ComponentSpec{
		Name: "sync_response",
		Summary: `
Returns the final message payload back to the input origin of the message, where
it is dealt with according to that specific input type.`,
		Description: `
For most inputs this mechanism is ignored entirely, in which case the sync
response is dropped without penalty. It is therefore safe to use this output
even when combining input types that might not have support for sync responses.
An example of an input able to utilise this is the ` + "`http_server`" + `.

It is safe to combine this output with others using broker types. For example,
with the ` + "`http_server`" + ` input we could send the payload to a Kafka
topic and also send a modified payload back with:

` + "```yaml" + `
input:
  http_server:
    path: /post
output:
  broker:
    pattern: fan_out
    outputs:
      - kafka:
          addresses: [ TODO:9092 ]
          topic: foo_topic
      - sync_response: {}
        processors:
          - mapping: 'root = content().uppercase()'
` + "```" + `

Using the above example and posting the message 'hello world' to the endpoint
` + "`/post`" + ` Benthos would send it unchanged to the topic
` + "`foo_topic`" + ` and also respond with 'HELLO WORLD'.

For more information please read [Synchronous Responses](/docs/guides/sync_responses).`,
		Categories: []string{
			"Utility",
		},
		Config: docs.FieldObject("", "").HasDefault(struct{}{}),
	})
	if err != nil {
		panic(err)
	}
}

// SyncResponseWriter is a writer implementation that adds messages to a
// ResultStore located in the context of the first message part of each batch.
// This is essentially a mechanism that returns the result of a pipeline
// directly back to the origin of the message.
type SyncResponseWriter struct{}

// Connect is a noop.
func (s SyncResponseWriter) Connect(ctx context.Context) error {
	return nil
}

// WriteBatch writes a message batch to a ResultStore located in the first
// message of the batch.
func (s SyncResponseWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	return transaction.SetAsResponse(msg)
}

// Close is a noop.
func (s SyncResponseWriter) Close(context.Context) error {
	return nil
}
