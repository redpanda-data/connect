package output

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeReject] = TypeSpec{
		constructor: fromSimpleConstructor(func(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (output.Streamed, error) {
			f, err := newRejectWriter(mgr, string(conf.Reject))
			if err != nil {
				return nil, err
			}
			return NewAsyncWriter(TypeReject, 1, f, log, stats)
		}),
		Status: docs.StatusStable,
		Summary: `
Rejects all messages, treating them as though the output destination failed to publish them.`,
		Description: `
The routing of messages after this output depends on the type of input it came from. For inputs that support propagating nacks upstream such as AMQP or NATS the message will be nacked. However, for inputs that are sequential such as files or Kafka the messages will simply be reprocessed from scratch.

If you're still scratching your head as to when this output could be useful check out [the examples below](#examples).`,
		Categories: []Category{
			CategoryUtility,
		},
		Examples: []docs.AnnotatedExample{
			{
				Title: "Rejecting Failed Messages",
				Summary: `
This input is particularly useful for routing messages that have failed during processing, where instead of routing them to some sort of dead letter queue we wish to push the error upstream. We can do this with a switch broker:`,
				Config: `
output:
  switch:
    retry_until_success: false
    cases:
      - check: '!errored()'
        output:
          amqp_1:
            url: amqps://guest:guest@localhost:5672/
            target_address: queue:/the_foos

      - output:
          reject: "processing failed due to: ${! error() }"
`,
			},
		},
		config: docs.FieldComponent().HasType(docs.FieldTypeString).HasDefault(""),
	}
}

//------------------------------------------------------------------------------

// RejectConfig contains configuration fields for the file based output type.
type RejectConfig string

// NewRejectConfig creates a new RejectConfig with default values.
func NewRejectConfig() RejectConfig {
	return RejectConfig("")
}

type rejectWriter struct {
	errExpr *field.Expression
}

func newRejectWriter(mgr interop.Manager, errorString string) (*rejectWriter, error) {
	if errorString == "" {
		return nil, errors.New("an error message must be provided in order to provide context for the rejection")
	}
	errExpr, err := mgr.BloblEnvironment().NewField(errorString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse error expression: %w", err)
	}
	return &rejectWriter{errExpr}, nil
}

func (w *rejectWriter) ConnectWithContext(ctx context.Context) error {
	return nil
}

func (w *rejectWriter) WriteWithContext(ctx context.Context, msg *message.Batch) error {
	errStr := w.errExpr.String(0, msg)
	return errors.New(errStr)
}

func (w *rejectWriter) CloseAsync() {
}

func (w *rejectWriter) WaitForClose(timeout time.Duration) error {
	return nil
}
