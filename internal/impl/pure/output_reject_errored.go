package pure

import (
	"context"
	"errors"
	"fmt"

	"github.com/benthosdev/benthos/v4/internal/batch"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/public/service"
)

func init() {
	err := service.RegisterBatchOutput(
		"reject_errored", service.NewConfigSpec().
			Stable().
			Categories("Utility").
			Summary(`Rejects messages that have failed their processing steps, resulting in nack behaviour at the input level, otherwise sends them to a child output.`).
			Description(`
The routing of messages rejected by this output depends on the type of input it came from. For inputs that support propagating nacks upstream such as AMQP or NATS the message will be nacked. However, for inputs that are sequential such as files or Kafka the messages will simply be reprocessed from scratch.`).
			Example(
				"Rejecting Failed Messages",
				`
The most straight forward use case for this output type is to nack messages that have failed their processing steps. In this example our mapping might fail, in which case the messages that failed are rejected and will be nacked by our input:`,
				`
input:
  nats_jetstream:
    urls: [ nats://127.0.0.1:4222 ]
    subject: foos.pending

pipeline:
  processors:
    - mutation: 'root.age = this.fuzzy.age.int64()'

output:
  reject_errored:
    nats_jetstream:
      urls: [ nats://127.0.0.1:4222 ]
      subject: foos.processed
`,
			).
			Example(
				"DLQing Failed Messages",
				`
Another use case for this output is to send failed messages straight into a dead-letter queue. We use it within a [fallback output](/docs/components/outputs/fallback) that allows us to specify where these failed messages should go to next.`,
				`
pipeline:
  processors:
    - mutation: 'root.age = this.fuzzy.age.int64()'

output:
  fallback:
    - reject_errored:
        http_client:
          url: http://foo:4195/post/might/become/unreachable
          retries: 3
          retry_period: 1s
    - http_client:
        url: http://bar:4196/somewhere/else
        retries: 3
        retry_period: 1s
`,
			).
			Field(service.NewOutputField("")),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			var w *rejectErroredBroker
			if w, err = newRejectErroredFromParsed(conf, mgr); err != nil {
				return
			}

			out = interop.NewUnwrapInternalOutput(w)
			return
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

func newRejectErroredFromParsed(conf *service.ParsedConfig, res *service.Resources) (*rejectErroredBroker, error) {
	pOutput, err := conf.FieldOutput()
	if err != nil {
		return nil, err
	}

	output := interop.UnwrapOwnedOutput(pOutput)

	var t *rejectErroredBroker
	if t, err = newRejectErroredBroker(output, res); err != nil {
		return nil, err
	}
	return t, nil
}

type rejectErroredBroker struct {
	log *service.Logger

	transactions <-chan message.Transaction

	outputTSChan chan message.Transaction
	output       output.Streamed

	shutSig *shutdown.Signaller
}

func newRejectErroredBroker(output output.Streamed, res *service.Resources) (*rejectErroredBroker, error) {
	t := &rejectErroredBroker{
		log:          res.Logger(),
		transactions: nil,
		output:       output,
		shutSig:      shutdown.NewSignaller(),
	}
	t.outputTSChan = make(chan message.Transaction)
	if err := t.output.Consume(t.outputTSChan); err != nil {
		return nil, err
	}
	return t, nil
}

//------------------------------------------------------------------------------

// Consume assigns a new messages channel for the broker to read.
func (t *rejectErroredBroker) Consume(ts <-chan message.Transaction) error {
	if t.transactions != nil {
		return component.ErrAlreadyStarted
	}
	t.transactions = ts

	go t.loop()
	return nil
}

// Connected returns a boolean indicating whether this output is currently
// connected to its target.
func (t *rejectErroredBroker) Connected() bool {
	return t.output.Connected()
}

//------------------------------------------------------------------------------

// loop is an internal loop that brokers incoming messages to many outputs.
func (t *rejectErroredBroker) loop() {
	defer func() {
		close(t.outputTSChan)
		t.output.TriggerCloseNow()
		_ = t.output.WaitForClose(context.Background())
		t.shutSig.ShutdownComplete()
	}()

	closeNowCtx, done := t.shutSig.CloseNowCtx(context.Background())
	defer done()

	for {
		var open bool
		var tran message.Transaction

		select {
		case tran, open = <-t.transactions:
			if !open {
				return
			}
		case <-t.shutSig.CloseAtLeisureChan():
			return
		}

		if len(tran.Payload) == 1 {
			// No need for pretentious batch fluffery when there's only one
			// message.
			if err := tran.Payload[0].ErrorGet(); err != nil {
				if aerr := tran.Ack(closeNowCtx, fmt.Errorf("rejecting due to failed processing: %w", err)); aerr != nil {
					t.log.With("error", aerr).Warn("Failed to nack rejected message")
				}
			} else {
				select {
				case t.outputTSChan <- tran:
				case <-t.shutSig.CloseNowChan():
					return
				}
			}
			continue
		}

		// Check for any failed messages in the batch.
		var batchErr *batch.Error
		for i, m := range tran.Payload {
			err := m.ErrorGet()
			if err == nil {
				continue
			}
			err = fmt.Errorf("rejecting due to failed processing: %w", err)
			if batchErr == nil {
				batchErr = batch.NewError(tran.Payload, err)
			}
			batchErr.Failed(i, err)
		}

		// If no messages failed we can pass the batch through unchanged.
		if batchErr == nil {
			select {
			case t.outputTSChan <- tran:
			case <-t.shutSig.CloseNowChan():
				return
			}
			continue
		}

		// If all messages failed then we can nack the entire batch immediately.
		if batchErr.IndexedErrors() == len(tran.Payload) {
			_ = tran.Ack(closeNowCtx, batchErr)
			continue
		}

		// If we get here it means that we have a batch of messages that mixes
		// rejected and non-rejected. This is an awkward place to be because if
		// a nack were to come back from the transaction we need to merge it
		// into our existing batch error. For this we need a sort group.
		sortGroup, sortedBatch := message.NewSortGroup(tran.Payload)

		// Reduce batch down into only those we aren't rejecting.
		forwardBatch := make(message.Batch, 0, len(tran.Payload)-batchErr.IndexedErrors())
		batchErr.WalkPartsNaively(func(i int, _ *message.Part, err error) bool {
			if err == nil {
				forwardBatch = append(forwardBatch, sortedBatch[i])
			}
			return true
		})

		select {
		case t.outputTSChan <- message.NewTransactionFunc(forwardBatch, func(ctx context.Context, err error) error {
			if err == nil {
				// An ack is simpler, we return the batch error containing our
				// rejections and then move on.
				return tran.Ack(ctx, batchErr)
			}

			var tmpBatchErr *batch.Error
			if errors.As(err, &tmpBatchErr) {
				tmpBatchErr.WalkPartsBySource(sortGroup, sortedBatch, func(i int, p *message.Part, err error) bool {
					if err != nil {
						batchErr.Failed(i, err)
					}
					return true
				})
				return tran.Ack(ctx, batchErr)
			}

			// If the nack returned isn't a batch error then it's batch-wide,
			// this means all messages were either rejected or failed to be
			// delivered.
			for _, p := range forwardBatch {
				if i := sortGroup.GetIndex(p); i >= 0 {
					batchErr.Failed(i, err)
				}
			}
			return tran.Ack(ctx, batchErr)
		}):
		case <-t.shutSig.CloseNowChan():
			return
		}
	}
}

func (t *rejectErroredBroker) TriggerCloseNow() {
	t.shutSig.CloseNow()
}

func (t *rejectErroredBroker) WaitForClose(ctx context.Context) error {
	select {
	case <-t.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
