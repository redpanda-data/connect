package pure

import (
	"context"
	"errors"

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
		"fallback", service.NewConfigSpec().
			Stable().
			Categories("Utility").
			Version("3.58.0").
			Summary("Attempts to send each message to a child output, starting from the first output on the list. If an output attempt fails then the next output in the list is attempted, and so on.").
			Description(`
This pattern is useful for triggering events in the case where certain output targets have broken. For example, if you had an output type `+"`http_client`"+` but wished to reroute messages whenever the endpoint becomes unreachable you could use this pattern:

`+"```yaml"+`
output:
  fallback:
    - http_client:
        url: http://foo:4195/post/might/become/unreachable
        retries: 3
        retry_period: 1s
    - http_client:
        url: http://bar:4196/somewhere/else
        retries: 3
        retry_period: 1s
      processors:
        - mapping: 'root = "failed to send this message to foo: " + content()'
    - file:
        path: /usr/local/benthos/everything_failed.jsonl
`+"```"+`

### Metadata

When a given output fails the message routed to the following output will have a metadata value named `+"`fallback_error`"+` containing a string error message outlining the cause of the failure. The content of this string will depend on the particular output and can be used to enrich the message or provide information used to broker the data to an appropriate output using something like a `+"`switch`"+` output.

### Batching

When an output within a fallback sequence uses batching, like so:

`+"```yaml"+`
output:
  fallback:
    - aws_dynamodb:
        table: foo
        string_columns:
          id: ${!json("id")}
          content: ${!content()}
        batching:
          count: 10
          period: 1s
    - file:
        path: /usr/local/benthos/failed_stuff.jsonl
`+"```"+`

Benthos makes a best attempt at inferring which specific messages of the batch failed, and only propagates those individual messages to the next fallback tier.

However, depending on the output and the error returned it is sometimes not possible to determine the individual messages that failed, in which case the whole batch is passed to the next tier in order to preserve at-least-once delivery guarantees.`).
			Field(service.NewOutputListField("").Default([]any{})),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			var w *fallbackBroker
			if w, err = newFallbackFromParsed(conf); err != nil {
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

func newFallbackFromParsed(conf *service.ParsedConfig) (*fallbackBroker, error) {
	pOutputs, err := conf.FieldOutputList()
	if err != nil {
		return nil, err
	}
	if len(pOutputs) == 0 {
		return nil, ErrBrokerNoOutputs
	}

	outputs := make([]output.Streamed, len(pOutputs))
	for i, po := range pOutputs {
		outputs[i] = interop.UnwrapOwnedOutput(po)
	}

	var t *fallbackBroker
	if t, err = newFallbackBroker(outputs); err != nil {
		return nil, err
	}
	return t, nil
}

type fallbackBroker struct {
	transactions <-chan message.Transaction

	outputTSChans []chan message.Transaction
	outputs       []output.Streamed

	shutSig *shutdown.Signaller
}

func newFallbackBroker(outputs []output.Streamed) (*fallbackBroker, error) {
	t := &fallbackBroker{
		transactions: nil,
		outputs:      outputs,
		shutSig:      shutdown.NewSignaller(),
	}
	if len(outputs) == 0 {
		return nil, errors.New("missing outputs")
	}
	t.outputTSChans = make([]chan message.Transaction, len(t.outputs))
	for i := range t.outputTSChans {
		t.outputTSChans[i] = make(chan message.Transaction)
		if err := t.outputs[i].Consume(t.outputTSChans[i]); err != nil {
			return nil, err
		}
	}
	return t, nil
}

//------------------------------------------------------------------------------

// Consume assigns a new messages channel for the broker to read.
func (t *fallbackBroker) Consume(ts <-chan message.Transaction) error {
	if t.transactions != nil {
		return component.ErrAlreadyStarted
	}
	t.transactions = ts

	go t.loop()
	return nil
}

// Connected returns a boolean indicating whether this output is currently
// connected to its target.
func (t *fallbackBroker) Connected() bool {
	for _, out := range t.outputs {
		if !out.Connected() {
			return false
		}
	}
	return true
}

//------------------------------------------------------------------------------

// loop is an internal loop that brokers incoming messages to many outputs.
func (t *fallbackBroker) loop() {
	defer func() {
		for _, c := range t.outputTSChans {
			close(c)
		}
		_ = closeAllOutputs(context.Background(), t.outputs)
		t.shutSig.ShutdownComplete()
	}()

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

		outSorter, outBatch := message.NewSortGroup(tran.Payload)
		nextBatchFromErr := func(err error) message.Batch {
			var bErr *batch.Error
			if len(outBatch) <= 1 || !errors.As(err, &bErr) {
				tmpBatch := outBatch.ShallowCopy()
				for _, m := range tmpBatch {
					m.MetaSetMut("fallback_error", err.Error())
				}
				return tmpBatch
			}

			var onlyErrs message.Batch
			seenIndexes := map[int]struct{}{}
			bErr.WalkPartsBySource(outSorter, outBatch, func(i int, p *message.Part, err error) bool {
				if err != nil && p != nil {
					if _, exists := seenIndexes[i]; exists {
						return true
					}
					seenIndexes[i] = struct{}{}
					tmp := p.ShallowCopy()
					tmp.MetaSetMut("fallback_error", err.Error())
					onlyErrs = append(onlyErrs, tmp)
				}
				return true
			})

			// This is an edge case that means the only failed messages aren't
			// capable of being associated with our origin batch. To be safe we
			// fall everything through.
			if len(onlyErrs) == 0 {
				tmpBatch := outBatch.ShallowCopy()
				for _, m := range tmpBatch {
					m.MetaSetMut("fallback_error", err.Error())
				}
				return tmpBatch
			}

			outSorter, outBatch = message.NewSortGroup(onlyErrs)
			return outBatch
		}

		i := 0
		var ackFn func(ctx context.Context, err error) error
		ackFn = func(ctx context.Context, err error) error {
			i++
			if err == nil || len(t.outputTSChans) <= i {
				return tran.Ack(ctx, err)
			}

			select {
			case t.outputTSChans[i] <- message.NewTransactionFunc(nextBatchFromErr(err), ackFn):
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		}

		select {
		case t.outputTSChans[i] <- message.NewTransactionFunc(outBatch.ShallowCopy(), ackFn):
		case <-t.shutSig.CloseAtLeisureChan():
			return
		}
	}
}

func (t *fallbackBroker) TriggerCloseNow() {
	t.shutSig.CloseNow()
}

func (t *fallbackBroker) WaitForClose(ctx context.Context) error {
	select {
	case <-t.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func closeAllOutputs(ctx context.Context, outputs []output.Streamed) error {
	for _, o := range outputs {
		o.TriggerCloseNow()
	}
	for _, o := range outputs {
		if err := o.WaitForClose(ctx); err != nil {
			return err
		}
	}
	return nil
}
