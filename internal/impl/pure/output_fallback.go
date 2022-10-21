package pure

import (
	"context"
	"errors"
	"strconv"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(newFallback), docs.ComponentSpec{
		Name:    "fallback",
		Version: "3.58.0",
		Summary: `
Attempts to send each message to a child output, starting from the first output on the list. If an output attempt fails then the next output in the list is attempted, and so on.`,
		Description: `
This pattern is useful for triggering events in the case where certain output targets have broken. For example, if you had an output type ` + "`http_client`" + ` but wished to reroute messages whenever the endpoint becomes unreachable you could use this pattern:

` + "```yaml" + `
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
` + "```" + `

### Metadata

When a given output fails the message routed to the following output will have a metadata value named ` + "`fallback_error`" + ` containing a string error message outlining the cause of the failure. The content of this string will depend on the particular output and can be used to enrich the message or provide information used to broker the data to an appropriate output using something like a ` + "`switch`" + ` output.

### Batching

When an output within a fallback sequence uses batching, like so:

` + "```yaml" + `
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
` + "```" + `

Benthos makes a best attempt at inferring which specific messages of the batch failed, and only propagates those individual messages to the next fallback tier.

However, depending on the output and the error returned it is sometimes not possible to determine the individual messages that failed, in which case the whole batch is passed to the next tier in order to preserve at-least-once delivery guarantees.`,
		Categories: []string{
			"Utility",
		},
		Config: docs.FieldOutput("", "").Array(),
	})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

func newFallback(conf output.Config, mgr bundle.NewManagement) (output.Streamed, error) {
	outputConfs := conf.Fallback
	if len(outputConfs) == 0 {
		return nil, ErrBrokerNoOutputs
	}
	outputs := make([]output.Streamed, len(outputConfs))

	var err error
	for i, oConf := range outputConfs {
		oMgr := mgr.IntoPath("fallback", strconv.Itoa(i))
		if outputs[i], err = oMgr.NewOutput(oConf); err != nil {
			return nil, err
		}
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

		i := 0
		var ackFn func(ctx context.Context, err error) error
		ackFn = func(ctx context.Context, err error) error {
			i++
			if err == nil || len(t.outputTSChans) <= i {
				return tran.Ack(ctx, err)
			}
			newPayload := tran.Payload.ShallowCopy()
			_ = newPayload.Iter(func(i int, p *message.Part) error {
				p.MetaSetMut("fallback_error", err.Error())
				return nil
			})
			select {
			case t.outputTSChans[i] <- message.NewTransactionFunc(newPayload, ackFn):
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		}

		select {
		case t.outputTSChans[i] <- message.NewTransactionFunc(tran.Payload.ShallowCopy(), ackFn):
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
