package pure

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(func(c output.Config, nm bundle.NewManagement) (output.Streamed, error) {
		if c.DropOn.Output == nil {
			return nil, errors.New("cannot create a drop_on output without a child")
		}
		wrapped, err := nm.NewOutput(*c.DropOn.Output)
		if err != nil {
			return nil, err
		}
		return newDropOnWriter(c.DropOn.DropOnConditions, wrapped, nm.Logger())
	}), docs.ComponentSpec{
		Name:        "drop_on",
		Summary:     `Attempts to write messages to a child output and if the write fails for one of a list of configurable reasons the message is dropped instead of being reattempted.`,
		Description: `Regular Benthos outputs will apply back pressure when downstream services aren't accessible, and Benthos retries (or nacks) all messages that fail to be delivered. However, in some circumstances, or for certain output types, we instead might want to relax these mechanisms, which is when this output becomes useful.`,
		Categories: []string{
			"Utility",
		},
		Config: docs.FieldComponent().WithChildren(
			docs.FieldBool("error", "Whether messages should be dropped when the child output returns an error. For example, this could be when an http_client output gets a 4XX response code."),
			docs.FieldString("back_pressure", "An optional duration string that determines the maximum length of time to wait for a given message to be accepted by the child output before the message should be dropped instead. The most common reason for an output to block is when waiting for a lost connection to be re-established. Once a message has been dropped due to back pressure all subsequent messages are dropped immediately until the output is ready to process them again. Note that if `error` is set to `false` and this field is specified then messages dropped due to back pressure will return an error response.", "30s", "1m"),
			docs.FieldOutput("output", "A child output.").HasDefault(nil),
		).ChildDefaultAndTypesFromStruct(output.NewDropOnConfig()),
		Examples: []docs.AnnotatedExample{
			{
				Title:   "Dropping failed HTTP requests",
				Summary: "In this example we have a fan_out broker, where we guarantee delivery to our Kafka output, but drop messages if they fail our secondary HTTP client output.",
				Config: `
output:
  broker:
    pattern: fan_out
    outputs:
      - kafka:
          addresses: [ foobar:6379 ]
          topic: foo
      - drop_on:
          error: true
          output:
            http_client:
              url: http://example.com/foo/messages
              verb: POST
`,
			},
			{
				Title:   "Dropping from outputs that cannot connect",
				Summary: "Most outputs that attempt to establish and long-lived connection will apply back-pressure when the connection is lost. The following example has a websocket output where if it takes longer than 10 seconds to establish a connection, or recover a lost one, pending messages are dropped.",
				Config: `
output:
  drop_on:
    back_pressure: 10s
    output:
      websocket:
        url: ws://example.com/foo/messages
`,
			},
		},
	})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type dropOnWriter struct {
	log log.Modular

	onError        bool
	onBackpressure time.Duration
	wrapped        output.Streamed

	transactionsIn  <-chan message.Transaction
	transactionsOut chan message.Transaction

	shutSig *shutdown.Signaller
}

func newDropOnWriter(conf output.DropOnConditions, wrapped output.Streamed, log log.Modular) (*dropOnWriter, error) {
	var backPressure time.Duration
	if len(conf.BackPressure) > 0 {
		var err error
		if backPressure, err = time.ParseDuration(conf.BackPressure); err != nil {
			return nil, fmt.Errorf("failed to parse back_pressure duration: %w", err)
		}
	}

	return &dropOnWriter{
		log:             log,
		wrapped:         wrapped,
		transactionsOut: make(chan message.Transaction),

		onError:        conf.Error,
		onBackpressure: backPressure,

		shutSig: shutdown.NewSignaller(),
	}, nil
}

func (d *dropOnWriter) loop() {
	cnCtx, cnDone := d.shutSig.CloseNowCtx(context.Background())
	defer func() {
		close(d.transactionsOut)

		d.wrapped.TriggerCloseNow()
		_ = d.wrapped.WaitForClose(context.Background())

		d.shutSig.ShutdownComplete()
		cnDone()
	}()

	resChan := make(chan error)

	var gotBackPressure bool
	for {
		var ts message.Transaction
		var open bool
		select {
		case ts, open = <-d.transactionsIn:
			if !open {
				return
			}
		case <-d.shutSig.CloseNowChan():
			return
		}

		var res error
		if d.onBackpressure > 0 {
			if !func() bool {
				// Use a ticker here and call Stop explicitly.
				ticker := time.NewTicker(d.onBackpressure)
				defer ticker.Stop()

				if gotBackPressure {
					select {
					case d.transactionsOut <- message.NewTransaction(ts.Payload, resChan):
						gotBackPressure = false
					default:
					}
				} else {
					select {
					case d.transactionsOut <- message.NewTransaction(ts.Payload, resChan):
					case <-ticker.C:
						gotBackPressure = true
					case <-d.shutSig.CloseNowChan():
						return false
					}
				}
				if !gotBackPressure {
					select {
					case res = <-resChan:
					case <-ticker.C:
						gotBackPressure = true
						go func() {
							// We must pull the response that we're due, since
							// the component isn't being shut down.
							<-resChan
						}()
					case <-d.shutSig.CloseNowChan():
						return false
					}
				}
				if gotBackPressure {
					d.log.Warnln("Message dropped due to back pressure.")
					if d.onError {
						res = nil
					} else {
						res = fmt.Errorf("experienced back pressure beyond: %v", d.onBackpressure)
					}
				}
				return true
			}() {
				return
			}
		} else {
			// Push data as usual, if the output blocks due to a disconnect then
			// we wait as long as it takes.
			select {
			case d.transactionsOut <- message.NewTransaction(ts.Payload, resChan):
			case <-d.shutSig.CloseNowChan():
				return
			}
			select {
			case res = <-resChan:
			case <-d.shutSig.CloseNowChan():
				return
			}
		}

		if res != nil && d.onError {
			d.log.Warnf("Message dropped due to: %v\n", res)
			res = nil
		}

		if err := ts.Ack(cnCtx, res); err != nil && cnCtx.Err() != nil {
			return
		}
	}
}

func (d *dropOnWriter) Consume(ts <-chan message.Transaction) error {
	if d.transactionsIn != nil {
		return component.ErrAlreadyStarted
	}
	if err := d.wrapped.Consume(d.transactionsOut); err != nil {
		return err
	}
	d.transactionsIn = ts
	go d.loop()
	return nil
}

func (d *dropOnWriter) Connected() bool {
	return d.wrapped.Connected()
}

func (d *dropOnWriter) TriggerCloseNow() {
	d.shutSig.CloseNow()
}

func (d *dropOnWriter) WaitForClose(ctx context.Context) error {
	select {
	case <-d.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
