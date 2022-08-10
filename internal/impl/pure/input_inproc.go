package pure

import (
	"context"
	"time"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/input/processors"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
)

func init() {
	err := bundle.AllInputs.Add(processors.WrapConstructor(func(c input.Config, nm bundle.NewManagement) (input.Streamed, error) {
		proc := &inprocInput{
			pipe:         string(c.Inproc),
			mgr:          nm,
			log:          nm.Logger(),
			stats:        nm.Metrics(),
			transactions: make(chan message.Transaction),
			shutSig:      shutdown.NewSignaller(),
		}

		go proc.loop()
		return proc, nil
	}), docs.ComponentSpec{
		Name: "inproc",
		Description: `
Directly connect to an output within a Benthos process by referencing it by a
chosen ID. This allows you to hook up isolated streams whilst running Benthos in
` + "[streams mode](/docs/guides/streams_mode/about)" + `, it is NOT recommended
that you connect the inputs of a stream with an output of the same stream, as
feedback loops can lead to deadlocks in your message flow.

It is possible to connect multiple inputs to the same inproc ID, resulting in
messages dispatching in a round-robin fashion to connected inputs. However, only
one output can assume an inproc ID, and will replace existing outputs if a
collision occurs.`,
		Categories: []string{
			"Utility",
		},
		Config: docs.FieldString("", "").HasDefault(""),
	})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type inprocInput struct {
	pipe  string
	mgr   bundle.NewManagement
	stats metrics.Type
	log   log.Modular

	transactions chan message.Transaction

	shutSig *shutdown.Signaller
}

func (i *inprocInput) loop() {
	defer func() {
		close(i.transactions)
		i.shutSig.ShutdownComplete()
	}()

	var inprocChan <-chan message.Transaction

messageLoop:
	for !i.shutSig.ShouldCloseAtLeisure() {
		if inprocChan == nil {
			for {
				var err error
				if inprocChan, err = i.mgr.GetPipe(i.pipe); err != nil {
					i.log.Errorf("Failed to connect to inproc output '%v': %v\n", i.pipe, err)
					select {
					case <-time.After(time.Second):
					case <-i.shutSig.CloseAtLeisureChan():
						return
					}
				} else {
					i.log.Infof("Receiving inproc messages from ID: %s\n", i.pipe)
					break
				}
			}
		}
		select {
		case t, open := <-inprocChan:
			if !open {
				inprocChan = nil
				continue messageLoop
			}
			select {
			case i.transactions <- t:
			case <-i.shutSig.CloseAtLeisureChan():
				return
			}
		case <-i.shutSig.CloseAtLeisureChan():
			return
		}
	}
}

func (i *inprocInput) TransactionChan() <-chan message.Transaction {
	return i.transactions
}

func (i *inprocInput) Connected() bool {
	return true
}

func (i *inprocInput) TriggerStopConsuming() {
	i.shutSig.CloseAtLeisure()
}

func (i *inprocInput) TriggerCloseNow() {
	i.shutSig.CloseNow()
}

func (i *inprocInput) WaitForClose(ctx context.Context) error {
	select {
	case <-i.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
