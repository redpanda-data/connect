package pure

import (
	"context"

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
		return newInprocOutput(c, nm, nm.Logger())
	}), docs.ComponentSpec{
		Name: "inproc",
		Description: `
Sends data directly to Benthos inputs by connecting to a unique ID. This allows
you to hook up isolated streams whilst running Benthos in
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

type inprocOutput struct {
	pipe string
	mgr  bundle.NewManagement
	log  log.Modular

	transactionsOut chan message.Transaction
	transactionsIn  <-chan message.Transaction

	shutSig *shutdown.Signaller
}

func newInprocOutput(conf output.Config, mgr bundle.NewManagement, log log.Modular) (output.Streamed, error) {
	i := &inprocOutput{
		pipe:            conf.Inproc,
		mgr:             mgr,
		log:             log,
		transactionsOut: make(chan message.Transaction),
		shutSig:         shutdown.NewSignaller(),
	}
	mgr.SetPipe(i.pipe, i.transactionsOut)
	return i, nil
}

func (i *inprocOutput) loop() {
	defer func() {
		i.mgr.UnsetPipe(i.pipe, i.transactionsOut)
		close(i.transactionsOut)
		i.shutSig.ShutdownComplete()
	}()

	i.log.Infof("Sending inproc messages to ID: %s\n", i.pipe)

	var open bool
	for {
		var ts message.Transaction
		select {
		case ts, open = <-i.transactionsIn:
			if !open {
				return
			}
		case <-i.shutSig.CloseNowChan():
			return
		}

		select {
		case i.transactionsOut <- ts:
		case <-i.shutSig.CloseNowChan():
			return
		}
	}
}

func (i *inprocOutput) Consume(ts <-chan message.Transaction) error {
	if i.transactionsIn != nil {
		return component.ErrAlreadyStarted
	}
	i.transactionsIn = ts
	go i.loop()
	return nil
}

func (i *inprocOutput) Connected() bool {
	return true
}

func (i *inprocOutput) TriggerCloseNow() {
	i.shutSig.CloseNow()
}

func (i *inprocOutput) WaitForClose(ctx context.Context) error {
	select {
	case <-i.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
