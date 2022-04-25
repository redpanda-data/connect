package pure

import (
	"sync/atomic"
	"time"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/interop"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	ooutput "github.com/benthosdev/benthos/v4/internal/old/output"
)

func init() {
	err := bundle.AllOutputs.Add(bundle.OutputConstructorFromSimple(func(c ooutput.Config, nm bundle.NewManagement) (output.Streamed, error) {
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
	running int32

	pipe string
	mgr  interop.Manager
	log  log.Modular

	transactionsOut chan message.Transaction
	transactionsIn  <-chan message.Transaction

	closedChan chan struct{}
	closeChan  chan struct{}
}

func newInprocOutput(conf ooutput.Config, mgr interop.Manager, log log.Modular) (output.Streamed, error) {
	i := &inprocOutput{
		running:         1,
		pipe:            conf.Inproc,
		mgr:             mgr,
		log:             log,
		transactionsOut: make(chan message.Transaction),
		closedChan:      make(chan struct{}),
		closeChan:       make(chan struct{}),
	}
	mgr.SetPipe(i.pipe, i.transactionsOut)
	return i, nil
}

func (i *inprocOutput) loop() {
	defer func() {
		atomic.StoreInt32(&i.running, 0)
		i.mgr.UnsetPipe(i.pipe, i.transactionsOut)
		close(i.transactionsOut)
		close(i.closedChan)
	}()

	i.log.Infof("Sending inproc messages to ID: %s\n", i.pipe)

	var open bool
	for atomic.LoadInt32(&i.running) == 1 {
		var ts message.Transaction
		select {
		case ts, open = <-i.transactionsIn:
			if !open {
				return
			}
		case <-i.closeChan:
			return
		}

		select {
		case i.transactionsOut <- ts:
		case <-i.closeChan:
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

func (i *inprocOutput) CloseAsync() {
	if atomic.CompareAndSwapInt32(&i.running, 1, 0) {
		close(i.closeChan)
	}
}

func (i *inprocOutput) WaitForClose(timeout time.Duration) error {
	select {
	case <-i.closedChan:
	case <-time.After(timeout):
		return component.ErrTimeout
	}
	return nil
}
