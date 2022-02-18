package output

import (
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeInproc] = TypeSpec{
		constructor: fromSimpleConstructor(NewInproc),
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
		Categories: []Category{
			CategoryUtility,
		},
		config: docs.FieldComponent().HasType(docs.FieldTypeString).HasDefault(""),
	}
}

//------------------------------------------------------------------------------

// InprocConfig contains configuration fields for the Inproc output type.
type InprocConfig string

// NewInprocConfig creates a new InprocConfig with default values.
func NewInprocConfig() InprocConfig {
	return InprocConfig("")
}

//------------------------------------------------------------------------------

// Inproc is an output type that serves Inproc messages.
type Inproc struct {
	running int32

	pipe  string
	mgr   interop.Manager
	log   log.Modular
	stats metrics.Type

	transactionsOut chan message.Transaction
	transactionsIn  <-chan message.Transaction

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewInproc creates a new Inproc output type.
func NewInproc(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (output.Streamed, error) {
	i := &Inproc{
		running:         1,
		pipe:            string(conf.Inproc),
		mgr:             mgr,
		log:             log,
		stats:           stats,
		transactionsOut: make(chan message.Transaction),
		closedChan:      make(chan struct{}),
		closeChan:       make(chan struct{}),
	}
	mgr.SetPipe(i.pipe, i.transactionsOut)
	return i, nil
}

//------------------------------------------------------------------------------

// loop is an internal loop that brokers incoming messages to output pipe.
func (i *Inproc) loop() {
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

// Consume assigns a messages channel for the output to read.
func (i *Inproc) Consume(ts <-chan message.Transaction) error {
	if i.transactionsIn != nil {
		return component.ErrAlreadyStarted
	}
	i.transactionsIn = ts
	go i.loop()
	return nil
}

// Connected returns a boolean indicating whether this output is currently
// connected to its target.
func (i *Inproc) Connected() bool {
	return true
}

// CloseAsync shuts down the Inproc output and stops processing messages.
func (i *Inproc) CloseAsync() {
	if atomic.CompareAndSwapInt32(&i.running, 1, 0) {
		close(i.closeChan)
	}
}

// WaitForClose blocks until the Inproc output has closed down.
func (i *Inproc) WaitForClose(timeout time.Duration) error {
	select {
	case <-i.closedChan:
	case <-time.After(timeout):
		return component.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
