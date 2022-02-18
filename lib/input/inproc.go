package input

import (
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/component/input"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func init() {
	Constructors[TypeInproc] = TypeSpec{
		constructor: fromSimpleConstructor(NewInproc),
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
		Categories: []Category{
			CategoryUtility,
		},
		config: docs.FieldComponent().HasType(docs.FieldTypeString).HasDefault(""),
	}
}

//------------------------------------------------------------------------------

// InprocConfig is a configuration type for the inproc input.
type InprocConfig string

// NewInprocConfig creates a new inproc input config.
func NewInprocConfig() InprocConfig {
	return InprocConfig("")
}

//------------------------------------------------------------------------------

// Inproc is an input type that reads from a named pipe, which could be the
// output of a separate Benthos stream of the same process.
type Inproc struct {
	running int32

	pipe  string
	mgr   interop.Manager
	stats metrics.Type
	log   log.Modular

	transactions chan message.Transaction

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewInproc creates a new Inproc input type.
func NewInproc(
	conf Config,
	mgr interop.Manager,
	log log.Modular,
	stats metrics.Type,
) (input.Streamed, error) {
	proc := &Inproc{
		running:      1,
		pipe:         string(conf.Inproc),
		mgr:          mgr,
		log:          log,
		stats:        stats,
		transactions: make(chan message.Transaction),
		closeChan:    make(chan struct{}),
		closedChan:   make(chan struct{}),
	}

	go proc.loop()
	return proc, nil
}

//------------------------------------------------------------------------------

func (i *Inproc) loop() {
	defer func() {
		close(i.transactions)
		close(i.closedChan)
	}()

	var inprocChan <-chan message.Transaction

messageLoop:
	for atomic.LoadInt32(&i.running) == 1 {
		if inprocChan == nil {
			for {
				var err error
				if inprocChan, err = i.mgr.GetPipe(i.pipe); err != nil {
					i.log.Errorf("Failed to connect to inproc output '%v': %v\n", i.pipe, err)
					select {
					case <-time.After(time.Second):
					case <-i.closeChan:
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
			case <-i.closeChan:
				return
			}
		case <-i.closeChan:
			return
		}
	}
}

// TransactionChan returns a transactions channel for consuming messages from
// this input type.
func (i *Inproc) TransactionChan() <-chan message.Transaction {
	return i.transactions
}

// Connected returns a boolean indicating whether this input is currently
// connected to its target.
func (i *Inproc) Connected() bool {
	return true
}

// CloseAsync shuts down the Inproc input and stops processing requests.
func (i *Inproc) CloseAsync() {
	if atomic.CompareAndSwapInt32(&i.running, 1, 0) {
		close(i.closeChan)
	}
}

// WaitForClose blocks until the Inproc input has closed down.
func (i *Inproc) WaitForClose(timeout time.Duration) error {
	select {
	case <-i.closedChan:
	case <-time.After(timeout):
		return component.ErrTimeout
	}
	return nil
}
