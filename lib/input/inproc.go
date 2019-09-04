// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package input

import (
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeInproc] = TypeSpec{
		constructor: NewInproc,
		description: `
Directly connect to an output within a Benthos process by referencing it by a
chosen ID. This allows you to hook up isolated streams whilst running Benthos in
` + "[`--streams` mode](../streams/README.md) mode" + `, it is NOT recommended
that you connect the inputs of a stream with an output of the same stream, as
feedback loops can lead to deadlocks in your message flow.

It is possible to connect multiple inputs to the same inproc ID, but only one
output can connect to an inproc ID, and will replace existing outputs if a
collision occurs.`,
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
	mgr   types.Manager
	stats metrics.Type
	log   log.Modular

	transactions chan types.Transaction

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewInproc creates a new Inproc input type.
func NewInproc(
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (Type, error) {
	proc := &Inproc{
		running:      1,
		pipe:         string(conf.Inproc),
		mgr:          mgr,
		log:          log,
		stats:        stats,
		transactions: make(chan types.Transaction),
		closeChan:    make(chan struct{}),
		closedChan:   make(chan struct{}),
	}

	go proc.loop()
	return proc, nil
}

//------------------------------------------------------------------------------

func (i *Inproc) loop() {
	// Metrics paths
	var (
		mRunning    = i.stats.GetGauge("running")
		mRcvd       = i.stats.GetCounter("batch.received")
		mPartsRcvd  = i.stats.GetCounter("received")
		mConn       = i.stats.GetCounter("connection.up")
		mFailedConn = i.stats.GetCounter("connection.failed")
		mLostConn   = i.stats.GetCounter("connection.lost")
		mCount      = i.stats.GetCounter("count")
	)

	defer func() {
		mRunning.Decr(1)
		close(i.transactions)
		close(i.closedChan)
	}()
	mRunning.Incr(1)

	var inprocChan <-chan types.Transaction

messageLoop:
	for atomic.LoadInt32(&i.running) == 1 {
		if inprocChan == nil {
			for {
				var err error
				if inprocChan, err = i.mgr.GetPipe(i.pipe); err != nil {
					mFailedConn.Incr(1)
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
			mConn.Incr(1)
		}
		select {
		case t, open := <-inprocChan:
			if !open {
				mLostConn.Incr(1)
				inprocChan = nil
				continue messageLoop
			}
			mCount.Incr(1)
			mRcvd.Incr(1)
			mPartsRcvd.Incr(int64(t.Payload.Len()))
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
func (i *Inproc) TransactionChan() <-chan types.Transaction {
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
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
