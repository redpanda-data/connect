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

package output

import (
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["inproc"] = TypeSpec{
		constructor: NewInproc,
		description: `
Sends data directly to inputs by connecting to a unique ID. This allows you to
hook up isolated streams whilst running Benthos in
` + "[`--streams` mode](../streams/README.md) mode" + `, it is NOT recommended
that you connect the inputs of a stream with an output of the same stream, as
feedback loops can lead to deadlocks in your message flow.

It is possible to connect multiple inputs to the same inproc ID, but only one
output can connect to an inproc ID, and will replace existing outputs if a
collision occurs.`,
	}
}

//------------------------------------------------------------------------------

// InprocConfig is configuration for the Inproc output type.
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
	mgr   types.Manager
	log   log.Modular
	stats metrics.Type

	transactionsOut chan types.Transaction
	transactionsIn  <-chan types.Transaction

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewInproc creates a new Inproc output type.
func NewInproc(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	i := &Inproc{
		running:         1,
		pipe:            string(conf.Inproc),
		mgr:             mgr,
		log:             log.NewModule(".output.inproc"),
		stats:           stats,
		transactionsOut: make(chan types.Transaction),
		closedChan:      make(chan struct{}),
		closeChan:       make(chan struct{}),
	}
	return i, nil
}

//------------------------------------------------------------------------------

// loop is an internal loop that brokers incoming messages to output pipe.
func (i *Inproc) loop() {
	var (
		mRunning  = i.stats.GetCounter("output.inproc." + i.pipe + ".running")
		mRunningF = i.stats.GetCounter("output.running")
		mCount    = i.stats.GetCounter("output.inproc." + i.pipe + ".count")
		mCountF   = i.stats.GetCounter("output.count")
	)

	defer func() {
		mRunning.Decr(1)
		mRunningF.Decr(1)
		atomic.StoreInt32(&i.running, 0)
		i.mgr.UnsetPipe(i.pipe, i.transactionsOut)
		close(i.transactionsOut)
		close(i.closedChan)
	}()
	mRunning.Incr(1)
	mRunningF.Incr(1)

	i.mgr.SetPipe(i.pipe, i.transactionsOut)
	i.log.Infof("Sending inproc messages to ID: %s\n", i.pipe)

	var open bool
	for atomic.LoadInt32(&i.running) == 1 {
		var ts types.Transaction
		select {
		case ts, open = <-i.transactionsIn:
			if !open {
				return
			}
		case <-i.closeChan:
			return
		}

		mCount.Incr(1)
		mCountF.Incr(1)
		select {
		case i.transactionsOut <- ts:
		case <-i.closeChan:
			return
		}
	}
}

// Consume assigns a messages channel for the output to read.
func (i *Inproc) Consume(ts <-chan types.Transaction) error {
	if i.transactionsIn != nil {
		return types.ErrAlreadyStarted
	}
	i.transactionsIn = ts
	go i.loop()
	return nil
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
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
