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

package test

import (
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/output"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

// BenchOutput is an output type that prints benchmark information to stdout.
type BenchOutput struct {
	running int32

	log   log.Modular
	stats metrics.Type

	period time.Duration

	transactions <-chan types.Transaction

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewBenchOutput creates a new BenchOutput output type.
func NewBenchOutput(
	period time.Duration,
	log log.Modular,
	stats metrics.Type,
) output.Type {
	return &BenchOutput{
		running:    1,
		log:        log.NewModule(".output.bench"),
		stats:      stats,
		period:     period,
		closeChan:  make(chan struct{}),
		closedChan: make(chan struct{}),
	}
}

//------------------------------------------------------------------------------

// loop is an internal loop that brokers incoming messages to output pipe.
func (o *BenchOutput) loop() {
	benchChan := StartPrintingBenchmarks(o.period)

	defer func() {
		o.stats.Decr("output.bench.running", 1)

		close(benchChan)
		close(o.closedChan)
	}()
	o.stats.Incr("output.bench.running", 1)

	for atomic.LoadInt32(&o.running) == 1 {
		var ts types.Transaction
		var open bool
		select {
		case ts, open = <-o.transactions:
			if !open {
				return
			}
			o.stats.Incr("output.bench.count", 1)
		case <-o.closeChan:
			return
		}

		benchMsg, err := BenchFromMessage(ts.Payload)
		if err != nil {
			o.log.Errorf("Failed to create bench: %v\n", err)
		} else {
			o.stats.Timing("output.bench.latency", int64(benchMsg.Latency))
			select {
			case benchChan <- benchMsg:
			case <-o.closeChan:
				return
			}
		}

		o.stats.Incr("output.bench.success", 1)
		select {
		case ts.ResponseChan <- types.NewSimpleResponse(nil):
		case <-o.closeChan:
			return
		}
	}
}

// Consume assigns a messages channel for the output to read.
func (o *BenchOutput) Consume(ts <-chan types.Transaction) error {
	if o.transactions != nil {
		return types.ErrAlreadyStarted
	}
	o.transactions = ts
	go o.loop()
	return nil
}

// CloseAsync shuts down the File output and stops processing messages.
func (o *BenchOutput) CloseAsync() {
	if atomic.CompareAndSwapInt32(&o.running, 1, 0) {
		close(o.closeChan)
	}
}

// WaitForClose blocks until the File output has closed down.
func (o *BenchOutput) WaitForClose(timeout time.Duration) error {
	select {
	case <-o.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
