// Copyright (c) 2019 Ashley Jeffs
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
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeDropOnError] = TypeSpec{
		constructor: NewDropOnError,
		description: `
Attempts to write messages to a child output and if the write fails for any
reason the message is dropped instead of being reattempted. This output can be
combined with a child ` + "`retry`" + ` output in order to set an explicit
number of retry attempts before dropping a message.

For example, the following configuration attempts to send to a hypothetical
output type ` + "`foo`" + ` three times, but if all three attempts fail the
message is dropped entirely:

` + "``` yaml" + `
output:
  drop_on_error:
    retry:
      max_retries: 2
      output:
        type: foo
` + "```" + ``,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			if conf.DropOnError.Config == nil {
				return struct{}{}, nil
			}
			return SanitiseConfig(*conf.DropOnError.Config)
		},
	}
}

//------------------------------------------------------------------------------

// DropOnErrorConfig contains configuration values for the DropOnError output
// type.
type DropOnErrorConfig struct {
	*Config `yaml:",inline" json:",inline"`
}

// NewDropOnErrorConfig creates a new DropOnErrorConfig with default values.
func NewDropOnErrorConfig() DropOnErrorConfig {
	return DropOnErrorConfig{
		Config: nil,
	}
}

//------------------------------------------------------------------------------

// MarshalJSON prints an empty object instead of nil.
func (d DropOnErrorConfig) MarshalJSON() ([]byte, error) {
	if d.Config != nil {
		return json.Marshal(d.Config)
	}
	return json.Marshal(struct{}{})
}

// MarshalYAML prints an empty object instead of nil.
func (d DropOnErrorConfig) MarshalYAML() (interface{}, error) {
	if d.Config != nil {
		return *d.Config, nil
	}
	return struct{}{}, nil
}

//------------------------------------------------------------------------------

// UnmarshalJSON ensures that when parsing child config it is initialised.
func (d *DropOnErrorConfig) UnmarshalJSON(bytes []byte) error {
	if d.Config == nil {
		nConf := NewConfig()
		d.Config = &nConf
	}

	return json.Unmarshal(bytes, d.Config)
}

// UnmarshalYAML ensures that when parsing child config it is initialised.
func (d *DropOnErrorConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	if d.Config == nil {
		nConf := NewConfig()
		d.Config = &nConf
	}

	return unmarshal(d.Config)
}

//------------------------------------------------------------------------------

// DropOnError is an output type that continuously writes a message to a child output
// until the send is successful.
type DropOnError struct {
	running int32

	wrapped Type

	stats metrics.Type
	log   log.Modular

	transactionsIn  <-chan types.Transaction
	transactionsOut chan types.Transaction

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewDropOnError creates a new DropOnError input type.
func NewDropOnError(
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (Type, error) {
	if conf.DropOnError.Config == nil {
		return nil, errors.New("cannot create a drop_on_error output without a child")
	}

	wrapped, err := New(*conf.DropOnError.Config, mgr, log, stats)
	if err != nil {
		return nil, fmt.Errorf("failed to create output '%v': %v", conf.DropOnError.Config.Type, err)
	}

	return &DropOnError{
		running: 1,

		log:             log,
		stats:           stats,
		wrapped:         wrapped,
		transactionsOut: make(chan types.Transaction),

		closeChan:  make(chan struct{}),
		closedChan: make(chan struct{}),
	}, nil
}

//------------------------------------------------------------------------------

func (d *DropOnError) loop() {
	// Metrics paths
	var (
		mDropped      = d.stats.GetCounter("drop_on_error.dropped")
		mDroppedBatch = d.stats.GetCounter("drop_on_error.batch.dropped")
	)

	defer func() {
		close(d.transactionsOut)
		d.wrapped.CloseAsync()
		err := d.wrapped.WaitForClose(time.Second)
		for ; err != nil; err = d.wrapped.WaitForClose(time.Second) {
		}
		close(d.closedChan)
	}()

	resChan := make(chan types.Response)

	for atomic.LoadInt32(&d.running) == 1 {
		var ts types.Transaction
		var open bool
		select {
		case ts, open = <-d.transactionsIn:
			if !open {
				return
			}
		case <-d.closeChan:
			return
		}

		select {
		case d.transactionsOut <- types.NewTransaction(ts.Payload, resChan):
		case <-d.closeChan:
			return
		}

		var res types.Response
		select {
		case res = <-resChan:
		case <-d.closeChan:
			return
		}

		if res.Error() != nil {
			mDropped.Incr(int64(ts.Payload.Len()))
			mDroppedBatch.Incr(1)
			d.log.Warnf("Message dropped due to: %v\n", res.Error())
		}

		select {
		case ts.ResponseChan <- response.NewAck():
		case <-d.closeChan:
			return
		}
	}
}

// Consume assigns a messages channel for the output to read.
func (d *DropOnError) Consume(ts <-chan types.Transaction) error {
	if d.transactionsIn != nil {
		return types.ErrAlreadyStarted
	}
	if err := d.wrapped.Consume(d.transactionsOut); err != nil {
		return err
	}
	d.transactionsIn = ts
	go d.loop()
	return nil
}

// Connected returns a boolean indicating whether this output is currently
// connected to its target.
func (d *DropOnError) Connected() bool {
	return d.wrapped.Connected()
}

// CloseAsync shuts down the DropOnError input and stops processing requests.
func (d *DropOnError) CloseAsync() {
	if atomic.CompareAndSwapInt32(&d.running, 1, 0) {
		close(d.closeChan)
	}
}

// WaitForClose blocks until the DropOnError input has closed down.
func (d *DropOnError) WaitForClose(timeout time.Duration) error {
	select {
	case <-d.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
