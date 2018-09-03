// Copyright (c) 2014 Ashley Jeffs
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
	"errors"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/processor/condition"
	"github.com/Jeffail/benthos/lib/response"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/throttle"
)

//------------------------------------------------------------------------------

var (
	// ErrSwitchNoConditionMet is returned when a message does not match any
	// output conditions.
	ErrSwitchNoConditionMet = errors.New("no switch output conditions were met by message")
	// ErrSwitchNoOutputs is returned when creating a Switch type with less than
	// 2 outputs.
	ErrSwitchNoOutputs = errors.New("attempting to create switch with less than 2 outputs")
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSwitch] = TypeSpec{
		constructor: NewSwitch,
		description: `
The switch output type allows you to configure multiple conditional output targets
by listing child outputs paired with conditions. In the following example, messages
containing "foo" will be sent to both the ` + "`foo`" + ` and ` + "`baz`" + ` outputs. Messages containing
"bar" will be sent to both the ` + "`bar`" + ` and ` + "`baz`" + ` outputs. Messages containing both "foo"
and "bar" will be sent to all three outputs. And finally, messages that do not contain
"foo" or "bar" will be sent to the ` + "`baz`" + ` output only.

` + "``` yaml" + `
output:
  type: switch
  switch:
    outputs:
    - output:
        type: foo
        foo:
          foo_field_1: value1
      condition:
        type: text
        text:
          operator: contains
          part: 0
          arg: foo
      fallthrough: true
    - output:
        type: bar
        bar:
          bar_field_1: value2
          bar_field_2: value3
      condition:
        type: text
        text:
          operator: contains
          part: 0
          arg: bar
      fallthrough: true
    - output:
        type: baz
        baz:
          baz_field_1: value4
        processors:
        - type: baz_processor
  processors:
  - type: some_processor
` + "```" + `

The switch output requires a minimum of two outputs. If no condition is defined for
an output, it behaves like a static ` + "`true`" + ` condition. If ` + "`fallthrough`" + ` is set to
` + "`true`" + `, the swith output will continue evaluating additional outputs after finding
a match. If an output applies back pressure it will block all subsequent messages,
and if an output fails to send a message, it will be retried continously until
completion or service shut down. Messages that do not match any outputs will be
dropped.
		`,
	}
}

//------------------------------------------------------------------------------

// SwitchConfig contains configuration fields for the Switch output type.
type SwitchConfig struct {
	Outputs []switchConfigOutputs `json:"outputs" yaml:"outputs"`
}

// NewSwitchConfig creates a new SwitchConfig with default values.
func NewSwitchConfig() SwitchConfig {
	return SwitchConfig{}
}

type switchConfigOutputs struct {
	cond        types.Condition
	Condition   *condition.Config `json:"condition" yaml:"condition"`
	Fallthrough bool              `json:"fallthrough" yaml:"fallthrough"`
	Output      Config            `json:"output" yaml:"output"`
}

//------------------------------------------------------------------------------

// Switch is a broker that implements types.Consumer and broadcasts each message
// out to an array of outputs.
type Switch struct {
	running int32

	logger log.Modular
	stats  metrics.Type

	throt *throttle.Type

	transactions <-chan types.Transaction

	outputTsChans  []chan types.Transaction
	outputResChans []chan types.Response
	outputs        []types.Output
	outputNs       []int
	confs          []switchConfigOutputs
	typeStr        string

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewSwitch creates a new FanOut type by providing outputs. Messages will be sent to
// a subset of outputs according to condition and fallthrough settings
func NewSwitch(
	conf Config,
	mgr types.Manager,
	logger log.Modular,
	stats metrics.Type,
) (Type, error) {
	lOutputs := len(conf.Switch.Outputs)
	if lOutputs < 2 {
		return nil, ErrSwitchNoOutputs
	}

	outputs := make([]types.Output, lOutputs)
	confs := make([]switchConfigOutputs, lOutputs)
	var err error
	for i, oConf := range conf.Switch.Outputs {
		outputs[i], err = New(oConf.Output, mgr, logger, stats)
		if err != nil {
			return nil, err
		}
		if oConf.Condition != nil {
			oConf.cond, err = condition.New(*oConf.Condition, mgr, logger, stats)
			if err != nil {
				return nil, err
			}
			confs[i] = oConf
		}
	}

	return newSwitch(outputs, confs, mgr, logger, stats)
}

func newSwitch(
	outputs []types.Output,
	confs []switchConfigOutputs,
	mgr types.Manager,
	logger log.Modular,
	stats metrics.Type,
) (*Switch, error) {
	o := &Switch{
		running:      1,
		stats:        stats,
		logger:       logger.NewModule(".broker.switch"),
		transactions: nil,
		outputs:      outputs,
		outputNs:     []int{},
		confs:        confs,
		closedChan:   make(chan struct{}),
		closeChan:    make(chan struct{}),
	}

	o.throt = throttle.New(throttle.OptCloseChan(o.closeChan))

	o.outputTsChans = make([]chan types.Transaction, len(o.outputs))
	o.outputResChans = make([]chan types.Response, len(o.outputs))
	for i := range o.outputTsChans {
		o.outputNs = append(o.outputNs, i)
		o.outputTsChans[i] = make(chan types.Transaction)
		o.outputResChans[i] = make(chan types.Response)
		if err := o.outputs[i].Consume(o.outputTsChans[i]); err != nil {
			return nil, err
		}
	}
	return o, nil
}

//------------------------------------------------------------------------------

// Consume assigns a new transactions channel for the broker to read.
func (o *Switch) Consume(transactions <-chan types.Transaction) error {
	if o.transactions != nil {
		return types.ErrAlreadyStarted
	}
	o.transactions = transactions

	go o.loop()
	return nil
}

//------------------------------------------------------------------------------

// loop is an internal loop that brokers incoming messages to many outputs.
func (o *Switch) loop() {
	var (
		mCount         = o.stats.GetCounter("output.count")
		mCountF        = o.stats.GetCounter("output." + o.typeStr + ".count")
		mDropped       = o.stats.GetCounter("output.send.dropped")
		mDroppedF      = o.stats.GetCounter("output." + o.typeStr + ".send.dropped")
		mError         = o.stats.GetCounter("output.send.error")
		mErrorF        = o.stats.GetCounter("output." + o.typeStr + ".send.error")
		mPartsCount    = o.stats.GetCounter("output.parts.count")
		mPartsCountF   = o.stats.GetCounter("output." + o.typeStr + ".parts.count")
		mPartsDropped  = o.stats.GetCounter("output.parts.send.dropped")
		mPartsDroppedF = o.stats.GetCounter("output." + o.typeStr + ".parts.send.dropped")
		mPartsError    = o.stats.GetCounter("output.parts.send.error")
		mPartsErrorF   = o.stats.GetCounter("output." + o.typeStr + ".parts.send.error")
		mPartsSuccess  = o.stats.GetCounter("output.parts.send.success")
		mPartsSuccessF = o.stats.GetCounter("output." + o.typeStr + ".parts.send.success")
		mRunning       = o.stats.GetGauge("output.running")
		mRunningF      = o.stats.GetGauge("output." + o.typeStr + ".running")
		mSuccess       = o.stats.GetCounter("output.send.success")
		mSuccessF      = o.stats.GetCounter("output." + o.typeStr + ".send.success")
	)

	defer func() {
		for i, output := range o.outputs {
			output.CloseAsync()
			close(o.outputTsChans[i])
		}
		for _, output := range o.outputs {
			if err := output.WaitForClose(time.Second); err != nil {
				for err != nil {
					err = output.WaitForClose(time.Second)
				}
			}
		}
		mRunning.Decr(1)
		mRunningF.Decr(1)
		close(o.closedChan)
	}()
	mRunning.Incr(1)
	mRunningF.Incr(1)

	for atomic.LoadInt32(&o.running) == 1 {
		var ts types.Transaction
		var open bool

		select {
		case ts, open = <-o.transactions:
			if !open {
				return
			}
		case <-o.closeChan:
			return
		}
		mCount.Incr(1)
		mCountF.Incr(1)
		lParts := int64(ts.Payload.Len())
		mPartsCount.Incr(lParts)
		mPartsCountF.Incr(lParts)

		var outputTargets []int
		for i, oConf := range o.confs {
			if oConf.cond == nil || oConf.cond.Check(ts.Payload) {
				outputTargets = append(outputTargets, o.outputNs[i])
				if !oConf.Fallthrough {
					break
				}
			}
		}
		if len(outputTargets) == 0 {
			select {
			case ts.ResponseChan <- response.NewAck():
				mDropped.Incr(1)
				mDroppedF.Incr(1)
				mPartsDropped.Incr(lParts)
				mPartsDroppedF.Incr(lParts)
			case <-o.closeChan:
				return
			}
			continue
		}

		for len(outputTargets) > 0 {
			for _, i := range outputTargets {
				msgCopy := ts.Payload.Copy()
				select {
				case o.outputTsChans[i] <- types.NewTransaction(msgCopy, o.outputResChans[i]):
				case <-o.closeChan:
					return
				}
			}
			newTargets := []int{}
			for _, i := range outputTargets {
				select {
				case res := <-o.outputResChans[i]:
					if res.Error() != nil {
						newTargets = append(newTargets, i)
						o.logger.Errorf("Failed to dispatch switch message: %v\n", res.Error())
						mError.Incr(1)
						mErrorF.Incr(1)
						mPartsError.Incr(lParts)
						mPartsErrorF.Incr(lParts)
						if !o.throt.Retry() {
							return
						}
					} else {
						o.throt.Reset()
						mSuccess.Incr(1)
						mSuccessF.Incr(1)
						mPartsSuccess.Incr(lParts)
						mPartsSuccessF.Incr(lParts)
					}
				case <-o.closeChan:
					return
				}
			}
			outputTargets = newTargets
		}
		select {
		case ts.ResponseChan <- response.NewAck():
		case <-o.closeChan:
			return
		}
	}
}

// CloseAsync shuts down the FanOut broker and stops processing requests.
func (o *Switch) CloseAsync() {
	if atomic.CompareAndSwapInt32(&o.running, 1, 0) {
		close(o.closeChan)
	}
}

// WaitForClose blocks until the FanOut broker has closed down.
func (o *Switch) WaitForClose(timeout time.Duration) error {
	select {
	case <-o.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
