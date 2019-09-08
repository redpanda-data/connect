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
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/throttle"
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
The switch output type allows you to configure multiple conditional output
targets by listing child outputs paired with conditions. Conditional logic is
currently applied per whole message batch. In order to multiplex per message of
a batch use the ` + "[`broker`](#broker)" + ` output with the pattern
` + "`fan_out`" + `.

In the following example, messages containing "foo" will be sent to both the
` + "`foo`" + ` and ` + "`baz`" + ` outputs. Messages containing "bar" will be
sent to both the ` + "`bar`" + ` and ` + "`baz`" + ` outputs. Messages
containing both "foo" and "bar" will be sent to all three outputs. And finally,
messages that do not contain "foo" or "bar" will be sent to the ` + "`baz`" + `
output only.

` + "``` yaml" + `
output:
  switch:
    retry_until_success: true
    outputs:
    - output:
        foo:
          foo_field_1: value1
      condition:
        text:
          operator: contains
          arg: foo
      fallthrough: true
    - output:
        bar:
          bar_field_1: value2
          bar_field_2: value3
      condition:
        text:
          operator: contains
          arg: bar
      fallthrough: true
    - output:
        baz:
          baz_field_1: value4
        processors:
        - type: baz_processor
  processors:
  - type: some_processor
` + "```" + `

The switch output requires a minimum of two outputs. If no condition is defined
for an output, it behaves like a static ` + "`true`" + ` condition. If
` + "`fallthrough`" + ` is set to ` + "`true`" + `, the switch output will
continue evaluating additional outputs after finding a match.

Messages that do not match any outputs will be dropped. If an output applies
back pressure it will block all subsequent messages.

If an output fails to send a message it will be retried continuously until
completion or service shut down. You can change this behaviour so that when an
output returns an error the switch output also returns an error by setting
` + "`retry_until_success`" + ` to ` + "`false`" + `. This allows you to
wrap the switch with a ` + "`try`" + ` broker, but care must be taken to ensure
duplicate messages aren't introduced during error conditions.`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			outSlice := []interface{}{}
			for _, out := range conf.Switch.Outputs {
				sanOutput, err := SanitiseConfig(out.Output)
				if err != nil {
					return nil, err
				}
				var sanCond interface{}
				if sanCond, err = condition.SanitiseConfig(out.Condition); err != nil {
					return nil, err
				}
				sanit := map[string]interface{}{
					"output":      sanOutput,
					"fallthrough": out.Fallthrough,
					"condition":   sanCond,
				}
				outSlice = append(outSlice, sanit)
			}
			return map[string]interface{}{
				"retry_until_success": conf.Switch.RetryUntilSuccess,
				"outputs":             outSlice,
			}, nil
		},
	}
}

//------------------------------------------------------------------------------

// SwitchConfig contains configuration fields for the Switch output type.
type SwitchConfig struct {
	RetryUntilSuccess bool                 `json:"retry_until_success" yaml:"retry_until_success"`
	Outputs           []SwitchConfigOutput `json:"outputs" yaml:"outputs"`
}

// NewSwitchConfig creates a new SwitchConfig with default values.
func NewSwitchConfig() SwitchConfig {
	return SwitchConfig{
		RetryUntilSuccess: true,
		Outputs:           []SwitchConfigOutput{},
	}
}

// SwitchConfigOutput contains configuration fields per output of a switch type.
type SwitchConfigOutput struct {
	Condition   condition.Config `json:"condition" yaml:"condition"`
	Fallthrough bool             `json:"fallthrough" yaml:"fallthrough"`
	Output      Config           `json:"output" yaml:"output"`
}

// NewSwitchConfigOutput creates a new switch output config with default values.
func NewSwitchConfigOutput() SwitchConfigOutput {
	cond := condition.NewConfig()
	cond.Type = condition.TypeStatic
	cond.Static = true

	return SwitchConfigOutput{
		Condition:   cond,
		Fallthrough: false,
		Output:      NewConfig(),
	}
}

//------------------------------------------------------------------------------

// UnmarshalJSON ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (s *SwitchConfigOutput) UnmarshalJSON(bytes []byte) error {
	type confAlias SwitchConfigOutput
	aliased := confAlias(NewSwitchConfigOutput())

	if err := json.Unmarshal(bytes, &aliased); err != nil {
		return err
	}

	*s = SwitchConfigOutput(aliased)
	return nil
}

// UnmarshalYAML ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (s *SwitchConfigOutput) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type confAlias SwitchConfigOutput
	aliased := confAlias(NewSwitchConfigOutput())

	if err := unmarshal(&aliased); err != nil {
		return err
	}

	*s = SwitchConfigOutput(aliased)
	return nil
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

	retryUntilSuccess bool
	outputs           []types.Output
	conditions        []types.Condition
	fallthroughs      []bool

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewSwitch creates a new Switch type by providing outputs. Messages will be
// sent to a subset of outputs according to condition and fallthrough settings.
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

	o := &Switch{
		running:           1,
		stats:             stats,
		logger:            logger,
		transactions:      nil,
		outputs:           make([]types.Output, lOutputs),
		conditions:        make([]types.Condition, lOutputs),
		fallthroughs:      make([]bool, lOutputs),
		retryUntilSuccess: conf.Switch.RetryUntilSuccess,
		closedChan:        make(chan struct{}),
		closeChan:         make(chan struct{}),
	}

	var err error
	for i, oConf := range conf.Switch.Outputs {
		ns := fmt.Sprintf("switch.%v", i)
		if o.outputs[i], err = New(
			oConf.Output, mgr,
			logger.NewModule("."+ns+".output"),
			metrics.Combine(stats, metrics.Namespaced(stats, ns+".output")),
		); err != nil {
			return nil, fmt.Errorf("failed to create output '%v' type '%v': %v", i, oConf.Output.Type, err)
		}
		if o.conditions[i], err = condition.New(
			oConf.Condition, mgr,
			logger.NewModule("."+ns+".condition"),
			metrics.Namespaced(stats, ns+".condition"),
		); err != nil {
			return nil, fmt.Errorf("failed to create output '%v' condition '%v': %v", i, oConf.Condition.Type, err)
		}
		o.fallthroughs[i] = oConf.Fallthrough
	}

	o.throt = throttle.New(throttle.OptCloseChan(o.closeChan))

	o.outputTsChans = make([]chan types.Transaction, len(o.outputs))
	o.outputResChans = make([]chan types.Response, len(o.outputs))
	for i := range o.outputTsChans {
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

// Connected returns a boolean indicating whether this output is currently
// connected to its target.
func (o *Switch) Connected() bool {
	for _, out := range o.outputs {
		if !out.Connected() {
			return false
		}
	}
	return true
}

//------------------------------------------------------------------------------

// loop is an internal loop that brokers incoming messages to many outputs.
func (o *Switch) loop() {
	var (
		mMsgDrop   = o.stats.GetCounter("switch.messages.dropped")
		mMsgRcvd   = o.stats.GetCounter("switch.messages.received")
		mMsgSnt    = o.stats.GetCounter("switch.messages.sent")
		mOutputErr = o.stats.GetCounter("switch.output.error")
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
		close(o.closedChan)
	}()

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
		mMsgRcvd.Incr(1)

		var outputTargets []int
		for i, oCond := range o.conditions {
			if oCond.Check(ts.Payload) {
				outputTargets = append(outputTargets, i)
				if !o.fallthroughs[i] {
					break
				}
			}
		}
		if len(outputTargets) == 0 {
			select {
			case ts.ResponseChan <- response.NewAck():
				mMsgDrop.Incr(1)
			case <-o.closeChan:
				return
			}
			continue
		}

		var oResponse types.Response

	outputsLoop:
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
						if o.retryUntilSuccess {
							newTargets = append(newTargets, i)
							o.logger.Errorf("Failed to dispatch switch message: %v\n", res.Error())
							mOutputErr.Incr(1)
							if !o.throt.Retry() {
								return
							}
						} else {
							oResponse = res
						}
					} else {
						o.throt.Reset()
						mMsgSnt.Incr(1)
					}
				case <-o.closeChan:
					return
				}
			}
			outputTargets = newTargets
			if oResponse != nil {
				break outputsLoop
			}
		}
		if oResponse == nil {
			oResponse = response.NewAck()
		}
		select {
		case ts.ResponseChan <- oResponse:
		case <-o.closeChan:
			return
		}
	}
}

// CloseAsync shuts down the Switch broker and stops processing requests.
func (o *Switch) CloseAsync() {
	if atomic.CompareAndSwapInt32(&o.running, 1, 0) {
		close(o.closeChan)
	}
}

// WaitForClose blocks until the Switch broker has closed down.
func (o *Switch) WaitForClose(timeout time.Duration) error {
	select {
	case <-o.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
