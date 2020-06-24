package output

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/throttle"
	"github.com/Jeffail/benthos/v3/lib/x/docs"
	"golang.org/x/sync/errgroup"
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
		Summary: `
The switch output type allows you to route messages to different outputs based
on their contents.`,
		Description: `
When [batching messages at the input level](/docs/configuration/batching/)
conditional logic is applied across the entire batch. In order to multiplex per
message of a batch use ` + "[broker based multiplexing](/docs/components/outputs/about#broker-multiplexing)" + `.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldAdvanced(
				"retry_until_success", `
If a selected output fails to send a message this field determines whether it is
reattempted indefinitely. If set to false the error is instead propagated back
to the input level.

If a message can be routed to >1 outputs it is usually best to set this to true
in order to avoid duplicate messages being routed to an output.`,
			),
			docs.FieldAdvanced(
				"strict_mode", `
This field determines whether an error should be reported if no condition is met.
If set to true, an error is propagated back to the input level. The default
behavior is false, which will drop the message.`,
			),
			docs.FieldCommon(
				"outputs", `
A list of switch cases, each consisting of an [output](/docs/components/outputs/about),
a [condition](/docs/components/conditions/about) and a field fallthrough,
indicating whether the next case should also be tested if the current resolves
to true.`,
				[]interface{}{
					map[string]interface{}{
						"fallthrough": false,
						"output": map[string]interface{}{
							"cache": map[string]interface{}{
								"target": "foo",
								"key":    "${!json(\"id\")}",
							},
						},
						"condition": map[string]interface{}{
							"jmespath": map[string]interface{}{
								"query": "contains(urls, 'http://benthos.dev')",
							},
						},
					},
					map[string]interface{}{
						"output": map[string]interface{}{
							"s3": map[string]interface{}{
								"bucket": "bar",
								"path":   "${!json(\"id\")}",
							},
						},
					},
				},
			),
		},
		Footnotes: `
## Examples

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
        bloblang: content().contains("foo")
      fallthrough: true
    - output:
        bar:
          bar_field_1: value2
          bar_field_2: value3
      condition:
        bloblang: content().contains("bar")
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
				"strict_mode":         conf.Switch.StrictMode,
				"outputs":             outSlice,
			}, nil
		},
	}
}

//------------------------------------------------------------------------------

// SwitchConfig contains configuration fields for the Switch output type.
type SwitchConfig struct {
	RetryUntilSuccess bool                 `json:"retry_until_success" yaml:"retry_until_success"`
	StrictMode        bool                 `json:"strict_mode" yaml:"strict_mode"`
	Outputs           []SwitchConfigOutput `json:"outputs" yaml:"outputs"`
}

// NewSwitchConfig creates a new SwitchConfig with default values.
func NewSwitchConfig() SwitchConfig {
	return SwitchConfig{
		RetryUntilSuccess: true,
		StrictMode:        false,
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
	logger log.Modular
	stats  metrics.Type

	maxInFlight  int
	transactions <-chan types.Transaction

	retryUntilSuccess bool
	strictMode        bool
	outputTsChans     []chan types.Transaction
	outputs           []types.Output
	conditions        []types.Condition
	fallthroughs      []bool

	ctx        context.Context
	close      func()
	closedChan chan struct{}
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

	ctx, done := context.WithCancel(context.Background())
	o := &Switch{
		stats:             stats,
		logger:            logger,
		maxInFlight:       1,
		transactions:      nil,
		outputs:           make([]types.Output, lOutputs),
		conditions:        make([]types.Condition, lOutputs),
		fallthroughs:      make([]bool, lOutputs),
		retryUntilSuccess: conf.Switch.RetryUntilSuccess,
		strictMode:        conf.Switch.StrictMode,
		closedChan:        make(chan struct{}),
		ctx:               ctx,
		close:             done,
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

	o.outputTsChans = make([]chan types.Transaction, len(o.outputs))
	for i := range o.outputTsChans {
		o.outputTsChans[i] = make(chan types.Transaction)
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
		wg         = sync.WaitGroup{}
		mMsgRcvd   = o.stats.GetCounter("switch.messages.received")
		mMsgSnt    = o.stats.GetCounter("switch.messages.sent")
		mOutputErr = o.stats.GetCounter("switch.output.error")
	)

	defer func() {
		wg.Wait()
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

	sendLoop := func() {
		defer wg.Done()
		for {
			var ts types.Transaction
			var open bool

			select {
			case ts, open = <-o.transactions:
				if !open {
					return
				}
			case <-o.ctx.Done():
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

			if o.strictMode && len(outputTargets) == 0 {
				select {
				case ts.ResponseChan <- response.NewError(ErrSwitchNoConditionMet):
				case <-o.ctx.Done():
					return
				}
				continue
			}

			var owg errgroup.Group
			for _, target := range outputTargets {
				msgCopy, i := ts.Payload.Copy(), target
				owg.Go(func() error {
					throt := throttle.New(throttle.OptCloseChan(o.ctx.Done()))
					resChan := make(chan types.Response)

					// Try until success or shutdown.
					for {
						select {
						case o.outputTsChans[i] <- types.NewTransaction(msgCopy, resChan):
						case <-o.ctx.Done():
							return types.ErrTypeClosed
						}
						select {
						case res := <-resChan:
							if res.Error() != nil {
								if o.retryUntilSuccess {
									o.logger.Errorf("Failed to dispatch switch message: %v\n", res.Error())
									mOutputErr.Incr(1)
									if !throt.Retry() {
										return types.ErrTypeClosed
									}
								} else {
									return res.Error()
								}
							} else {
								mMsgSnt.Incr(1)
								return nil
							}
						case <-o.ctx.Done():
							return types.ErrTypeClosed
						}
					}
				})
			}

			var oResponse types.Response = response.NewAck()
			if resErr := owg.Wait(); resErr != nil {
				oResponse = response.NewError(resErr)
			}
			select {
			case ts.ResponseChan <- oResponse:
			case <-o.ctx.Done():
				return
			}
		}
	}

	// Max in flight
	for i := 0; i < o.maxInFlight; i++ {
		wg.Add(1)
		go sendLoop()
	}
}

// CloseAsync shuts down the Switch broker and stops processing requests.
func (o *Switch) CloseAsync() {
	o.close()
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
