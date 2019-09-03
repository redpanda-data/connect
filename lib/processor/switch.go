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

package processor

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/tracing"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	olog "github.com/opentracing/opentracing-go/log"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSwitch] = TypeSpec{
		constructor: NewSwitch,
		description: `
Switch is a processor that lists child case objects each containing a condition
and processors. Each batch of messages is tested against the condition of each
child case until a condition passes, whereby the processors of that case will be
executed on the batch.

Each case may specify a boolean ` + "`fallthrough`" + ` field indicating whether
the next case should be executed after it (the default is ` + "`false`" + `.)

A case takes this form:

` + "``` yaml" + `
- condition:
    type: foo
  processors:
  - type: foo
  fallthrough: false
` + "```" + `

In order to switch each message of a batch individually use this processor with
the ` + "[`for_each`](#for_each)" + ` processor.

You can find a [full list of conditions here](../conditions).`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			switchSlice := []interface{}{}
			for _, switchCase := range conf.Switch {
				var sanProcs []interface{}
				for _, proc := range switchCase.Processors {
					sanProc, err := SanitiseConfig(proc)
					if err != nil {
						return nil, err
					}
					sanProcs = append(sanProcs, sanProc)
				}
				sanCond, err := condition.SanitiseConfig(switchCase.Condition)
				if err != nil {
					return nil, err
				}
				sanit := map[string]interface{}{
					"condition":   sanCond,
					"processors":  sanProcs,
					"fallthrough": switchCase.Fallthrough,
				}
				switchSlice = append(switchSlice, sanit)
			}
			return switchSlice, nil
		},
	}
}

//------------------------------------------------------------------------------

// SwitchCaseConfig contains a condition, processors and other fields for an
// individual case in the Switch processor.
type SwitchCaseConfig struct {
	Condition   condition.Config `json:"condition" yaml:"condition"`
	Processors  []Config         `json:"processors" yaml:"processors"`
	Fallthrough bool             `json:"fallthrough" yaml:"fallthrough"`
}

// NewSwitchCaseConfig returns a new SwitchCaseConfig with default values.
func NewSwitchCaseConfig() SwitchCaseConfig {
	cond := condition.NewConfig()
	cond.Type = condition.TypeStatic
	cond.Static = true
	return SwitchCaseConfig{
		Condition:   cond,
		Processors:  []Config{},
		Fallthrough: false,
	}
}

// UnmarshalJSON ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (s *SwitchCaseConfig) UnmarshalJSON(bytes []byte) error {
	type confAlias SwitchCaseConfig
	aliased := confAlias(NewSwitchCaseConfig())

	if err := json.Unmarshal(bytes, &aliased); err != nil {
		return err
	}

	*s = SwitchCaseConfig(aliased)
	return nil
}

// UnmarshalYAML ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (s *SwitchCaseConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type confAlias SwitchCaseConfig
	aliased := confAlias(NewSwitchCaseConfig())

	if err := unmarshal(&aliased); err != nil {
		return err
	}

	*s = SwitchCaseConfig(aliased)
	return nil
}

//------------------------------------------------------------------------------

// SwitchConfig is a config struct containing fields for the Switch processor.
type SwitchConfig []SwitchCaseConfig

// NewSwitchConfig returns a default SwitchConfig.
func NewSwitchConfig() SwitchConfig {
	return SwitchConfig{}
}

//------------------------------------------------------------------------------

// switchCase contains a condition, processors and other fields for an
// individual case in the Switch processor.
type switchCase struct {
	condition   types.Condition
	processors  []types.Processor
	fallThrough bool
}

// Switch is a processor that only applies child processors under a certain
// condition.
type Switch struct {
	cases []switchCase
	log   log.Modular

	mCount     metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewSwitch returns a Switch processor.
func NewSwitch(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	var cases []switchCase
	for i, caseConf := range conf.Switch {
		prefix := strconv.Itoa(i)

		var err error
		var cond types.Condition
		var procs []types.Processor

		if cond, err = condition.New(
			caseConf.Condition, mgr,
			log.NewModule("."+prefix+".condition"),
			metrics.Namespaced(stats, prefix+".condition"),
		); err != nil {
			return nil, err
		}

		for j, procConf := range caseConf.Processors {
			procPrefix := prefix + "." + strconv.Itoa(j)
			var proc types.Processor
			if proc, err = New(
				procConf, mgr,
				log.NewModule("."+procPrefix),
				metrics.Namespaced(stats, procPrefix),
			); err != nil {
				return nil, err
			}
			procs = append(procs, proc)
		}

		cases = append(cases, switchCase{
			condition:   cond,
			processors:  procs,
			fallThrough: caseConf.Fallthrough,
		})
	}
	return &Switch{
		cases: cases,
		log:   log,

		mCount:     stats.GetCounter("count"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (s *Switch) ProcessMessage(msg types.Message) (msgs []types.Message, res types.Response) {
	s.mCount.Incr(1)

	var procs []types.Processor
	fellthrough := false

	spans := tracing.CreateChildSpans(TypeSwitch, msg)

	for i, switchCase := range s.cases {
		if !fellthrough && !switchCase.condition.Check(msg) {
			continue
		}
		procs = append(procs, switchCase.processors...)
		for _, s := range spans {
			s.LogFields(
				olog.String("event", "case_match"),
				olog.String("value", strconv.Itoa(i)),
			)
		}
		if fellthrough = switchCase.fallThrough; !fellthrough {
			break
		}
	}

	for _, s := range spans {
		s.Finish()
	}

	msgs, res = ExecuteAll(procs, msg)

	s.mBatchSent.Incr(int64(len(msgs)))
	totalParts := 0
	for _, msg := range msgs {
		totalParts += msg.Len()
	}
	s.mSent.Incr(int64(totalParts))
	return
}

// CloseAsync shuts down the processor and stops processing requests.
func (s *Switch) CloseAsync() {
	for _, s := range s.cases {
		for _, proc := range s.processors {
			proc.CloseAsync()
		}
	}
}

// WaitForClose blocks until the processor has closed down.
func (s *Switch) WaitForClose(timeout time.Duration) error {
	stopBy := time.Now().Add(timeout)
	for _, s := range s.cases {
		for _, proc := range s.processors {
			if err := proc.WaitForClose(time.Until(stopBy)); err != nil {
				return err
			}
		}
	}
	return nil
}

//------------------------------------------------------------------------------
