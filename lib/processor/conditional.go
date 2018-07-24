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

package processor

import (
	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/processor/condition"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["conditional"] = TypeSpec{
		constructor: NewConditional,
		description: `
Conditional is a processor that has a list of child 'processors',
'else_processors', and a condition. For each message if the condition passes the
child 'processors' will be applied, otherwise the 'else_processors' are applied.
This processor is useful for applying processors such as 'dedupe' based on the
content type of the message.

You can find a [full list of conditions here](../conditions).`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			condSanit, err := condition.SanitiseConfig(conf.Conditional.Condition)
			if err != nil {
				return nil, err
			}
			procConfs := make([]interface{}, len(conf.Conditional.Processors))
			for i, pConf := range conf.Conditional.Processors {
				if procConfs[i], err = SanitiseConfig(pConf); err != nil {
					return nil, err
				}
			}
			elseProcConfs := make([]interface{}, len(conf.Conditional.ElseProcessors))
			for i, pConf := range conf.Conditional.ElseProcessors {
				if elseProcConfs[i], err = SanitiseConfig(pConf); err != nil {
					return nil, err
				}
			}
			return map[string]interface{}{
				"condition":       condSanit,
				"processors":      procConfs,
				"else_processors": elseProcConfs,
			}, nil
		},
	}
}

//------------------------------------------------------------------------------

// ConditionalConfig is a config struct containing fields for the Conditional
// processor.
type ConditionalConfig struct {
	Condition      condition.Config `json:"condition" yaml:"condition"`
	Processors     []Config         `json:"processors" yaml:"processors"`
	ElseProcessors []Config         `json:"else_processors" yaml:"else_processors"`
}

// NewConditionalConfig returns a default ConditionalConfig.
func NewConditionalConfig() ConditionalConfig {
	return ConditionalConfig{
		Condition:      condition.NewConfig(),
		Processors:     []Config{},
		ElseProcessors: []Config{},
	}
}

//------------------------------------------------------------------------------

// Conditional is a processor that only applies child processors under a certain
// condition.
type Conditional struct {
	cond         condition.Type
	children     []Type
	elseChildren []Type

	log log.Modular

	mCount      metrics.StatCounter
	mCondPassed metrics.StatCounter
	mCondFailed metrics.StatCounter
	mSent       metrics.StatCounter
	mSentParts  metrics.StatCounter
	mDropped    metrics.StatCounter
}

// NewConditional returns a Conditional processor.
func NewConditional(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	nsStats := metrics.Namespaced(stats, "processor.conditional")
	nsLog := log.NewModule(".processor.conditional")
	cond, err := condition.New(conf.Conditional.Condition, mgr, nsLog, nsStats)
	if err != nil {
		return nil, err
	}

	nsStats = metrics.Namespaced(stats, "processor.conditional.if")
	nsLog = log.NewModule(".processor.conditional.if")
	var children []Type
	for _, pconf := range conf.Conditional.Processors {
		var proc Type
		if proc, err = New(pconf, mgr, nsLog, nsStats); err != nil {
			return nil, err
		}
		children = append(children, proc)
	}

	nsStats = metrics.Namespaced(stats, "processor.conditional.else")
	nsLog = log.NewModule(".processor.conditional.else")
	var elseChildren []Type
	for _, pconf := range conf.Conditional.ElseProcessors {
		var proc Type
		if proc, err = New(pconf, mgr, nsLog, nsStats); err != nil {
			return nil, err
		}
		elseChildren = append(elseChildren, proc)
	}

	return &Conditional{
		cond:         cond,
		children:     children,
		elseChildren: elseChildren,

		log: log.NewModule(".processor.conditional"),

		mCount:      stats.GetCounter("processor.conditional.count"),
		mCondPassed: stats.GetCounter("processor.conditional.passed"),
		mCondFailed: stats.GetCounter("processor.conditional.failed"),
		mSent:       stats.GetCounter("processor.conditional.sent"),
		mSentParts:  stats.GetCounter("processor.conditional.parts.sent"),
		mDropped:    stats.GetCounter("processor.conditional.dropped"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage does nothing and returns the message unchanged.
func (c *Conditional) ProcessMessage(msg types.Message) (msgs []types.Message, res types.Response) {
	c.mCount.Incr(1)

	var procs []Type

	if c.cond.Check(msg) {
		c.mCondPassed.Incr(1)
		c.log.Traceln("Condition passed")
		procs = c.children
	} else {
		c.mCondFailed.Incr(1)
		c.log.Traceln("Condition failed")
		procs = c.elseChildren
	}

	resultMsgs := []types.Message{msg}
	var resultRes types.Response

	for i := 0; len(resultMsgs) > 0 && i < len(procs); i++ {
		var nextResultMsgs []types.Message
		for _, m := range resultMsgs {
			var rMsgs []types.Message
			rMsgs, resultRes = procs[i].ProcessMessage(m)
			nextResultMsgs = append(nextResultMsgs, rMsgs...)
		}
		resultMsgs = nextResultMsgs
	}

	if len(resultMsgs) == 0 {
		c.mDropped.Incr(1)
		res = resultRes
	} else {
		c.mSent.Incr(int64(len(resultMsgs)))
		totalParts := 0
		for _, msg := range resultMsgs {
			totalParts += msg.Len()
		}
		c.mSentParts.Incr(int64(totalParts))
		msgs = resultMsgs
	}

	return
}

//------------------------------------------------------------------------------
