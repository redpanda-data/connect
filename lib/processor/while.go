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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/tracing"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeWhile] = TypeSpec{
		constructor: NewWhile,
		description: `
While is a processor that has a condition and a list of child processors. The
child processors are executed continuously on a message batch for as long as the
child condition resolves to true.

The field ` + "`at_least_once`" + `, if true, ensures that the child processors
are always executed at least one time (like a do .. while loop.)

The field ` + "`max_loops`" + `, if greater than zero, caps the number of loops
for a message batch to this value.

If following a loop execution the number of messages in a batch is reduced to
zero the loop is exited regardless of the condition result. If following a loop
execution there are more than 1 message batches the condition is checked against
the first batch only.

You can find a [full list of conditions here](../conditions).`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			condSanit, err := condition.SanitiseConfig(conf.While.Condition)
			if err != nil {
				return nil, err
			}
			procConfs := make([]interface{}, len(conf.While.Processors))
			for i, pConf := range conf.While.Processors {
				if procConfs[i], err = SanitiseConfig(pConf); err != nil {
					return nil, err
				}
			}
			return map[string]interface{}{
				"at_least_once": conf.While.AtLeastOnce,
				"max_loops":     conf.While.MaxLoops,
				"condition":     condSanit,
				"processors":    procConfs,
			}, nil
		},
	}
}

//------------------------------------------------------------------------------

// WhileConfig is a config struct containing fields for the While
// processor.
type WhileConfig struct {
	AtLeastOnce bool             `json:"at_least_once" yaml:"at_least_once"`
	MaxLoops    int              `json:"max_loops" yaml:"max_loops"`
	Condition   condition.Config `json:"condition" yaml:"condition"`
	Processors  []Config         `json:"processors" yaml:"processors"`
}

// NewWhileConfig returns a default WhileConfig.
func NewWhileConfig() WhileConfig {
	return WhileConfig{
		AtLeastOnce: false,
		MaxLoops:    0,
		Condition:   condition.NewConfig(),
		Processors:  []Config{},
	}
}

//------------------------------------------------------------------------------

// While is a processor that applies child processors for as long as a child
// condition resolves to true.
type While struct {
	running     int32
	maxLoops    int
	atLeastOnce bool
	cond        condition.Type
	children    []types.Processor

	log log.Modular

	mCount      metrics.StatCounter
	mLoop       metrics.StatCounter
	mCondFailed metrics.StatCounter
	mSent       metrics.StatCounter
	mBatchSent  metrics.StatCounter
}

// NewWhile returns a While processor.
func NewWhile(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	cond, err := condition.New(conf.While.Condition, mgr, log.NewModule(".condition"), metrics.Namespaced(stats, "condition"))
	if err != nil {
		return nil, err
	}

	var children []types.Processor
	for i, pconf := range conf.While.Processors {
		ns := fmt.Sprintf("while.%v", i)
		nsStats := metrics.Namespaced(stats, ns)
		nsLog := log.NewModule("." + ns)
		var proc Type
		if proc, err = New(pconf, mgr, nsLog, nsStats); err != nil {
			return nil, err
		}
		children = append(children, proc)
	}

	return &While{
		running:     1,
		maxLoops:    conf.While.MaxLoops,
		atLeastOnce: conf.While.AtLeastOnce,
		cond:        cond,
		children:    children,

		log: log,

		mCount:      stats.GetCounter("count"),
		mLoop:       stats.GetCounter("loop"),
		mCondFailed: stats.GetCounter("failed"),
		mSent:       stats.GetCounter("sent"),
		mBatchSent:  stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (w *While) ProcessMessage(msg types.Message) (msgs []types.Message, res types.Response) {
	w.mCount.Incr(1)

	spans := tracing.CreateChildSpans(TypeWhile, msg)
	msgs = []types.Message{msg}

	loops := 0
	condResult := w.atLeastOnce || w.cond.Check(msg)
	for condResult {
		if atomic.LoadInt32(&w.running) != 1 {
			return nil, response.NewError(types.ErrTypeClosed)
		}
		if w.maxLoops > 0 && loops >= w.maxLoops {
			w.log.Traceln("Reached max loops count")
			break
		}

		w.mLoop.Incr(1)
		w.log.Traceln("Looped")
		for _, s := range spans {
			s.LogEvent("loop")
		}

		msgs, res = ExecuteAll(w.children, msgs...)
		if len(msgs) == 0 {
			return
		}
		condResult = w.cond.Check(msgs[0])
		loops++
	}

	for _, s := range spans {
		s.SetTag("result", condResult)
		s.Finish()
	}

	w.mBatchSent.Incr(int64(len(msgs)))
	totalParts := 0
	for _, msg := range msgs {
		totalParts += msg.Len()
	}
	w.mSent.Incr(int64(totalParts))
	return
}

// CloseAsync shuts down the processor and stops processing requests.
func (w *While) CloseAsync() {
	atomic.StoreInt32(&w.running, 0)
	for _, p := range w.children {
		p.CloseAsync()
	}
}

// WaitForClose blocks until the processor has closed down.
func (w *While) WaitForClose(timeout time.Duration) error {
	stopBy := time.Now().Add(timeout)
	for _, p := range w.children {
		if err := p.WaitForClose(time.Until(stopBy)); err != nil {
			return err
		}
	}
	return nil
}

//------------------------------------------------------------------------------
