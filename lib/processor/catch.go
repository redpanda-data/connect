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
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeCatch] = TypeSpec{
		constructor: NewCatch,
		description: `
Behaves similarly to the ` + "[`for_each`](#for_each)" + ` processor, where a
list of child processors are applied to individual messages of a batch. However,
processors are only applied to messages that failed a processing step prior to
the catch.

For example, with the following config:

` + "``` yaml" + `
- type: foo
- catch:
  - type: bar
  - type: baz
` + "```" + `

If the processor ` + "`foo`" + ` fails for a particular message, that message
will be fed into the processors ` + "`bar` and `baz`" + `. Messages that do not
fail for the processor ` + "`foo`" + ` will skip these processors.

When messages leave the catch block their fail flags are cleared. This processor
is useful for when it's possible to recover failed messages, or when special
actions (such as logging/metrics) are required before dropping them.

More information about error handing can be found [here](../error_handling.md).`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			var err error
			procConfs := make([]interface{}, len(conf.Catch))
			for i, pConf := range conf.Catch {
				if procConfs[i], err = SanitiseConfig(pConf); err != nil {
					return nil, err
				}
			}
			return procConfs, nil
		},
	}
}

//------------------------------------------------------------------------------

// CatchConfig is a config struct containing fields for the Catch processor.
type CatchConfig []Config

// NewCatchConfig returns a default CatchConfig.
func NewCatchConfig() CatchConfig {
	return []Config{}
}

//------------------------------------------------------------------------------

// Catch is a processor that applies a list of child processors to each message of
// a batch individually, where processors are skipped for messages that failed a
// previous processor step.
type Catch struct {
	children []types.Processor

	log log.Modular

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewCatch returns a Catch processor.
func NewCatch(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	var children []types.Processor
	for i, pconf := range conf.Catch {
		prefix := fmt.Sprintf("%v", i)
		proc, err := New(pconf, mgr, log.NewModule("."+prefix), metrics.Namespaced(stats, prefix))
		if err != nil {
			return nil, err
		}
		children = append(children, proc)
	}
	return &Catch{
		children: children,
		log:      log,

		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (p *Catch) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	p.mCount.Incr(1)

	resultMsgs := make([]types.Message, msg.Len())
	msg.Iter(func(i int, p types.Part) error {
		tmpMsg := message.New(nil)
		tmpMsg.SetAll([]types.Part{p})
		resultMsgs[i] = tmpMsg
		return nil
	})

	var res types.Response
	if resultMsgs, res = ExecuteCatchAll(p.children, resultMsgs...); res != nil {
		return nil, res
	}

	resMsg := message.New(nil)
	for _, m := range resultMsgs {
		m.Iter(func(i int, p types.Part) error {
			resMsg.Append(p)
			return nil
		})
	}
	if resMsg.Len() == 0 {
		return nil, res
	}

	resMsg.Iter(func(i int, p types.Part) error {
		ClearFail(p)
		return nil
	})

	p.mBatchSent.Incr(1)
	p.mSent.Incr(int64(resMsg.Len()))

	resMsgs := [1]types.Message{resMsg}
	return resMsgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (p *Catch) CloseAsync() {
	for _, c := range p.children {
		c.CloseAsync()
	}
}

// WaitForClose blocks until the processor has closed down.
func (p *Catch) WaitForClose(timeout time.Duration) error {
	stopBy := time.Now().Add(timeout)
	for _, c := range p.children {
		if err := c.WaitForClose(time.Until(stopBy)); err != nil {
			return err
		}
	}
	return nil
}

//------------------------------------------------------------------------------
