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
	"github.com/Jeffail/benthos/lib/message"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeProcessBatch] = TypeSpec{
		constructor: NewProcessBatch,
		description: `
A processor that applies a list of child processors to messages of a batch as
though they were each a batch of one message. This is useful for forcing batch
wide processors such as ` + "[`dedupe`](#dedupe)" + ` to apply to individual
message parts of a batch instead.

Please note that most processors already process per message of a batch, and
this processor is not needed in those cases.`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			var err error
			procConfs := make([]interface{}, len(conf.ProcessBatch))
			for i, pConf := range conf.ProcessBatch {
				if procConfs[i], err = SanitiseConfig(pConf); err != nil {
					return nil, err
				}
			}
			return procConfs, nil
		},
	}
}

//------------------------------------------------------------------------------

// ProcessBatchConfig is a config struct containing fields for the ProcessBatch
// processor.
type ProcessBatchConfig []Config

// NewProcessBatchConfig returns a default ProcessBatchConfig.
func NewProcessBatchConfig() ProcessBatchConfig {
	return []Config{}
}

//------------------------------------------------------------------------------

// ProcessBatch is a processor that applies a list of child processors to each
// message of a batch individually.
type ProcessBatch struct {
	children []types.Processor

	log log.Modular

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mSentParts metrics.StatCounter
	mDropped   metrics.StatCounter
}

// NewProcessBatch returns a ProcessBatch processor.
func NewProcessBatch(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	nsStats := metrics.Namespaced(stats, "processor.process_batch")
	nsLog := log.NewModule(".processor.process_batch")

	var children []types.Processor
	for _, pconf := range conf.ProcessBatch {
		proc, err := New(pconf, mgr, nsLog, nsStats)
		if err != nil {
			return nil, err
		}
		children = append(children, proc)
	}
	return &ProcessBatch{
		children: children,
		log:      nsLog,

		mCount:     stats.GetCounter("processor.process_batch.count"),
		mErr:       stats.GetCounter("processor.process_batch.error"),
		mSent:      stats.GetCounter("processor.process_batch.sent"),
		mSentParts: stats.GetCounter("processor.process_batch.parts.sent"),
		mDropped:   stats.GetCounter("processor.process_batch.dropped"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (p *ProcessBatch) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	p.mCount.Incr(1)

	resultMsgs := make([]types.Message, msg.Len())
	msg.Iter(func(i int, p types.Part) error {
		tmpMsg := message.New(nil)
		tmpMsg.SetAll([]types.Part{p})
		resultMsgs[i] = tmpMsg
		return nil
	})

	var res types.Response
	if resultMsgs, res = ExecuteAll(p.children, resultMsgs...); res != nil {
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
		p.mDropped.Incr(1)
		return nil, res
	}

	p.mSent.Incr(1)
	p.mSentParts.Incr(int64(resMsg.Len()))

	resMsgs := [1]types.Message{resMsg}
	return resMsgs[:], nil
}

//------------------------------------------------------------------------------
