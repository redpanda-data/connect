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
	"strconv"
	"time"

	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/tracing"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	olog "github.com/opentracing/opentracing-go/log"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeGroupBy] = TypeSpec{
		constructor: NewGroupBy,
		description: `
Splits a batch of messages into N batches, where each resulting batch contains a
group of messages determined by conditions that are applied per message of the
original batch. Once the groups are established a list of processors are applied
to their respective grouped batch, which can be used to label the batch as per
their grouping.

Each group is configured in a list with a condition and a list of processors:

` + "``` yaml" + `
group_by:
- condition:
    static: true
  processors:
  - type: noop
` + "```" + `

Messages are added to the first group that passes and can only belong to a
single group. Messages that do not pass the conditions of any group are placed
in a final batch with no processors applied.

For example, imagine we have a batch of messages that we wish to split into two
groups - the foos and the bars - which should be sent to different output
destinations based on those groupings. We also need to send the foos as a tar
gzip archive. For this purpose we can use the ` + "`group_by`" + ` processor
with a ` + "[`switch`](../outputs/README.md#switch)" + ` output:

` + "``` yaml" + `
pipeline:
  processors:
  - group_by:
    - condition:
        text:
          operator: contains
          arg: "this is a foo"
      processors:
      - archive:
          format: tar
      - compress:
          algorithm: gzip
      - metadata:
          operator: set
          key: grouping
          value: foo
output:
  switch:
    outputs:
    - output:
        type: foo_output
      condition:
        metadata:
          operator: equals
          key: grouping
          arg: foo
    - output:
        type: bar_output
` + "```" + `

Since any message that isn't a foo is a bar, and bars do not require their own
processing steps, we only need a single grouping configuration.`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			groups := []interface{}{}
			for _, g := range conf.GroupBy {
				condSanit, err := condition.SanitiseConfig(g.Condition)
				if err != nil {
					return nil, err
				}
				procsSanit := []interface{}{}
				for _, p := range g.Processors {
					var procSanit interface{}
					if procSanit, err = SanitiseConfig(p); err != nil {
						return nil, err
					}
					procsSanit = append(procsSanit, procSanit)
				}
				groups = append(groups, map[string]interface{}{
					"condition":  condSanit,
					"processors": procsSanit,
				})
			}
			return groups, nil
		},
	}
}

//------------------------------------------------------------------------------

// GroupByElement represents a group determined by a condition and a list of
// group specific processors.
type GroupByElement struct {
	Condition  condition.Config `json:"condition" yaml:"condition"`
	Processors []Config         `json:"processors" yaml:"processors"`
}

//------------------------------------------------------------------------------

// GroupByConfig is a configuration struct containing fields for the GroupBy
// processor, which breaks message batches down into N batches of a smaller size
// according to conditions.
type GroupByConfig []GroupByElement

// NewGroupByConfig returns a GroupByConfig with default values.
func NewGroupByConfig() GroupByConfig {
	return GroupByConfig{}
}

//------------------------------------------------------------------------------

type group struct {
	Condition  condition.Type
	Processors []types.Processor
}

// GroupBy is a processor that group_bys messages into a message per part.
type GroupBy struct {
	log   log.Modular
	stats metrics.Type

	groups     []group
	mGroupPass []metrics.StatCounter

	mCount        metrics.StatCounter
	mGroupDefault metrics.StatCounter
	mSent         metrics.StatCounter
	mBatchSent    metrics.StatCounter
}

// NewGroupBy returns a GroupBy processor.
func NewGroupBy(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	var err error
	groups := make([]group, len(conf.GroupBy))
	groupCtrs := make([]metrics.StatCounter, len(conf.GroupBy))

	for i, gConf := range conf.GroupBy {
		groupPrefix := fmt.Sprintf("groups.%v", i)
		nsLog := log.NewModule("." + groupPrefix)
		nsStats := metrics.Namespaced(stats, groupPrefix)

		if groups[i].Condition, err = condition.New(
			gConf.Condition, mgr,
			nsLog.NewModule(".condition"), metrics.Namespaced(nsStats, "condition"),
		); err != nil {
			return nil, fmt.Errorf("failed to create condition for group '%v': %v", i, err)
		}
		for j, pConf := range gConf.Processors {
			prefix := fmt.Sprintf("processor.%v", j)
			var proc Type
			if proc, err = New(
				pConf, mgr,
				nsLog.NewModule("."+prefix), metrics.Namespaced(nsStats, prefix),
			); err != nil {
				return nil, fmt.Errorf("failed to create processor '%v' for group '%v': %v", j, i, err)
			}
			groups[i].Processors = append(groups[i].Processors, proc)
		}

		groupCtrs[i] = stats.GetCounter(groupPrefix + ".passed")
	}

	return &GroupBy{
		log:   log,
		stats: stats,

		groups:     groups,
		mGroupPass: groupCtrs,

		mCount:        stats.GetCounter("count"),
		mGroupDefault: stats.GetCounter("groups.default.passed"),
		mSent:         stats.GetCounter("sent"),
		mBatchSent:    stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (g *GroupBy) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	g.mCount.Incr(1)

	if msg.Len() == 0 {
		return nil, response.NewAck()
	}

	groups := make([]types.Message, len(g.groups))
	for i := range groups {
		groups[i] = message.New(nil)
	}
	groupless := message.New(nil)

	spans := tracing.CreateChildSpans(TypeGroupBy, msg)

	msg.Iter(func(i int, p types.Part) error {
		for j, group := range g.groups {
			if group.Condition.Check(message.Lock(msg, i)) {
				groupStr := strconv.Itoa(j)
				spans[i].LogFields(
					olog.String("event", "grouped"),
					olog.String("type", groupStr),
				)
				spans[i].SetTag("group", groupStr)
				groups[j].Append(p.Copy())
				g.mGroupPass[j].Incr(1)
				return nil
			}
		}

		spans[i].LogFields(
			olog.String("event", "grouped"),
			olog.String("type", "default"),
		)
		spans[i].SetTag("group", "default")
		groupless.Append(p.Copy())
		g.mGroupDefault.Incr(1)
		return nil
	})

	for _, s := range spans {
		s.Finish()
	}

	msgs := []types.Message{}
	for i, gmsg := range groups {
		if gmsg.Len() == 0 {
			continue
		}

		resultMsgs, res := ExecuteAll(g.groups[i].Processors, gmsg)
		if len(resultMsgs) > 0 {
			msgs = append(msgs, resultMsgs...)
		}
		if res != nil {
			if err := res.Error(); err != nil {
				g.log.Errorf("Processor error: %v\n", err)
			}
		}
	}

	if groupless.Len() > 0 {
		msgs = append(msgs, groupless)
	}

	if len(msgs) == 0 {
		return nil, response.NewAck()
	}

	g.mBatchSent.Incr(int64(len(msgs)))
	for _, m := range msgs {
		g.mSent.Incr(int64(m.Len()))
	}
	return msgs, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (g *GroupBy) CloseAsync() {
	for _, group := range g.groups {
		for _, p := range group.Processors {
			p.CloseAsync()
		}
	}
}

// WaitForClose blocks until the processor has closed down.
func (g *GroupBy) WaitForClose(timeout time.Duration) error {
	stopBy := time.Now().Add(timeout)
	for _, group := range g.groups {
		for _, p := range group.Processors {
			if err := p.WaitForClose(time.Until(stopBy)); err != nil {
				return err
			}
		}
	}
	return nil
}

//------------------------------------------------------------------------------
