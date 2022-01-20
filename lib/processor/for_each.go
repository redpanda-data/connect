package processor

import (
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeForEach] = TypeSpec{
		constructor: NewForEach,
		Categories: []Category{
			CategoryComposition,
		},
		Summary: `
A processor that applies a list of child processors to messages of a batch as
though they were each a batch of one message.`,
		Description: `
This is useful for forcing batch wide processors such as
` + "[`dedupe`](/docs/components/processors/dedupe)" + ` or interpolations such
as the ` + "`value`" + ` field of the ` + "`metadata`" + ` processor to execute
on individual message parts of a batch instead.

Please note that most processors already process per message of a batch, and
this processor is not needed in those cases.`,
		config: docs.FieldComponent().Array().HasType(docs.FieldTypeProcessor),
	}
}

//------------------------------------------------------------------------------

// ForEachConfig is a config struct containing fields for the ForEach
// processor.
type ForEachConfig []Config

// NewForEachConfig returns a default ForEachConfig.
func NewForEachConfig() ForEachConfig {
	return []Config{}
}

//------------------------------------------------------------------------------

// ForEach is a processor that applies a list of child processors to each
// message of a batch individually.
type ForEach struct {
	children []types.Processor

	log log.Modular

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewForEach returns a ForEach processor.
func NewForEach(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	var children []types.Processor
	for i, pconf := range conf.ForEach {
		pMgr, pLog, pStats := interop.LabelChild(fmt.Sprintf("%v", i), mgr, log, stats)
		proc, err := New(pconf, pMgr, pLog, pStats)
		if err != nil {
			return nil, fmt.Errorf("child processor [%v]: %w", i, err)
		}
		children = append(children, proc)
	}
	return &ForEach{
		children: children,
		log:      log,

		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}, nil
}

// NewProcessBatch returns a ForEach processor.
func NewProcessBatch(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	var children []types.Processor
	for i, pconf := range conf.ProcessBatch {
		pMgr, pLog, pStats := interop.LabelChild(fmt.Sprintf("%v", i), mgr, log, stats)
		proc, err := New(pconf, pMgr, pLog, pStats)
		if err != nil {
			return nil, err
		}
		children = append(children, proc)
	}
	return &ForEach{
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
func (p *ForEach) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	p.mCount.Incr(1)

	individualMsgs := make([]types.Message, msg.Len())
	msg.Iter(func(i int, p types.Part) error {
		tmpMsg := message.New(nil)
		tmpMsg.SetAll([]types.Part{p})
		individualMsgs[i] = tmpMsg
		return nil
	})

	resMsg := message.New(nil)
	for _, tmpMsg := range individualMsgs {
		resultMsgs, res := ExecuteAll(p.children, tmpMsg)
		if res != nil && res.Error() != nil {
			return nil, res
		}
		for _, m := range resultMsgs {
			m.Iter(func(i int, p types.Part) error {
				resMsg.Append(p)
				return nil
			})
		}
	}

	if resMsg.Len() == 0 {
		return nil, response.NewAck()
	}

	p.mBatchSent.Incr(1)
	p.mSent.Incr(int64(resMsg.Len()))

	resMsgs := [1]types.Message{resMsg}
	return resMsgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (p *ForEach) CloseAsync() {
	for _, c := range p.children {
		c.CloseAsync()
	}
}

// WaitForClose blocks until the processor has closed down.
func (p *ForEach) WaitForClose(timeout time.Duration) error {
	stopBy := time.Now().Add(timeout)
	for _, c := range p.children {
		if err := c.WaitForClose(time.Until(stopBy)); err != nil {
			return err
		}
	}
	return nil
}

//------------------------------------------------------------------------------
