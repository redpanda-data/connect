package processor

import (
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeTry] = TypeSpec{
		constructor: NewTry,
		Categories: []Category{
			CategoryComposition,
		},
		Summary: `Executes a list of child processors on messages only if no prior processors have failed (or the errors have been cleared).`,
		Description: `
This processor behaves similarly to the ` + "[`for_each`](/docs/components/processors/for_each)" + ` processor, where a list of child processors are applied to individual messages of a batch. However, if a message has failed any prior processor (before or during the try block) then that message will skip all following processors.

For example, with the following config:

` + "```yaml" + `
pipeline:
  processors:
    - resource: foo
    - try:
      - resource: bar
      - resource: baz
      - resource: buz
` + "```" + `

If the processor ` + "`bar`" + ` fails for a particular message, that message will skip the processors ` + "`baz` and `buz`" + `. Similarly, if ` + "`bar`" + ` succeeds but ` + "`baz`" + ` does not then ` + "`buz`" + ` will be skipped. If the processor ` + "`foo`" + ` fails for a message then none of ` + "`bar`, `baz` or `buz`" + ` are executed on that message.

This processor is useful for when child processors depend on the successful output of previous processors. This processor can be followed with a ` + "[catch](/docs/components/processors/catch)" + ` processor for defining child processors to be applied only to failed messages.

More information about error handing can be found [here](/docs/configuration/error_handling).

### Nesting within a catch block

In some cases it might be useful to nest a try block within a catch block, since the ` + "[`catch` processor](/docs/components/processors/catch)" + ` only clears errors _after_ executing its child processors this means a nested try processor will not execute unless the errors are explicitly cleared beforehand.

This can be done by inserting an empty catch block before the try block like as follows:

` + "```yaml" + `
pipeline:
  processors:
    - resource: foo
    - catch:
      - log:
          level: ERROR
          message: "Foo failed due to: ${! error() }"
      - catch: [] # Clear prior error
      - try:
        - resource: bar
        - resource: baz
` + "```" + `


`,
		config: docs.FieldComponent().Array().HasType(docs.FieldTypeProcessor),
	}
}

//------------------------------------------------------------------------------

// TryConfig is a config struct containing fields for the Try processor.
type TryConfig []Config

// NewTryConfig returns a default TryConfig.
func NewTryConfig() TryConfig {
	return []Config{}
}

//------------------------------------------------------------------------------

// Try is a processor that applies a list of child processors to each message of
// a batch individually, where processors are skipped for messages that failed a
// previous processor step.
type Try struct {
	children []types.Processor

	log log.Modular

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewTry returns a Try processor.
func NewTry(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	var children []types.Processor
	for i, pconf := range conf.Try {
		pMgr, pLog, pStats := interop.LabelChild(fmt.Sprintf("%v", i), mgr, log, stats)
		proc, err := New(pconf, pMgr, pLog, pStats)
		if err != nil {
			return nil, err
		}
		children = append(children, proc)
	}
	return &Try{
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
func (p *Try) ProcessMessage(msg *message.Batch) ([]*message.Batch, error) {
	p.mCount.Incr(1)

	resultMsgs := make([]*message.Batch, msg.Len())
	_ = msg.Iter(func(i int, p *message.Part) error {
		tmpMsg := message.QuickBatch(nil)
		tmpMsg.SetAll([]*message.Part{p})
		resultMsgs[i] = tmpMsg
		return nil
	})

	var res error
	if resultMsgs, res = ExecuteTryAll(p.children, resultMsgs...); res != nil || len(resultMsgs) == 0 {
		return nil, res
	}

	resMsg := message.QuickBatch(nil)
	for _, m := range resultMsgs {
		_ = m.Iter(func(i int, p *message.Part) error {
			resMsg.Append(p)
			return nil
		})
	}
	if resMsg.Len() == 0 {
		return nil, res
	}

	p.mBatchSent.Incr(1)
	p.mSent.Incr(int64(resMsg.Len()))

	resMsgs := [1]*message.Batch{resMsg}
	return resMsgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (p *Try) CloseAsync() {
	for _, c := range p.children {
		c.CloseAsync()
	}
}

// WaitForClose blocks until the processor has closed down.
func (p *Try) WaitForClose(timeout time.Duration) error {
	stopBy := time.Now().Add(timeout)
	for _, c := range p.children {
		if err := c.WaitForClose(time.Until(stopBy)); err != nil {
			return err
		}
	}
	return nil
}

//------------------------------------------------------------------------------
