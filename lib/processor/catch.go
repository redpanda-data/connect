package processor

import (
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeCatch] = TypeSpec{
		constructor: NewCatch,
		Categories: []Category{
			CategoryComposition,
		},
		Summary: `
Applies a list of child processors _only_ when a previous processing step has
failed.`,
		Description: `
Behaves similarly to the ` + "[`for_each`](/docs/components/processors/for_each)" + ` processor, where a
list of child processors are applied to individual messages of a batch. However,
processors are only applied to messages that failed a processing step prior to
the catch.

For example, with the following config:

` + "```yaml" + `
pipeline:
  processors:
    - resource: foo
    - catch:
      - resource: bar
      - resource: baz
` + "```" + `

If the processor ` + "`foo`" + ` fails for a particular message, that message
will be fed into the processors ` + "`bar` and `baz`" + `. Messages that do not
fail for the processor ` + "`foo`" + ` will skip these processors.

When messages leave the catch block their fail flags are cleared. This processor
is useful for when it's possible to recover failed messages, or when special
actions (such as logging/metrics) are required before dropping them.

More information about error handing can be found [here](/docs/configuration/error_handling).`,
		config: docs.FieldComponent().Array().HasType(docs.FieldTypeProcessor).
			Linter(func(ctx docs.LintContext, line, col int, value interface{}) []docs.Lint {
				childProcs, ok := value.([]interface{})
				if !ok {
					return nil
				}
				for _, child := range childProcs {
					childObj, ok := child.(map[string]interface{})
					if !ok {
						continue
					}
					if _, exists := childObj["catch"]; exists {
						// No need to lint as a nested catch will clear errors,
						// allowing nested try blocks to work as expected.
						return nil
					}
					if _, exists := childObj["try"]; exists {
						return []docs.Lint{
							docs.NewLintError(line, "`catch` block contains a `try` block which will never execute due to errors only being cleared at the end of the `catch`, for more information about nesting `try` within `catch` read: https://www.benthos.dev/docs/components/processors/try#nesting-within-a-catch-block"),
						}
					}
				}
				return nil
			}),
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
	children []processor.V1

	log log.Modular

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewCatch returns a Catch processor.
func NewCatch(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (processor.V1, error) {
	var children []processor.V1
	for i, pconf := range conf.Catch {
		pMgr, pLog, pStats := interop.LabelChild(fmt.Sprintf("%v", i), mgr, log, stats)
		proc, err := New(pconf, pMgr, pLog, pStats)
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
func (p *Catch) ProcessMessage(msg *message.Batch) ([]*message.Batch, error) {
	p.mCount.Incr(1)

	resultMsgs := make([]*message.Batch, msg.Len())
	_ = msg.Iter(func(i int, p *message.Part) error {
		tmpMsg := message.QuickBatch(nil)
		tmpMsg.SetAll([]*message.Part{p})
		resultMsgs[i] = tmpMsg
		return nil
	})

	var res error
	if resultMsgs, res = ExecuteCatchAll(p.children, resultMsgs...); res != nil || len(resultMsgs) == 0 {
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

	_ = resMsg.Iter(func(i int, p *message.Part) error {
		ClearFail(p)
		return nil
	})

	p.mBatchSent.Incr(1)
	p.mSent.Incr(int64(resMsg.Len()))

	resMsgs := [1]*message.Batch{resMsg}
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
