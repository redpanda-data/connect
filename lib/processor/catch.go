package processor

import (
	"context"
	"strconv"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeCatch] = TypeSpec{
		constructor: func(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (processor.V1, error) {
			p, err := newCatch(conf.Catch, mgr)
			if err != nil {
				return nil, err
			}
			return processor.NewV2BatchedToV1Processor("catch", p, mgr.Metrics()), nil
		},
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

type catchProc struct {
	children []processor.V1
}

func newCatch(conf CatchConfig, mgr interop.Manager) (*catchProc, error) {
	var children []processor.V1
	for i, pconf := range conf {
		pMgr := mgr.IntoPath("catch", strconv.Itoa(i))
		proc, err := New(pconf, pMgr, pMgr.Logger(), pMgr.Metrics())
		if err != nil {
			return nil, err
		}
		children = append(children, proc)
	}
	return &catchProc{
		children: children,
	}, nil
}

//------------------------------------------------------------------------------

func (p *catchProc) ProcessBatch(ctx context.Context, spans []*tracing.Span, msg *message.Batch) ([]*message.Batch, error) {
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

	resMsgs := [1]*message.Batch{resMsg}
	return resMsgs[:], nil
}

func (p *catchProc) Close(ctx context.Context) error {
	for _, c := range p.children {
		c.CloseAsync()
	}
	deadline, exists := ctx.Deadline()
	if !exists {
		deadline = time.Now().Add(time.Second * 5)
	}
	for _, c := range p.children {
		if err := c.WaitForClose(time.Until(deadline)); err != nil {
			return err
		}
	}
	return nil
}
