package pure

import (
	"context"
	"strconv"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/tracing"
)

func init() {
	err := bundle.AllProcessors.Add(func(conf processor.Config, mgr bundle.NewManagement) (processor.V1, error) {
		p, err := newCatch(conf.Catch, mgr)
		if err != nil {
			return nil, err
		}
		return processor.NewV2BatchedToV1Processor("catch", p, mgr), nil
	}, docs.ComponentSpec{
		Name: "catch",
		Categories: []string{
			"Composition",
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

More information about error handling can be found [here](/docs/configuration/error_handling).`,
		Config: docs.FieldProcessor("", "").Array().
			LinterFunc(func(ctx docs.LintContext, line, col int, value any) []docs.Lint {
				childProcs, ok := value.([]any)
				if !ok {
					return nil
				}
				for _, child := range childProcs {
					childObj, ok := child.(map[string]any)
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
							docs.NewLintError(line, docs.LintCustom, "`catch` block contains a `try` block which will never execute due to errors only being cleared at the end of the `catch`, for more information about nesting `try` within `catch` read: https://www.benthos.dev/docs/components/processors/try#nesting-within-a-catch-block"),
						}
					}
				}
				return nil
			}),
	})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type catchProc struct {
	children []processor.V1
}

func newCatch(conf []processor.Config, mgr bundle.NewManagement) (*catchProc, error) {
	var children []processor.V1
	for i, pconf := range conf {
		pMgr := mgr.IntoPath("catch", strconv.Itoa(i))
		proc, err := pMgr.NewProcessor(pconf)
		if err != nil {
			return nil, err
		}
		children = append(children, proc)
	}
	return &catchProc{
		children: children,
	}, nil
}

func (p *catchProc) ProcessBatch(ctx context.Context, spans []*tracing.Span, msg message.Batch) ([]message.Batch, error) {
	resultMsgs := make([]message.Batch, msg.Len())
	_ = msg.Iter(func(i int, p *message.Part) error {
		resultMsgs[i] = message.Batch{p}
		return nil
	})

	var res error
	if resultMsgs, res = processor.ExecuteCatchAll(ctx, p.children, resultMsgs...); res != nil || len(resultMsgs) == 0 {
		return nil, res
	}

	resMsg := message.QuickBatch(nil)
	for _, m := range resultMsgs {
		_ = m.Iter(func(i int, p *message.Part) error {
			resMsg = append(resMsg, p)
			return nil
		})
	}
	if resMsg.Len() == 0 {
		return nil, res
	}

	_ = resMsg.Iter(func(i int, p *message.Part) error {
		p.ErrorSet(nil)
		return nil
	})

	resMsgs := [1]message.Batch{resMsg}
	return resMsgs[:], nil
}

func (p *catchProc) Close(ctx context.Context) error {
	for _, child := range p.children {
		if err := child.Close(ctx); err != nil {
			return err
		}
	}
	return nil
}
