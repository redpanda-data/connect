package pure

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/benthosdev/benthos/v4/internal/bloblang/mapping"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/internal/tracing"
)

func init() {
	err := bundle.AllProcessors.Add(func(conf processor.Config, mgr bundle.NewManagement) (processor.V1, error) {
		p, err := newWhile(conf.While, mgr)
		if err != nil {
			return nil, err
		}
		return processor.NewV2BatchedToV1Processor("while", p, mgr), nil
	}, docs.ComponentSpec{
		Name: "while",
		Categories: []string{
			"Composition",
		},
		Summary: `
A processor that checks a [Bloblang query](/docs/guides/bloblang/about/) against each batch of messages and executes child processors on them for as long as the query resolves to true.`,
		Description: `
The field ` + "`at_least_once`" + `, if true, ensures that the child processors are always executed at least one time (like a do .. while loop.)

The field ` + "`max_loops`" + `, if greater than zero, caps the number of loops for a message batch to this value.

If following a loop execution the number of messages in a batch is reduced to zero the loop is exited regardless of the condition result. If following a loop execution there are more than 1 message batches the query is checked against the first batch only.

The conditions of this processor are applied across entire message batches. You can find out more about batching [in this doc](/docs/configuration/batching).`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldBool("at_least_once", "Whether to always run the child processors at least one time."),
			docs.FieldInt("max_loops", "An optional maximum number of loops to execute. Helps protect against accidentally creating infinite loops.").Advanced(),
			docs.FieldBloblang(
				"check",
				"A [Bloblang query](/docs/guides/bloblang/about/) that should return a boolean value indicating whether the while loop should execute again.",
				`errored()`,
				`this.urls.unprocessed.length() > 0`,
			).HasDefault(""),
			docs.FieldProcessor("processors", "A list of child processors to execute on each loop.").Array(),
		).ChildDefaultAndTypesFromStruct(processor.NewWhileConfig()),
	})
	if err != nil {
		panic(err)
	}
}

type whileProc struct {
	maxLoops    int
	atLeastOnce bool
	check       *mapping.Executor
	children    []processor.V1
	log         log.Modular

	shutSig *shutdown.Signaller
}

func newWhile(conf processor.WhileConfig, mgr bundle.NewManagement) (*whileProc, error) {
	var check *mapping.Executor
	var err error

	if len(conf.Check) > 0 {
		if check, err = mgr.BloblEnvironment().NewMapping(conf.Check); err != nil {
			return nil, fmt.Errorf("failed to parse check query: %w", err)
		}
	} else {
		return nil, errors.New("a check query is required")
	}

	var children []processor.V1
	for i, pconf := range conf.Processors {
		pMgr := mgr.IntoPath("while", "processors", strconv.Itoa(i))
		proc, err := pMgr.NewProcessor(pconf)
		if err != nil {
			return nil, err
		}
		children = append(children, proc)
	}

	return &whileProc{
		maxLoops:    conf.MaxLoops,
		atLeastOnce: conf.AtLeastOnce,
		check:       check,
		children:    children,
		log:         mgr.Logger(),
		shutSig:     shutdown.NewSignaller(),
	}, nil
}

func (w *whileProc) checkMsg(msg message.Batch) bool {
	c, err := w.check.QueryPart(0, msg)
	if err != nil {
		c = false
		w.log.Errorf("Query failed for loop: %v", err)
	}
	return c
}

func (w *whileProc) ProcessBatch(ctx context.Context, spans []*tracing.Span, msg message.Batch) (msgs []message.Batch, res error) {
	msgs = []message.Batch{msg}

	loops := 0
	condResult := w.atLeastOnce || w.checkMsg(msg)
	for condResult {
		if w.shutSig.ShouldCloseAtLeisure() || ctx.Err() != nil {
			return nil, component.ErrTypeClosed
		}
		if w.maxLoops > 0 && loops >= w.maxLoops {
			w.log.Traceln("Reached max loops count")
			break
		}

		w.log.Traceln("Looped")
		for _, s := range spans {
			s.LogKV("event", "loop")
		}

		msgs, res = processor.ExecuteAll(ctx, w.children, msgs...)
		if len(msgs) == 0 {
			return
		}
		condResult = w.checkMsg(msgs[0])
		loops++
	}

	for _, s := range spans {
		s.SetTag("result", strconv.FormatBool(condResult))
	}

	totalParts := 0
	for _, msg := range msgs {
		totalParts += msg.Len()
	}
	return
}

func (w *whileProc) Close(ctx context.Context) error {
	w.shutSig.CloseNow()
	for _, p := range w.children {
		if err := p.Close(ctx); err != nil {
			return err
		}
	}
	return nil
}
