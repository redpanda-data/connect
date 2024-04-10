package pure

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/benthosdev/benthos/v4/internal/bloblang/mapping"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	wpFieldAtLeastOnce = "at_least_once"
	wpFieldMaxLoops    = "max_loops"
	wpFieldCheck       = "check"
	wpFieldProcessors  = "processors"
)

func whileProcSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Composition").
		Stable().
		Summary("A processor that checks a [Bloblang query](/docs/guides/bloblang/about/) against each batch of messages and executes child processors on them for as long as the query resolves to true.").
		Description(`
The field `+"`at_least_once`"+`, if true, ensures that the child processors are always executed at least one time (like a do .. while loop.)

The field `+"`max_loops`"+`, if greater than zero, caps the number of loops for a message batch to this value.

If following a loop execution the number of messages in a batch is reduced to zero the loop is exited regardless of the condition result. If following a loop execution there are more than 1 message batches the query is checked against the first batch only.

The conditions of this processor are applied across entire message batches. You can find out more about batching [in this doc](/docs/configuration/batching).`).
		Fields(

			service.NewBoolField(wpFieldAtLeastOnce).
				Description("Whether to always run the child processors at least one time.").
				Default(false),
			service.NewIntField(wpFieldMaxLoops).
				Description("An optional maximum number of loops to execute. Helps protect against accidentally creating infinite loops.").
				Advanced().
				Default(0),
			service.NewBloblangField(wpFieldCheck).
				Description("A [Bloblang query](/docs/guides/bloblang/about/) that should return a boolean value indicating whether the while loop should execute again.").
				Examples(`errored()`, `this.urls.unprocessed.length() > 0`).
				Default(""),
			service.NewProcessorListField(wpFieldProcessors).
				Description("A list of child processors to execute on each loop."),
		)
}

func init() {
	err := service.RegisterBatchProcessor(
		"while", whileProcSpec(),
		func(conf *service.ParsedConfig, res *service.Resources) (service.BatchProcessor, error) {
			maxLoops, err := conf.FieldInt(wpFieldMaxLoops)
			if err != nil {
				return nil, err
			}

			atLeastOnce, err := conf.FieldBool(wpFieldAtLeastOnce)
			if err != nil {
				return nil, err
			}

			checkStr, err := conf.FieldString(wpFieldCheck)
			if err != nil {
				return nil, err
			}

			iProcs, err := conf.FieldProcessorList(wpFieldProcessors)
			if err != nil {
				return nil, err
			}

			children := make([]processor.V1, len(iProcs))
			for i, c := range iProcs {
				children[i] = interop.UnwrapOwnedProcessor(c)
			}

			mgr := interop.UnwrapManagement(res)
			p, err := newWhile(maxLoops, atLeastOnce, checkStr, children, mgr)
			if err != nil {
				return nil, err
			}
			return interop.NewUnwrapInternalBatchProcessor(processor.NewAutoObservedBatchedProcessor("while", p, mgr)), nil
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

func newWhile(maxLoops int, atLeastOnce bool, checkStr string, children []processor.V1, mgr bundle.NewManagement) (*whileProc, error) {
	var check *mapping.Executor
	var err error

	if checkStr != "" {
		if check, err = mgr.BloblEnvironment().NewMapping(checkStr); err != nil {
			return nil, fmt.Errorf("failed to parse check query: %w", err)
		}
	} else {
		return nil, errors.New("a check query is required")
	}

	return &whileProc{
		maxLoops:    maxLoops,
		atLeastOnce: atLeastOnce,
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
		w.log.Error("Query failed for loop: %v", err)
	}
	return c
}

func (w *whileProc) ProcessBatch(ctx *processor.BatchProcContext, msg message.Batch) (msgs []message.Batch, res error) {
	msgs = []message.Batch{msg}

	loops := 0
	condResult := w.atLeastOnce || w.checkMsg(msg)
	for condResult {
		if w.shutSig.ShouldCloseAtLeisure() || ctx.Context().Err() != nil {
			return nil, component.ErrTypeClosed
		}
		if w.maxLoops > 0 && loops >= w.maxLoops {
			w.log.Trace("Reached max loops count")
			break
		}

		w.log.Trace("Looped")
		for i := range msg {
			ctx.Span(i).LogKV("event", "loop")
		}

		msgs, res = processor.ExecuteAll(ctx.Context(), w.children, msgs...)
		if len(msgs) == 0 {
			return
		}
		condResult = w.checkMsg(msgs[0])
		loops++
	}

	for i := range msg {
		ctx.Span(i).SetTag("result", strconv.FormatBool(condResult))
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
