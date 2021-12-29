package processor

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func init() {
	Constructors[TypeWhile] = TypeSpec{
		constructor: func(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (processor.V1, error) {
			p, err := newWhile(conf.While, mgr)
			if err != nil {
				return nil, err
			}
			return processor.NewV2BatchedToV1Processor("while", p, mgr.Metrics()), nil
		},
		Categories: []Category{
			CategoryComposition,
		},
		Summary: `
A processor that checks a [Bloblang query](/docs/guides/bloblang/about/) against each batch of messages and executes child processors on them for as long as the query resolves to true.`,
		Description: `
The field ` + "`at_least_once`" + `, if true, ensures that the child processors are always executed at least one time (like a do .. while loop.)

The field ` + "`max_loops`" + `, if greater than zero, caps the number of loops for a message batch to this value.

If following a loop execution the number of messages in a batch is reduced to zero the loop is exited regardless of the condition result. If following a loop execution there are more than 1 message batches the query is checked against the first batch only.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("at_least_once", "Whether to always run the child processors at least one time."),
			docs.FieldAdvanced("max_loops", "An optional maximum number of loops to execute. Helps protect against accidentally creating infinite loops."),
			docs.FieldBloblang(
				"check",
				"A [Bloblang query](/docs/guides/bloblang/about/) that should return a boolean value indicating whether the while loop should execute again.",
				`errored()`,
				`this.urls.unprocessed.length() > 0`,
			).HasDefault(""),
			docs.FieldCommon("processors", "A list of child processors to execute on each loop.").Array().HasType(docs.FieldTypeProcessor),
		},
		UsesBatches: true,
	}
}

//------------------------------------------------------------------------------

// WhileConfig is a config struct containing fields for the While
// processor.
type WhileConfig struct {
	AtLeastOnce bool     `json:"at_least_once" yaml:"at_least_once"`
	MaxLoops    int      `json:"max_loops" yaml:"max_loops"`
	Check       string   `json:"check" yaml:"check"`
	Processors  []Config `json:"processors" yaml:"processors"`
}

// NewWhileConfig returns a default WhileConfig.
func NewWhileConfig() WhileConfig {
	return WhileConfig{
		AtLeastOnce: false,
		MaxLoops:    0,
		Check:       "",
		Processors:  []Config{},
	}
}

//------------------------------------------------------------------------------

type whileProc struct {
	maxLoops    int
	atLeastOnce bool
	check       *mapping.Executor
	children    []processor.V1
	log         log.Modular

	shutSig *shutdown.Signaller
}

func newWhile(conf WhileConfig, mgr interop.Manager) (*whileProc, error) {
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
		proc, err := New(pconf, pMgr, pMgr.Logger(), pMgr.Metrics())
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

func (w *whileProc) checkMsg(msg *message.Batch) bool {
	c, err := w.check.QueryPart(0, msg)
	if err != nil {
		c = false
		w.log.Errorf("Query failed for loop: %v", err)
	}
	return c
}

func (w *whileProc) ProcessBatch(ctx context.Context, spans []*tracing.Span, msg *message.Batch) (msgs []*message.Batch, res error) {
	msgs = []*message.Batch{msg}

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

		msgs, res = ExecuteAll(w.children, msgs...)
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
		p.CloseAsync()
	}
	deadline, exists := ctx.Deadline()
	if !exists {
		deadline = time.Now().Add(time.Second * 5)
	}
	for _, p := range w.children {
		if err := p.WaitForClose(time.Until(deadline)); err != nil {
			return err
		}
	}
	return nil
}
