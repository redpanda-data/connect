package processor

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/tracing"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/google/go-cmp/cmp"
	opentracinglog "github.com/opentracing/opentracing-go/log"
	yaml "gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeWhile] = TypeSpec{
		constructor: NewWhile,
		Categories: []Category{
			CategoryComposition,
		},
		Summary: `
While is a processor that checks a [Bloblang query](/docs/guides/bloblang/about/) against messages and executes child processors on them for as long as the query resolves to true.`,
		Description: `
The field ` + "`at_least_once`" + `, if true, ensures that the child processors are always executed at least one time (like a do .. while loop.)

The field ` + "`max_loops`" + `, if greater than zero, caps the number of loops for a message batch to this value.

If following a loop execution the number of messages in a batch is reduced to zero the loop is exited regardless of the condition result. If following a loop execution there are more than 1 message batches the query is checked against the first batch only.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("at_least_once", "Whether to always run the child processors at least one time."),
			docs.FieldAdvanced("max_loops", "An optional maximum number of loops to execute. Helps protect against accidentally creating infinite loops."),
			docs.FieldCommon(
				"check",
				"A [Bloblang query](/docs/guides/bloblang/about/) that should return a boolean value indicating whether the while loop should execute again.",
				`errored()`,
				`this.urls.unprocessed.length() > 0`,
			).HasDefault(""),
			docs.FieldDeprecated("condition").HasType(docs.FieldCondition).OmitWhen(func(v, _ interface{}) (string, bool) {
				defaultBytes, err := yaml.Marshal(condition.NewConfig())
				if err != nil {
					return "", false
				}
				var iDefault interface{}
				if err = yaml.Unmarshal(defaultBytes, &iDefault); err != nil {
					return "", false
				}
				return "field condition is deprecated in favour of check", cmp.Equal(v, iDefault)
			}),
			docs.FieldCommon("processors", "A list of child processors to execute on each loop.").Array().HasType(docs.FieldProcessor),
		},
	}
}

//------------------------------------------------------------------------------

// WhileConfig is a config struct containing fields for the While
// processor.
type WhileConfig struct {
	AtLeastOnce bool             `json:"at_least_once" yaml:"at_least_once"`
	MaxLoops    int              `json:"max_loops" yaml:"max_loops"`
	Check       string           `json:"check" yaml:"check"`
	Condition   condition.Config `json:"condition" yaml:"condition"`
	Processors  []Config         `json:"processors" yaml:"processors"`
}

// NewWhileConfig returns a default WhileConfig.
func NewWhileConfig() WhileConfig {
	return WhileConfig{
		AtLeastOnce: false,
		MaxLoops:    0,
		Check:       "",
		Condition:   condition.NewConfig(),
		Processors:  []Config{},
	}
}

//------------------------------------------------------------------------------

// While is a processor that applies child processors for as long as a child
// condition resolves to true.
type While struct {
	running     int32
	maxLoops    int
	atLeastOnce bool
	cond        condition.Type
	check       *mapping.Executor
	children    []types.Processor

	log log.Modular

	mCount      metrics.StatCounter
	mLoop       metrics.StatCounter
	mCondFailed metrics.StatCounter
	mSent       metrics.StatCounter
	mBatchSent  metrics.StatCounter
}

// NewWhile returns a While processor.
func NewWhile(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	var cond condition.Type
	var check *mapping.Executor
	var err error

	if !isDefaultGroupCond(conf.While.Condition) {
		if cond, err = condition.New(
			conf.While.Condition, mgr,
			log.NewModule(".condition"),
			metrics.Namespaced(stats, "condition"),
		); err != nil {
			return nil, err
		}
	}
	if len(conf.While.Check) > 0 {
		if check, err = bloblang.NewMapping("", conf.While.Check); err != nil {
			return nil, fmt.Errorf("failed to parse check query: %w", err)
		}
	}

	if cond == nil && check == nil {
		return nil, errors.New("a check query is required")
	}

	if cond != nil && check != nil {
		return nil, errors.New("cannot specify both a condition and a check query")
	}

	var children []types.Processor
	for i, pconf := range conf.While.Processors {
		ns := fmt.Sprintf("while.%v", i)
		nsStats := metrics.Namespaced(stats, ns)
		nsLog := log.NewModule("." + ns)
		var proc Type
		if proc, err = New(pconf, mgr, nsLog, nsStats); err != nil {
			return nil, err
		}
		children = append(children, proc)
	}

	return &While{
		running:     1,
		maxLoops:    conf.While.MaxLoops,
		atLeastOnce: conf.While.AtLeastOnce,
		cond:        cond,
		check:       check,
		children:    children,

		log: log,

		mCount:      stats.GetCounter("count"),
		mLoop:       stats.GetCounter("loop"),
		mCondFailed: stats.GetCounter("failed"),
		mSent:       stats.GetCounter("sent"),
		mBatchSent:  stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

func (w *While) checkMsg(msg types.Message) bool {
	if w.cond != nil {
		return w.cond.Check(msg)
	}
	c, err := w.check.QueryPart(0, msg)
	if err != nil {
		c = false
		w.log.Errorf("Query failed for loop: %v\n", err)
	}
	return c
}

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (w *While) ProcessMessage(msg types.Message) (msgs []types.Message, res types.Response) {
	w.mCount.Incr(1)

	spans := tracing.CreateChildSpans(TypeWhile, msg)
	msgs = []types.Message{msg}

	loops := 0
	condResult := w.atLeastOnce || w.checkMsg(msg)
	for condResult {
		if atomic.LoadInt32(&w.running) != 1 {
			return nil, response.NewError(types.ErrTypeClosed)
		}
		if w.maxLoops > 0 && loops >= w.maxLoops {
			w.log.Traceln("Reached max loops count")
			break
		}

		w.mLoop.Incr(1)
		w.log.Traceln("Looped")
		for _, s := range spans {
			s.LogFields(opentracinglog.Event("loop"))
		}

		msgs, res = ExecuteAll(w.children, msgs...)
		if len(msgs) == 0 {
			return
		}
		condResult = w.checkMsg(msgs[0])
		loops++
	}

	for _, s := range spans {
		s.SetTag("result", condResult)
		s.Finish()
	}

	w.mBatchSent.Incr(int64(len(msgs)))
	totalParts := 0
	for _, msg := range msgs {
		totalParts += msg.Len()
	}
	w.mSent.Incr(int64(totalParts))
	return
}

// CloseAsync shuts down the processor and stops processing requests.
func (w *While) CloseAsync() {
	atomic.StoreInt32(&w.running, 0)
	for _, p := range w.children {
		p.CloseAsync()
	}
}

// WaitForClose blocks until the processor has closed down.
func (w *While) WaitForClose(timeout time.Duration) error {
	stopBy := time.Now().Add(timeout)
	for _, p := range w.children {
		if err := p.WaitForClose(time.Until(stopBy)); err != nil {
			return err
		}
	}
	return nil
}

//------------------------------------------------------------------------------
