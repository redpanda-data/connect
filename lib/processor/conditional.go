package processor

import (
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/tracing"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeConditional] = TypeSpec{
		constructor: NewConditional,
		Deprecated:  true,
		Summary: `
Executes a set of child processors when a [condition](/docs/components/conditions/about)
passes for a message batch, otherwise a different set of processors are applied.`,
		Description: `
## Alternatives

All functionality of this processor has been superseded by the
[switch](/docs/components/processors/switch) processor.

Conditional is a processor that has a list of child ` + "`processors`," + `
` + "`else_processors`, and a `condition`" + `. For each message batch, if the
condition passes, the child ` + "`processors`" + ` will be applied, otherwise
the ` + "`else_processors`" + ` are applied.

In order to conditionally process each message of a batch individually use this
processor with the ` + "[`for_each`](/docs/components/processors/for_each)" + ` processor.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("condition", "The [`condition`](/docs/components/conditions/about) to check against messages."),
			docs.FieldCommon("processors", "A list of processors to apply when the condition passes."),
			docs.FieldCommon("else_processors", "A list of processors to apply when the condition does not pass."),
		},
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			condSanit, err := condition.SanitiseConfig(conf.Conditional.Condition)
			if err != nil {
				return nil, err
			}
			procConfs := make([]interface{}, len(conf.Conditional.Processors))
			for i, pConf := range conf.Conditional.Processors {
				if procConfs[i], err = SanitiseConfig(pConf); err != nil {
					return nil, err
				}
			}
			elseProcConfs := make([]interface{}, len(conf.Conditional.ElseProcessors))
			for i, pConf := range conf.Conditional.ElseProcessors {
				if elseProcConfs[i], err = SanitiseConfig(pConf); err != nil {
					return nil, err
				}
			}
			return map[string]interface{}{
				"condition":       condSanit,
				"processors":      procConfs,
				"else_processors": elseProcConfs,
			}, nil
		},
	}
}

//------------------------------------------------------------------------------

// ConditionalConfig is a config struct containing fields for the Conditional
// processor.
type ConditionalConfig struct {
	Condition      condition.Config `json:"condition" yaml:"condition"`
	Processors     []Config         `json:"processors" yaml:"processors"`
	ElseProcessors []Config         `json:"else_processors" yaml:"else_processors"`
}

// NewConditionalConfig returns a default ConditionalConfig.
func NewConditionalConfig() ConditionalConfig {
	return ConditionalConfig{
		Condition:      condition.NewConfig(),
		Processors:     []Config{},
		ElseProcessors: []Config{},
	}
}

//------------------------------------------------------------------------------

// Conditional is a processor that only applies child processors under a certain
// condition.
type Conditional struct {
	cond         condition.Type
	children     []types.Processor
	elseChildren []types.Processor

	log log.Modular

	mCount      metrics.StatCounter
	mCondPassed metrics.StatCounter
	mCondFailed metrics.StatCounter
	mSent       metrics.StatCounter
	mBatchSent  metrics.StatCounter
}

// NewConditional returns a Conditional processor.
func NewConditional(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	cond, err := condition.New(conf.Conditional.Condition, mgr, log.NewModule(".condition"), metrics.Namespaced(stats, "condition"))
	if err != nil {
		return nil, err
	}

	var children []types.Processor
	for i, pconf := range conf.Conditional.Processors {
		ns := fmt.Sprintf("if.%v", i)
		nsStats := metrics.Namespaced(stats, ns)
		nsLog := log.NewModule("." + ns)
		var proc Type
		if proc, err = New(pconf, mgr, nsLog, nsStats); err != nil {
			return nil, err
		}
		children = append(children, proc)
	}

	var elseChildren []types.Processor
	for i, pconf := range conf.Conditional.ElseProcessors {
		ns := fmt.Sprintf("else.%v", i)
		nsStats := metrics.Namespaced(stats, ns)
		nsLog := log.NewModule("." + ns)
		var proc Type
		if proc, err = New(pconf, mgr, nsLog, nsStats); err != nil {
			return nil, err
		}
		elseChildren = append(elseChildren, proc)
	}

	return &Conditional{
		cond:         cond,
		children:     children,
		elseChildren: elseChildren,

		log: log,

		mCount:      stats.GetCounter("count"),
		mCondPassed: stats.GetCounter("passed"),
		mCondFailed: stats.GetCounter("failed"),
		mSent:       stats.GetCounter("sent"),
		mBatchSent:  stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (c *Conditional) ProcessMessage(msg types.Message) (msgs []types.Message, res types.Response) {
	c.mCount.Incr(1)

	var procs []types.Processor

	spans := tracing.CreateChildSpans(TypeConditional, msg)

	condResult := c.cond.Check(msg)
	if condResult {
		c.mCondPassed.Incr(1)
		c.log.Traceln("Condition passed")
		procs = c.children
	} else {
		c.mCondFailed.Incr(1)
		c.log.Traceln("Condition failed")
		procs = c.elseChildren
	}
	for _, s := range spans {
		s.SetTag("result", condResult)
		s.Finish()
	}

	resultMsgs, resultRes := ExecuteAll(procs, msg)
	if len(resultMsgs) == 0 {
		res = resultRes
	} else {
		c.mBatchSent.Incr(int64(len(resultMsgs)))
		totalParts := 0
		for _, msg := range resultMsgs {
			totalParts += msg.Len()
		}
		c.mSent.Incr(int64(totalParts))
		msgs = resultMsgs
	}

	return
}

// CloseAsync shuts down the processor and stops processing requests.
func (c *Conditional) CloseAsync() {
	for _, p := range c.children {
		p.CloseAsync()
	}
	for _, p := range c.elseChildren {
		p.CloseAsync()
	}
}

// WaitForClose blocks until the processor has closed down.
func (c *Conditional) WaitForClose(timeout time.Duration) error {
	stopBy := time.Now().Add(timeout)
	for _, p := range c.children {
		if err := p.WaitForClose(time.Until(stopBy)); err != nil {
			return err
		}
	}
	for _, p := range c.elseChildren {
		if err := p.WaitForClose(time.Until(stopBy)); err != nil {
			return err
		}
	}
	return nil
}

//------------------------------------------------------------------------------
