package condition

import (
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeProcessorFailed] = TypeSpec{
		constructor: NewProcessorFailed,
		Description: `
Returns true if a processing stage of a message has failed. This condition is
useful for dropping failed messages or creating dead letter queues, you can read
more about these patterns [here](/docs/configuration/error_handling).`,
	}
}

//------------------------------------------------------------------------------

// ProcessorFailedConfig is a configuration struct containing fields for the
// processor_failed condition.
type ProcessorFailedConfig struct {
	Part int `json:"part" yaml:"part"`
}

// NewProcessorFailedConfig returns a ProcessorFailedConfig with default values.
func NewProcessorFailedConfig() ProcessorFailedConfig {
	return ProcessorFailedConfig{
		Part: 0,
	}
}

//------------------------------------------------------------------------------

// ProcessorFailed is a condition that checks whether processing steps have
// failed for a message.
type ProcessorFailed struct {
	part int

	mCount metrics.StatCounter
	mTrue  metrics.StatCounter
	mFalse metrics.StatCounter
}

// NewProcessorFailed returns a ProcessorFailed condition.
func NewProcessorFailed(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	return &ProcessorFailed{
		part: conf.ProcessorFailed.Part,

		mCount: stats.GetCounter("count"),
		mTrue:  stats.GetCounter("true"),
		mFalse: stats.GetCounter("false"),
	}, nil
}

//------------------------------------------------------------------------------

// Check attempts to check a message part against a configured condition.
func (p *ProcessorFailed) Check(msg types.Message) bool {
	p.mCount.Incr(1)
	if l := len(msg.Get(p.part).Metadata().Get("benthos_processing_failed")); l > 0 {
		p.mTrue.Incr(1)
		return true
	}
	p.mFalse.Incr(1)
	return false
}

//------------------------------------------------------------------------------
