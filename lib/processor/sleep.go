package processor

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSleep] = TypeSpec{
		constructor: NewSleep,
		Categories: []Category{
			CategoryUtility,
		},
		Summary: `
Sleep for a period of time specified as a duration string. This processor will
interpolate functions within the ` + "`duration`" + ` field, you can find a list
of functions [here](/docs/configuration/interpolation#bloblang-queries).`,
		Description: `
This processor executes once per message batch. In order to execute once for
each message of a batch place it within a
` + "[`for_each`](/docs/components/processors/for_each)" + ` processor:

` + "```yaml" + `
pipeline:
  processors:
    - for_each:
      - sleep:
          duration: ${! meta("sleep_for") }
` + "```" + ``,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldInterpolatedString("duration", "The duration of time to sleep for each execution."),
		},
	}
}

//------------------------------------------------------------------------------

// SleepConfig contains configuration fields for the Sleep processor.
type SleepConfig struct {
	Duration string `json:"duration" yaml:"duration"`
}

// NewSleepConfig returns a SleepConfig with default values.
func NewSleepConfig() SleepConfig {
	return SleepConfig{
		Duration: "100us",
	}
}

//------------------------------------------------------------------------------

// Sleep is a processor that limits the stream of a pipeline to one message
// batch per period specified.
type Sleep struct {
	closed    int32
	closeChan chan struct{}

	conf  Config
	log   log.Modular
	stats metrics.Type

	durationStr *field.Expression

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewSleep returns a Sleep processor.
func NewSleep(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (processor.V1, error) {
	durationStr, err := interop.NewBloblangField(mgr, conf.Sleep.Duration)
	if err != nil {
		return nil, fmt.Errorf("failed to parse duration expression: %v", err)
	}
	t := &Sleep{
		closeChan: make(chan struct{}),
		conf:      conf,
		log:       log,
		stats:     stats,

		durationStr: durationStr,

		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}
	return t, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (s *Sleep) ProcessMessage(msg *message.Batch) ([]*message.Batch, error) {
	s.mCount.Incr(1)

	spans := tracing.CreateChildSpans(TypeSleep, msg)
	defer func() {
		for _, span := range spans {
			span.Finish()
		}
	}()

	period, err := time.ParseDuration(s.durationStr.String(0, msg))
	if err != nil {
		s.log.Errorf("Failed to parse duration: %v\n", err)
		s.mErr.Incr(1)
	}
	select {
	case <-time.After(period):
	case <-s.closeChan:
	}

	s.mBatchSent.Incr(1)
	s.mSent.Incr(int64(msg.Len()))
	msgs := [1]*message.Batch{msg}
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (s *Sleep) CloseAsync() {
	if atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		close(s.closeChan)
	}
}

// WaitForClose blocks until the processor has closed down.
func (s *Sleep) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
