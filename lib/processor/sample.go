package processor

import (
	"math/rand"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSample] = TypeSpec{
		constructor: NewSample,
		Description: `
Retains a randomly sampled percentage of message batches (0 to 100) and drops
all others. The random seed is static in order to sample deterministically, but
can be set in config to allow parallel samples that are unique.`,
	}
}

//------------------------------------------------------------------------------

// SampleConfig contains configuration fields for the Sample processor.
type SampleConfig struct {
	Retain     float64 `json:"retain" yaml:"retain"`
	RandomSeed int64   `json:"seed" yaml:"seed"`
}

// NewSampleConfig returns a SampleConfig with default values.
func NewSampleConfig() SampleConfig {
	return SampleConfig{
		Retain:     10.0, // 10%
		RandomSeed: 0,
	}
}

//------------------------------------------------------------------------------

// Sample is a processor that drops messages based on a random sample.
type Sample struct {
	conf  Config
	log   log.Modular
	stats metrics.Type

	retain float64
	gen    *rand.Rand
	mut    sync.Mutex

	mCount     metrics.StatCounter
	mDropped   metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewSample returns a Sample processor.
func NewSample(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	gen := rand.New(rand.NewSource(conf.Sample.RandomSeed))
	return &Sample{
		conf:   conf,
		log:    log,
		stats:  stats,
		retain: conf.Sample.Retain / 100.0,
		gen:    gen,

		mCount:     stats.GetCounter("count"),
		mDropped:   stats.GetCounter("dropped"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (s *Sample) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	s.mCount.Incr(1)
	s.mut.Lock()
	defer s.mut.Unlock()
	if s.gen.Float64() > s.retain {
		s.mDropped.Incr(1)
		return nil, response.NewAck()
	}
	s.mBatchSent.Incr(1)
	s.mSent.Incr(int64(msg.Len()))
	msgs := [1]types.Message{msg}
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (s *Sample) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (s *Sample) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
