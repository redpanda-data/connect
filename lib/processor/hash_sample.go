package processor

import (
	"math"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/OneOfOne/xxhash"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeHashSample] = TypeSpec{
		constructor: NewHashSample,
		Status:      docs.StatusDeprecated,
		Footnotes: `
## Alternatives

All functionality of this processor has been superseded by the
[bloblang](/docs/components/processors/bloblang) processor.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("retain_min", "The lower percentage of the sample range."),
			docs.FieldCommon("retain_max", "The upper percentage of the sample range."),
			docs.FieldAdvanced("parts", "An array of message indexes within the batch to sample based on. If left empty all messages are included. This field is only applicable when batching messages [at the input level](/docs/configuration/batching).").Array(),
		},
	}
}

//------------------------------------------------------------------------------

// hashSamplingNorm is the constant factor to normalise a uint64 into the
// (0.0, 100.0) range.
const hashSamplingNorm = 100.0 / float64(math.MaxUint64)

func scaleNum(n uint64) float64 {
	return float64(n) * hashSamplingNorm
}

//------------------------------------------------------------------------------

// HashSampleConfig contains configuration fields for the HashSample processor.
type HashSampleConfig struct {
	RetainMin float64 `json:"retain_min" yaml:"retain_min"`
	RetainMax float64 `json:"retain_max" yaml:"retain_max"`
	Parts     []int   `json:"parts" yaml:"parts"` // message parts to hash
}

// NewHashSampleConfig returns a HashSampleConfig with default values.
func NewHashSampleConfig() HashSampleConfig {
	return HashSampleConfig{
		RetainMin: 0.0,
		RetainMax: 10.0,     // retain the first [0, 10%) interval
		Parts:     []int{0}, // only consider the 1st part
	}
}

//------------------------------------------------------------------------------

// HashSample is a processor that removes messages based on a sample factor by
// hashing its contents.
type HashSample struct {
	conf  Config
	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mDropOOB   metrics.StatCounter
	mDropped   metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewHashSample returns a HashSample processor.
func NewHashSample(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	return &HashSample{
		conf:  conf,
		log:   log,
		stats: stats,

		mCount:     stats.GetCounter("count"),
		mDropOOB:   stats.GetCounter("dropped_part_out_of_bounds"),
		mDropped:   stats.GetCounter("dropped"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (s *HashSample) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	s.mCount.Incr(1)

	hash := xxhash.New64()

	lParts := msg.Len()
	for _, index := range s.conf.HashSample.Parts {
		if index < 0 {
			// Negative indexes count backwards from the end.
			index = lParts + index
		}

		// Check boundary of part index.
		if index < 0 || index >= lParts {
			s.mDropOOB.Incr(1)
			s.mDropped.Incr(1)
			s.log.Debugf("Cannot sample message part %v for parts count: %v\n", index, lParts)
			return nil, response.NewAck()
		}

		// Attempt to add part to hash.
		if _, err := hash.Write(msg.Get(index).Get()); err != nil {
			s.mErr.Incr(1)
			s.log.Debugf("Cannot hash message part for sampling: %v\n", err)
			return nil, response.NewAck()
		}
	}

	rate := scaleNum(hash.Sum64())
	if rate >= s.conf.HashSample.RetainMin && rate < s.conf.HashSample.RetainMax {
		s.mBatchSent.Incr(1)
		s.mSent.Incr(int64(msg.Len()))
		msgs := [1]types.Message{msg}
		return msgs[:], nil
	}

	s.mDropped.Incr(int64(msg.Len()))
	return nil, response.NewAck()
}

// CloseAsync shuts down the processor and stops processing requests.
func (s *HashSample) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (s *HashSample) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
