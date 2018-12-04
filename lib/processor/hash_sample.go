// Copyright (c) 2018 Lorenzo Alberton
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package processor

import (
	"math"

	"github.com/OneOfOne/xxhash"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/response"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeHashSample] = TypeSpec{
		constructor: NewHashSample,
		description: `
Retains a percentage of messages deterministically by hashing selected parts of
the message and checking the hash against a valid range, dropping all others.

For example, setting ` + "`retain_min` to `0.0` and `remain_max` to `50.0`" + `
results in dropping half of the input stream, and setting ` + "`retain_min`" + `
to ` + "`50.0` and `retain_max` to `100.1`" + ` will drop the _other_ half.`,
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
	mErrHash   metrics.StatCounter
	mSent      metrics.StatCounter
	mSentParts metrics.StatCounter
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
		mErrHash:   stats.GetCounter("hashing_error"),
		mSent:      stats.GetCounter("sent"),
		mSentParts: stats.GetCounter("parts.sent"),
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
		if _, err := hash.Write(msg.Get(index).Get()); nil != err {
			s.mErrHash.Incr(1)
			s.log.Debugf("Cannot hash message part for sampling: %v\n", err)
			return nil, response.NewAck()
		}
	}

	rate := scaleNum(hash.Sum64())
	if rate >= s.conf.HashSample.RetainMin && rate < s.conf.HashSample.RetainMax {
		s.mSent.Incr(1)
		s.mSentParts.Incr(int64(msg.Len()))
		msgs := [1]types.Message{msg}
		return msgs[:], nil
	}

	s.mDropped.Incr(1)
	return nil, response.NewAck()
}

//------------------------------------------------------------------------------
