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
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["hash_sample"] = TypeSpec{
		constructor: NewHashSample,
		description: `
Passes on a percentage of messages deterministically by hashing selected parts
of the message and checking the hash against a valid range, dropping all others.

For example, a 'hash_sample' with 'retain_min' of 0.0 and 'remain_max' of 50.0
will receive half of the input stream, and a 'hash_sample' with 'retain_min' of
50.0 and 'retain_max' of 100.1 will receive the other half.

The part indexes can be negative, and if so the part will be selected from the
end counting backwards starting from -1. E.g. if index = -1 then the selected
part will be the last part of the message, if index = -2 then the part before
the last element with be selected, and so on.`,
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

// HashSampleConfig contains any configuration for the HashSample processor.
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

// HashSample is a processor that checks each message against a set of bounds
// and rejects messages if they aren't within them.
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
		log:   log.NewModule(".processor.hash_sample"),
		stats: stats,

		mCount:     stats.GetCounter("processor.hash_sample.count"),
		mDropOOB:   stats.GetCounter("processor.hash_sample.dropped_part_out_of_bounds"),
		mDropped:   stats.GetCounter("processor.hash_sample.dropped"),
		mErrHash:   stats.GetCounter("processor.hash_sample.hashing_error"),
		mSent:      stats.GetCounter("processor.hash_sample.sent"),
		mSentParts: stats.GetCounter("processor.hash_sample.parts.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage checks each message against a set of bounds.
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
			return nil, types.NewSimpleResponse(nil)
		}

		// Attempt to add part to hash.
		if _, err := hash.Write(msg.Get(index)); nil != err {
			s.mErrHash.Incr(1)
			s.log.Debugf("Cannot hash message part for sampling: %v\n", err)
			return nil, types.NewSimpleResponse(nil)
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
	return nil, types.NewSimpleResponse(nil)
}

//------------------------------------------------------------------------------
