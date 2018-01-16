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

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

//------------------------------------------------------------------------------

func init() {
	constructors["hash_sample"] = typeSpec{
		constructor: NewHashSample,
		description: `
Passes on a percentage of messages, deterministically by hashing the message and
checking the hash against a valid range, and drops all others.`,
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
		RetainMax: 0.1,      // retain the first [0, 10%) interval
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
}

// NewHashSample returns a HashSample processor.
func NewHashSample(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	return &HashSample{
		conf:  conf,
		log:   log.NewModule(".processor.hash_sample"),
		stats: stats,
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage checks each message against a set of bounds.
func (s *HashSample) ProcessMessage(msg *types.Message) (*types.Message, types.Response, bool) {
	s.stats.Incr("processor.hash_sample.count", 1)

	hash := xxhash.New64()

	lParts := len(msg.Parts)
	for _, index := range s.conf.HashSample.Parts {
		// check boundary of parts first
		if index >= lParts {
			s.stats.Incr("processor.hash_sample.dropped_part_out_of_bounds", 1)
			s.stats.Incr("processor.hash_sample.dropped", 1)
			s.log.Errorf("Cannot sample message due to parts count: %v != 1\n", lParts)
			return nil, types.NewSimpleResponse(nil), false
		}
		_, err := hash.Write(msg.Parts[index])
		if nil != err {
			s.stats.Incr("processor.hash_sample.hashing_error", 1)
			s.log.Errorf("Cannot hash message part for sampling: %v\n", err)
			return nil, types.NewSimpleResponse(nil), false
		}
	}

	rate := scaleNum(hash.Sum64())
	if rate >= s.conf.HashSample.RetainMin && rate < s.conf.HashSample.RetainMax {
		return msg, nil, true
	}

	s.stats.Incr("processor.hash_sample.dropped", 1)
	return nil, types.NewSimpleResponse(nil), false
}

//------------------------------------------------------------------------------
