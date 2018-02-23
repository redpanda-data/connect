// Copyright (c) 2017 Ashley Jeffs
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
	"math/rand"

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

//------------------------------------------------------------------------------

func init() {
	constructors["sample"] = typeSpec{
		constructor: NewSample,
		description: `
Passes on a percentage of messages, either randomly or sequentially, and drops
all others.`,
	}
}

//------------------------------------------------------------------------------

// SampleConfig contains any configuration for the Sample processor.
type SampleConfig struct {
	Retain     float64 `json:"retain" yaml:"retain"`
	RandomSeed int64   `json:"seed" yaml:"seed"`
}

// NewSampleConfig returns a SampleConfig with default values.
func NewSampleConfig() SampleConfig {
	return SampleConfig{
		Retain:     0.1, // 10%
		RandomSeed: 0,
	}
}

//------------------------------------------------------------------------------

// Sample is a processor that checks each message against a set of bounds
// and rejects messages if they aren't within them.
type Sample struct {
	conf  Config
	log   log.Modular
	stats metrics.Type

	gen *rand.Rand
}

// NewSample returns a Sample processor.
func NewSample(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	gen := rand.New(rand.NewSource(conf.Sample.RandomSeed))
	return &Sample{
		conf:  conf,
		log:   log.NewModule(".processor.sample"),
		stats: stats,
		gen:   gen,
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage checks each message against a set of bounds.
func (s *Sample) ProcessMessage(msg *types.Message) ([]*types.Message, types.Response) {
	s.stats.Incr("processor.sample.count", 1)
	if s.gen.Float64() > s.conf.Sample.Retain {
		s.stats.Incr("processor.sample.dropped", 1)
		return nil, types.NewSimpleResponse(nil)
	}
	msgs := [1]*types.Message{msg}
	return msgs[:], nil
}

//------------------------------------------------------------------------------
