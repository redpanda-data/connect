// Copyright (c) 2014 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, sub to the following conditions:
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

package metrics

import (
	"fmt"
	"time"

	"gopkg.in/alexcesaro/statsd.v2"
)

//------------------------------------------------------------------------------

func init() {
	constructors["statsd"] = typeSpec{
		constructor: NewStatsd,
		description: `Use the statsd protocol.`,
	}
}

//------------------------------------------------------------------------------

// StatsdConfig is config for the Statsd metrics type.
type StatsdConfig struct {
	Address       string `json:"address" yaml:"address"`
	FlushPeriod   string `json:"flush_period" yaml:"flush_period"`
	MaxPacketSize int    `json:"max_packet_size" yaml:"max_packet_size"`
	Network       string `json:"network" yaml:"network"`
	Prefix        string `json:"prefix" yaml:"prefix"`
}

// NewStatsdConfig creates an StatsdConfig struct with default values.
func NewStatsdConfig() StatsdConfig {
	return StatsdConfig{
		Address:       "localhost:4040",
		FlushPeriod:   "100ms",
		MaxPacketSize: 1440,
		Network:       "udp",
		Prefix:        "",
	}
}

//------------------------------------------------------------------------------

// Statsd is a stats object with capability to hold internal stats as a JSON
// endpoint.
type Statsd struct {
	config Config
	s      *statsd.Client
}

// NewStatsd creates and returns a new Statsd object.
func NewStatsd(config Config) (Type, error) {
	flushPeriod, err := time.ParseDuration(config.Statsd.FlushPeriod)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse flush period: %s", err)
	}
	c, err := statsd.New(
		statsd.Address(config.Statsd.Address),
		statsd.FlushPeriod(flushPeriod),
		statsd.MaxPacketSize(config.Statsd.MaxPacketSize),
		statsd.Network(config.Statsd.Network),
		statsd.Prefix(config.Statsd.Prefix),
	)
	if err != nil {
		return nil, err
	}
	return &Statsd{
		config: config,
		s:      c,
	}, nil
}

//------------------------------------------------------------------------------

// Incr increments a stat by a value.
func (h *Statsd) Incr(stat string, value int64) error {
	h.s.Count(stat, value)
	return nil
}

// Decr decrements a stat by a value.
func (h *Statsd) Decr(stat string, value int64) error {
	h.s.Count(stat, -value)
	return nil
}

// Timing sets a stat representing a duration.
func (h *Statsd) Timing(stat string, delta int64) error {
	h.s.Timing(stat, delta)
	return nil
}

// Gauge sets a stat as a gauge value.
func (h *Statsd) Gauge(stat string, value int64) error {
	h.s.Gauge(stat, value)
	return nil
}

// Close stops the Statsd object from aggregating metrics and cleans up
// resources.
func (h *Statsd) Close() error {
	h.s.Close()
	return nil
}

//------------------------------------------------------------------------------
