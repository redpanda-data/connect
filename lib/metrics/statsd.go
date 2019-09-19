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
	"io/ioutil"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/quipo/statsd"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeStatsd] = TypeSpec{
		constructor: NewStatsd,
		description: `
Push metrics over a TCP or UDP connection using the
[StatsD protocol](https://github.com/statsd/statsd).`,
	}
}

//------------------------------------------------------------------------------

type wrappedLogger struct {
	m log.Modular
}

func (w *wrappedLogger) Println(v ...interface{}) {
	w.m.Warnf(fmt.Sprintln(v...))
}

//------------------------------------------------------------------------------

// StatsdConfig is config for the Statsd metrics type.
type StatsdConfig struct {
	Prefix      string `json:"prefix" yaml:"prefix"`
	Address     string `json:"address" yaml:"address"`
	FlushPeriod string `json:"flush_period" yaml:"flush_period"`
	Network     string `json:"network" yaml:"network"`
}

// NewStatsdConfig creates an StatsdConfig struct with default values.
func NewStatsdConfig() StatsdConfig {
	return StatsdConfig{
		Prefix:      "benthos",
		Address:     "localhost:4040",
		FlushPeriod: "100ms",
		Network:     "udp",
	}
}

//------------------------------------------------------------------------------

// StatsdStat is a representation of a single metric stat. Interactions with
// this stat are thread safe.
type StatsdStat struct {
	path string
	s    statsd.Statsd
}

// Incr increments a metric by an amount.
func (s *StatsdStat) Incr(count int64) error {
	s.s.Incr(s.path, count)
	return nil
}

// Decr decrements a metric by an amount.
func (s *StatsdStat) Decr(count int64) error {
	s.s.Decr(s.path, count)
	return nil
}

// Timing sets a timing metric.
func (s *StatsdStat) Timing(delta int64) error {
	s.s.Timing(s.path, delta)
	return nil
}

// Set sets a gauge metric.
func (s *StatsdStat) Set(value int64) error {
	s.s.Gauge(s.path, value)
	return nil
}

//------------------------------------------------------------------------------

// Statsd is a stats object with capability to hold internal stats as a JSON
// endpoint.
type Statsd struct {
	config Config
	s      statsd.Statsd
	log    log.Modular
}

// NewStatsd creates and returns a new Statsd object.
func NewStatsd(config Config, opts ...func(Type)) (Type, error) {
	flushPeriod, err := time.ParseDuration(config.Statsd.FlushPeriod)
	if err != nil {
		return nil, fmt.Errorf("failed to parse flush period: %s", err)
	}
	s := &Statsd{
		config: config,
		log:    log.New(ioutil.Discard, log.Config{LogLevel: "OFF"}),
	}
	for _, opt := range opts {
		opt(s)
	}

	prefix := config.Statsd.Prefix
	if len(prefix) > 0 && prefix[len(prefix)-1] != '.' {
		prefix = prefix + "."
	}

	statsdclient := statsd.NewStatsdBuffer(
		flushPeriod,
		statsd.NewStatsdClient(config.Statsd.Address, prefix),
	)
	statsdclient.Logger = &wrappedLogger{m: s.log}
	if config.Statsd.Network == "udp" {
		if err := statsdclient.CreateSocket(); err != nil {
			return nil, err
		}
	} else {
		if err := statsdclient.CreateTCPSocket(); err != nil {
			return nil, err
		}
	}
	s.s = statsdclient
	return s, nil
}

//------------------------------------------------------------------------------

// GetCounter returns a stat counter object for a path.
func (h *Statsd) GetCounter(path string) StatCounter {
	return &StatsdStat{
		path: path,
		s:    h.s,
	}
}

// GetCounterVec returns a stat counter object for a path with the labels
// discarded.
func (h *Statsd) GetCounterVec(path string, n []string) StatCounterVec {
	return fakeCounterVec(func([]string) StatCounter {
		return &StatsdStat{
			path: path,
			s:    h.s,
		}
	})
}

// GetTimer returns a stat timer object for a path.
func (h *Statsd) GetTimer(path string) StatTimer {
	return &StatsdStat{
		path: path,
		s:    h.s,
	}
}

// GetTimerVec returns a stat timer object for a path with the labels
// discarded.
func (h *Statsd) GetTimerVec(path string, n []string) StatTimerVec {
	return fakeTimerVec(func([]string) StatTimer {
		return &StatsdStat{
			path: path,
			s:    h.s,
		}
	})
}

// GetGauge returns a stat gauge object for a path.
func (h *Statsd) GetGauge(path string) StatGauge {
	return &StatsdStat{
		path: path,
		s:    h.s,
	}
}

// GetGaugeVec returns a stat timer object for a path with the labels
// discarded.
func (h *Statsd) GetGaugeVec(path string, n []string) StatGaugeVec {
	return fakeGaugeVec(func([]string) StatGauge {
		return &StatsdStat{
			path: path,
			s:    h.s,
		}
	})
}

// SetLogger sets the logger used to print connection errors.
func (h *Statsd) SetLogger(log log.Modular) {
	h.log = log
}

// Close stops the Statsd object from aggregating metrics and cleans up
// resources.
func (h *Statsd) Close() error {
	h.s.Close()
	return nil
}

//------------------------------------------------------------------------------
