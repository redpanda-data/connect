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
	"github.com/smira/go-statsd"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeDatadog] = TypeSpec{
		constructor: NewDatadog,
		description: `
Push metrics over UDP connection using the
[Datadog StatsD protocol](github.com/smira/go-statsd) with datadog\influxdb tags.`,
	}
}

//------------------------------------------------------------------------------

type wrappedDatadogLogger struct {
	log log.Modular
}

func (s wrappedDatadogLogger) Printf(msg string, args ...interface{}) {
	s.log.Warnf(fmt.Sprintf(msg, args...))
}

//------------------------------------------------------------------------------

// DatadogConfig is config for the Datadog metrics type.
type DatadogConfig struct {
	Prefix      string `json:"prefix" yaml:"prefix"`
	Address     string `json:"address" yaml:"address"`
	FlushPeriod string `json:"flush_period" yaml:"flush_period"`
	TagFormat   string `json:"tag_format" yaml:"tag_format"`
}

// NewDatadogConfig creates an DatadogConfig struct with default values.
func NewDatadogConfig() DatadogConfig {
	return DatadogConfig{
		Prefix:      "benthos",
		Address:     "localhost:4040",
		FlushPeriod: "100ms",
		TagFormat:   "none",
	}
}

const (
	TagFormatDatadog  = "datadog"
	TagFormatInfluxDB = "influxdb"
)

//------------------------------------------------------------------------------

// DatadogStat is a representation of a single metric stat. Interactions with
// this stat are thread safe.
type DatadogStat struct {
	path string
	s    *statsd.Client
	tags []statsd.Tag
}

// Incr increments a metric by an amount.
func (s *DatadogStat) Incr(count int64) error {
	s.s.Incr(s.path, count, s.tags...)
	return nil
}

// Decr decrements a metric by an amount.
func (s *DatadogStat) Decr(count int64) error {
	s.s.Decr(s.path, count, s.tags...)
	return nil
}

// Timing sets a timing metric.
func (s *DatadogStat) Timing(delta int64) error {
	s.s.Timing(s.path, delta, s.tags...)
	return nil
}

// Set sets a gauge metric.
func (s *DatadogStat) Set(value int64) error {
	s.s.Gauge(s.path, value, s.tags...)
	return nil
}

//------------------------------------------------------------------------------

// Datadog is a stats object with capability to hold internal stats as a JSON
// endpoint.
type Datadog struct {
	config Config
	s      *statsd.Client
	log    log.Modular
}

// NewDatadog creates and returns a new Datadog object.
func NewDatadog(config Config, opts ...func(Type)) (Type, error) {
	flushPeriod, err := time.ParseDuration(config.Statsd.FlushPeriod)
	if err != nil {
		return nil, fmt.Errorf("failed to parse flush period: %s", err)
	}

	s := &Datadog{
		config: config,
		log:    log.New(ioutil.Discard, log.Config{LogLevel: "OFF"}),
	}
	for _, opt := range opts {
		opt(s)
	}

	prefix := config.Datadog.Prefix
	if len(prefix) > 0 && prefix[len(prefix)-1] != '.' {
		prefix = prefix + "."
	}

	var tagFormat *statsd.TagFormat

	if TagFormatInfluxDB == config.Datadog.Prefix {
		tagFormat = statsd.TagFormatInfluxDB
	} else if TagFormatDatadog == config.Datadog.Prefix {
		tagFormat = statsd.TagFormatDatadog
	} else {
		return nil, fmt.Errorf("tag format %s does not supported", config.Datadog.Prefix)
	}

	client := statsd.NewClient(config.Statsd.Address,
		statsd.FlushInterval(flushPeriod),
		statsd.MetricPrefix(prefix),
		statsd.TagStyle(tagFormat),
		statsd.Logger(wrappedDatadogLogger{log: s.log}))

	s.s = client
	return s, nil
}

//------------------------------------------------------------------------------

// GetCounter returns a stat counter object for a path.
func (h *Datadog) GetCounter(path string) StatCounter {
	return &DatadogStat{
		path: path,
		s:    h.s,
	}
}

// GetCounterVec returns a stat counter object for a path with the labels
func (h *Datadog) GetCounterVec(path string, n []string) StatCounterVec {
	return &fCounterVec{
		f: func(l []string) StatCounter {
			return &DatadogStat{
				path: path,
				s:    h.s,
				tags: tags(n, l),
			}
		},
	}
}

// GetTimer returns a stat timer object for a path.
func (h *Datadog) GetTimer(path string) StatTimer {
	return &DatadogStat{
		path: path,
		s:    h.s,
	}
}

// GetTimerVec returns a stat timer object for a path with the labels
func (h *Datadog) GetTimerVec(path string, n []string) StatTimerVec {
	return &fTimerVec{
		f: func(l []string) StatTimer {
			return &DatadogStat{
				path: path,
				s:    h.s,
				tags: tags(n, l),
			}
		},
	}
}

// GetGauge returns a stat gauge object for a path.
func (h *Datadog) GetGauge(path string) StatGauge {
	return &DatadogStat{
		path: path,
		s:    h.s,
	}
}

// GetGaugeVec returns a stat timer object for a path with the labels
func (h *Datadog) GetGaugeVec(path string, n []string) StatGaugeVec {
	return &fGaugeVec{
		f: func(l []string) StatGauge {
			return &DatadogStat{
				path: path,
				s:    h.s,
				tags: tags(n, l),
			}
		},
	}
}

// SetLogger sets the logger used to print connection errors.
func (h *Datadog) SetLogger(log log.Modular) {
	h.log = log
}

// Close stops the Datadog object from aggregating metrics and cleans up
// resources.
func (h *Datadog) Close() error {
	h.s.Close()
	return nil
}

// tags merges tag labels with their interpolated values
//
// no attempt is made to merge labels and values if slices
// are not the same length
func tags(labels []string, values []string) []statsd.Tag {
	if len(labels) != len(values) {
		return nil
	}
	tags := make([]statsd.Tag, len(labels))
	for i := range labels {
		tags[i] = statsd.StringTag(labels[i], values[i])
	}
	return tags
}

//------------------------------------------------------------------------------
