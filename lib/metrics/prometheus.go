// Copyright (c) 2018 Ashley Jeffs
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

// +build !wasm

package metrics

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/push"
)

//------------------------------------------------------------------------------

// PromGauge is a representation of a single metric stat. Interactions with this
// stat are thread safe.
type PromGauge struct {
	ctr prometheus.Gauge
}

// Incr increments a metric by an amount.
func (p *PromGauge) Incr(count int64) error {
	p.ctr.Add(float64(count))
	return nil
}

// Decr decrements a metric by an amount.
func (p *PromGauge) Decr(count int64) error {
	p.ctr.Add(float64(-count))
	return nil
}

// Set sets a gauge metric.
func (p *PromGauge) Set(value int64) error {
	p.ctr.Set(float64(value))
	return nil
}

// PromCounter is a representation of a single metric stat. Interactions with
// this stat are thread safe.
type PromCounter struct {
	ctr prometheus.Counter
}

// Incr increments a metric by an amount.
func (p *PromCounter) Incr(count int64) error {
	p.ctr.Add(float64(count))
	return nil
}

// PromTiming is a representation of a single metric stat. Interactions with
// this stat are thread safe.
type PromTiming struct {
	sum prometheus.Observer
}

// Timing sets a timing metric.
func (p *PromTiming) Timing(val int64) error {
	p.sum.Observe(float64(val))
	return nil
}

//------------------------------------------------------------------------------

// PromCounterVec creates StatCounters with dynamic labels.
type PromCounterVec struct {
	ctr *prometheus.CounterVec
}

// With returns a StatCounter with a set of label values.
func (p *PromCounterVec) With(labelValues ...string) StatCounter {
	return &PromCounter{
		ctr: p.ctr.WithLabelValues(labelValues...),
	}
}

// PromTimingVec creates StatTimers with dynamic labels.
type PromTimingVec struct {
	sum *prometheus.SummaryVec
}

// With returns a StatTimer with a set of label values.
func (p *PromTimingVec) With(labelValues ...string) StatTimer {
	return &PromTiming{
		sum: p.sum.WithLabelValues(labelValues...),
	}
}

// PromGaugeVec creates StatGauges with dynamic labels.
type PromGaugeVec struct {
	ctr *prometheus.GaugeVec
}

// With returns a StatGauge with a set of label values.
func (p *PromGaugeVec) With(labelValues ...string) StatGauge {
	return &PromGauge{
		ctr: p.ctr.WithLabelValues(labelValues...),
	}
}

//------------------------------------------------------------------------------

// Prometheus is a stats object with capability to hold internal stats as a JSON
// endpoint.
type Prometheus struct {
	log        log.Modular
	closedChan chan struct{}
	running    int32

	config PrometheusConfig
	prefix string

	counters map[string]*prometheus.CounterVec
	gauges   map[string]*prometheus.GaugeVec
	timers   map[string]*prometheus.SummaryVec

	sync.Mutex
}

// NewPrometheus creates and returns a new Prometheus object.
func NewPrometheus(config Config, opts ...func(Type)) (Type, error) {
	p := &Prometheus{
		log:        log.Noop(),
		running:    1,
		closedChan: make(chan struct{}),
		config:     config.Prometheus,
		prefix:     config.Prometheus.Prefix,
		counters:   map[string]*prometheus.CounterVec{},
		gauges:     map[string]*prometheus.GaugeVec{},
		timers:     map[string]*prometheus.SummaryVec{},
	}

	for _, opt := range opts {
		opt(p)
	}

	if len(p.config.PushURL) > 0 && len(p.config.PushInterval) > 0 {
		interval, err := time.ParseDuration(p.config.PushInterval)
		if err != nil {
			return nil, fmt.Errorf("failed to parse push interval: %v", err)
		}
		go func() {
			for {
				select {
				case <-p.closedChan:
					return
				case <-time.After(interval):
					if err = push.New(p.config.PushURL, p.config.PushJobName).Gatherer(prometheus.DefaultGatherer).Push(); err != nil {
						p.log.Errorf("Failed to push metrics: %v\n", err)
					}
				}
			}
		}()
	}

	return p, nil
}

//------------------------------------------------------------------------------

// HandlerFunc returns an http.HandlerFunc for scraping metrics.
func (p *Prometheus) HandlerFunc() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		promhttp.Handler().ServeHTTP(w, r)
	}
}

//------------------------------------------------------------------------------

func toPromName(dotSepName string) string {
	dotSepName = strings.Replace(dotSepName, "_", "__", -1)
	dotSepName = strings.Replace(dotSepName, "-", "__", -1)
	return strings.Replace(dotSepName, ".", "_", -1)
}

// GetCounter returns a stat counter object for a path.
func (p *Prometheus) GetCounter(path string) StatCounter {
	stat := toPromName(path)

	var ctr *prometheus.CounterVec

	p.Lock()
	var exists bool
	if ctr, exists = p.counters[stat]; !exists {
		ctr = prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: p.prefix,
			Name:      stat,
			Help:      "Benthos Counter metric",
		}, nil)
		prometheus.MustRegister(ctr)
		p.counters[stat] = ctr
	}
	p.Unlock()

	return &PromCounter{
		ctr: ctr.WithLabelValues(),
	}
}

// GetTimer returns a stat timer object for a path.
func (p *Prometheus) GetTimer(path string) StatTimer {
	stat := toPromName(path)

	var tmr *prometheus.SummaryVec

	p.Lock()
	var exists bool
	if tmr, exists = p.timers[stat]; !exists {
		tmr = prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Namespace:  p.prefix,
			Name:       stat,
			Help:       "Benthos Timing metric",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}, nil)
		prometheus.MustRegister(tmr)
		p.timers[stat] = tmr
	}
	p.Unlock()

	return &PromTiming{
		sum: tmr.WithLabelValues(),
	}
}

// GetGauge returns a stat gauge object for a path.
func (p *Prometheus) GetGauge(path string) StatGauge {
	stat := toPromName(path)

	var ctr *prometheus.GaugeVec

	p.Lock()
	var exists bool
	if ctr, exists = p.gauges[stat]; !exists {
		ctr = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: p.prefix,
			Name:      stat,
			Help:      "Benthos Gauge metric",
		}, nil)
		prometheus.MustRegister(ctr)
		p.gauges[stat] = ctr
	}
	p.Unlock()

	return &PromGauge{
		ctr: ctr.WithLabelValues(),
	}
}

// GetCounterVec returns an editable counter stat for a given path with labels,
// these labels must be consistent with any other metrics registered on the same
// path.
func (p *Prometheus) GetCounterVec(path string, labelNames []string) StatCounterVec {
	stat := toPromName(path)

	var ctr *prometheus.CounterVec

	p.Lock()
	var exists bool
	if ctr, exists = p.counters[stat]; !exists {
		ctr = prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: p.prefix,
			Name:      stat,
			Help:      "Benthos Counter metric",
		}, labelNames)
		prometheus.MustRegister(ctr)
		p.counters[stat] = ctr
	}
	p.Unlock()

	return &PromCounterVec{
		ctr: ctr,
	}
}

// GetTimerVec returns an editable timer stat for a given path with labels,
// these labels must be consistent with any other metrics registered on the same
// path.
func (p *Prometheus) GetTimerVec(path string, labelNames []string) StatTimerVec {
	stat := toPromName(path)

	var tmr *prometheus.SummaryVec

	p.Lock()
	var exists bool
	if tmr, exists = p.timers[stat]; !exists {
		tmr = prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Namespace:  p.prefix,
			Name:       stat,
			Help:       "Benthos Timing metric",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}, labelNames)
		prometheus.MustRegister(tmr)
		p.timers[stat] = tmr
	}
	p.Unlock()

	return &PromTimingVec{
		sum: tmr,
	}
}

// GetGaugeVec returns an editable gauge stat for a given path with labels,
// these labels must be consistent with any other metrics registered on the same
// path.
func (p *Prometheus) GetGaugeVec(path string, labelNames []string) StatGaugeVec {
	stat := toPromName(path)

	var ctr *prometheus.GaugeVec

	p.Lock()
	var exists bool
	if ctr, exists = p.gauges[stat]; !exists {
		ctr = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: p.prefix,
			Name:      stat,
			Help:      "Benthos Gauge metric",
		}, labelNames)
		prometheus.MustRegister(ctr)
		p.gauges[stat] = ctr
	}
	p.Unlock()

	return &PromGaugeVec{
		ctr: ctr,
	}
}

// SetLogger does nothing.
func (p *Prometheus) SetLogger(log log.Modular) {
	p.log = log
}

// Close stops the Prometheus object from aggregating metrics and cleans up
// resources.
func (p *Prometheus) Close() error {
	if atomic.CompareAndSwapInt32(&p.running, 1, 0) {
		close(p.closedChan)
	}
	if len(p.config.PushURL) > 0 {
		return push.New(p.config.PushURL, p.config.PushJobName).Gatherer(prometheus.DefaultGatherer).Push()
	}
	return nil
}

//------------------------------------------------------------------------------
