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

package metrics

import (
	"net/http"
	"strings"
	"sync"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

//------------------------------------------------------------------------------

func init() {
	constructors["prometheus"] = typeSpec{
		constructor: NewPrometheus,
		description: `Host endpoints for Prometheus scraping.`,
	}
}

//------------------------------------------------------------------------------

// PrometheusConfig is config for the Prometheus metrics type.
type PrometheusConfig struct {
}

// NewPrometheusConfig creates an PrometheusConfig struct with default values.
func NewPrometheusConfig() PrometheusConfig {
	return PrometheusConfig{}
}

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

// Gauge sets a gauge metric.
func (p *PromGauge) Gauge(value int64) error {
	p.ctr.Set(float64(value))
	return nil
}

//------------------------------------------------------------------------------

// PromTiming is a representation of a single metric stat. Interactions with
// this stat are thread safe.
type PromTiming struct {
	sum prometheus.Summary
}

// Timing sets a timing metric.
func (p *PromTiming) Timing(val int64) error {
	p.sum.Observe(float64(val))
	return nil
}

//------------------------------------------------------------------------------

// Prometheus is a stats object with capability to hold internal stats as a JSON
// endpoint.
type Prometheus struct {
	config Config
	prefix string

	gauges map[string]prometheus.Gauge
	timers map[string]prometheus.Summary

	sync.Mutex
}

// NewPrometheus creates and returns a new Prometheus object.
func NewPrometheus(config Config, opts ...func(Type)) (Type, error) {
	p := &Prometheus{
		config: config,
		prefix: toPromName(config.Prefix),
		gauges: map[string]prometheus.Gauge{},
		timers: map[string]prometheus.Summary{},
	}

	for _, opt := range opts {
		opt(p)
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
func (p *Prometheus) GetCounter(path ...string) StatCounter {
	dotPath := strings.Join(path, ".")
	stat := toPromName(dotPath)

	var ctr prometheus.Gauge

	p.Lock()
	var exists bool
	if ctr, exists = p.gauges[stat]; !exists {
		ctr = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: p.prefix,
			Name:      stat,
			Help:      "Benthos Gauge metric",
		})
		prometheus.MustRegister(ctr)
		p.gauges[stat] = ctr
	}
	p.Unlock()

	return &PromGauge{
		ctr: ctr,
	}
}

// GetTimer returns a stat timer object for a path.
func (p *Prometheus) GetTimer(path ...string) StatTimer {
	dotPath := strings.Join(path, ".")
	stat := toPromName(dotPath)

	var tmr prometheus.Summary

	p.Lock()
	var exists bool
	if tmr, exists = p.timers[stat]; !exists {
		tmr = prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace: p.prefix,
			Name:      stat,
			Help:      "Benthos Timing metric",
		})
		prometheus.MustRegister(tmr)
		p.timers[stat] = tmr
	}
	p.Unlock()

	return &PromTiming{
		sum: tmr,
	}
}

// GetGauge returns a stat gauge object for a path.
func (p *Prometheus) GetGauge(path ...string) StatGauge {
	dotPath := strings.Join(path, ".")
	stat := toPromName(dotPath)

	var ctr prometheus.Gauge

	p.Lock()
	var exists bool
	if ctr, exists = p.gauges[stat]; !exists {
		ctr = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: p.prefix,
			Name:      stat,
			Help:      "Benthos Gauge metric",
		})
		prometheus.MustRegister(ctr)
		p.gauges[stat] = ctr
	}
	p.Unlock()

	return &PromGauge{
		ctr: ctr,
	}
}

// Incr increments a stat by a value.
func (p *Prometheus) Incr(stat string, value int64) error {
	p.Lock()
	if ctr, exists := p.gauges[stat]; exists {
		ctr.Add(float64(value))
	} else {
		ctr = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: p.prefix,
			Name:      toPromName(stat),
			Help:      "Benthos Gauge metric",
		})
		ctr.Set(float64(value))
		prometheus.MustRegister(ctr)
		p.gauges[stat] = ctr
	}
	p.Unlock()
	return nil
}

// Decr decrements a stat by a value.
func (p *Prometheus) Decr(stat string, value int64) error {
	p.Lock()
	if ctr, exists := p.gauges[stat]; exists {
		ctr.Sub(float64(value))
	} else {
		ctr = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: p.prefix,
			Name:      toPromName(stat),
			Help:      "Benthos Gauge metric",
		})
		ctr.Set(-float64(value))
		prometheus.MustRegister(ctr)
		p.gauges[stat] = ctr
	}
	p.Unlock()
	return nil
}

// Timing sets a stat representing a duration.
func (p *Prometheus) Timing(stat string, delta int64) error {
	p.Lock()
	if tmr, exists := p.timers[stat]; exists {
		tmr.Observe(float64(delta))
	} else {
		tmr = prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace: p.prefix,
			Name:      toPromName(stat),
			Help:      "Benthos Timing metric",
		})
		tmr.Observe(float64(delta))
		prometheus.MustRegister(tmr)
		p.timers[stat] = tmr
	}
	p.Unlock()
	return nil
}

// Gauge sets a stat as a gauge value.
func (p *Prometheus) Gauge(stat string, value int64) error {
	p.Lock()
	if ctr, exists := p.gauges[stat]; exists {
		ctr.Set(float64(value))
	} else {
		ctr = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: p.prefix,
			Name:      toPromName(stat),
			Help:      "Benthos Gauge metric",
		})
		ctr.Set(float64(value))
		prometheus.MustRegister(ctr)
		p.gauges[stat] = ctr
	}
	p.Unlock()
	return nil
}

// SetLogger does nothing.
func (p *Prometheus) SetLogger(log log.Modular) {
}

// Close stops the Prometheus object from aggregating metrics and cleans up
// resources.
func (p *Prometheus) Close() error {
	return nil
}

//------------------------------------------------------------------------------
