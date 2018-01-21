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

// Prometheus is a stats object with capability to hold internal stats as a JSON
// endpoint.
type Prometheus struct {
	config Config

	gauges map[string]prometheus.Gauge
	timers map[string]prometheus.Summary
}

// NewPrometheus creates and returns a new Prometheus object.
func NewPrometheus(config Config) (Type, error) {
	return &Prometheus{
		config: config,
		gauges: map[string]prometheus.Gauge{},
		timers: map[string]prometheus.Summary{},
	}, nil
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

// Incr increments a stat by a value.
func (p *Prometheus) Incr(stat string, value int64) error {
	if ctr, exists := p.gauges[stat]; exists {
		ctr.Add(float64(value))
	} else {
		ctr = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: toPromName(p.config.Prefix),
			Name:      toPromName(stat),
			Help:      "Benthos Gauge metric",
		})
		ctr.Set(float64(value))
		prometheus.MustRegister(ctr)
		p.gauges[stat] = ctr
	}
	return nil
}

// Decr decrements a stat by a value.
func (p *Prometheus) Decr(stat string, value int64) error {
	if ctr, exists := p.gauges[stat]; exists {
		ctr.Sub(float64(value))
	} else {
		ctr = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: toPromName(p.config.Prefix),
			Name:      toPromName(stat),
			Help:      "Benthos Gauge metric",
		})
		ctr.Set(-float64(value))
		prometheus.MustRegister(ctr)
		p.gauges[stat] = ctr
	}
	return nil
}

// Timing sets a stat representing a duration.
func (p *Prometheus) Timing(stat string, delta int64) error {
	if tmr, exists := p.timers[stat]; exists {
		tmr.Observe(float64(delta))
	} else {
		tmr = prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace: toPromName(p.config.Prefix),
			Name:      toPromName(stat),
			Help:      "Benthos Timing metric",
		})
		tmr.Observe(float64(delta))
		prometheus.MustRegister(tmr)
		p.timers[stat] = tmr
	}
	return nil
}

// Gauge sets a stat as a gauge value.
func (p *Prometheus) Gauge(stat string, value int64) error {
	if ctr, exists := p.gauges[stat]; exists {
		ctr.Set(float64(value))
	} else {
		ctr = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: toPromName(p.config.Prefix),
			Name:      toPromName(stat),
			Help:      "Benthos Gauge metric",
		})
		ctr.Set(float64(value))
		prometheus.MustRegister(ctr)
		p.gauges[stat] = ctr
	}
	return nil
}

// Close stops the Prometheus object from aggregating metrics and cleans up
// resources.
func (p *Prometheus) Close() error {
	return nil
}

//------------------------------------------------------------------------------
