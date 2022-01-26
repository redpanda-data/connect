//go:build !wasm
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
	"github.com/prometheus/client_golang/prometheus/collectors"
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

	config      PrometheusConfig
	pathMapping *pathMapping
	prefix      string

	pusher *push.Pusher
	reg    *prometheus.Registry

	counters map[string]*prometheus.CounterVec
	gauges   map[string]*prometheus.GaugeVec
	timers   map[string]*prometheus.SummaryVec

	mut sync.Mutex
}

// NewPrometheus creates and returns a new Prometheus object.
func NewPrometheus(config Config, opts ...func(Type)) (Type, error) {
	p := &Prometheus{
		log:        log.Noop(),
		running:    1,
		closedChan: make(chan struct{}),
		config:     config.Prometheus,
		prefix:     config.Prometheus.Prefix,
		reg:        prometheus.NewRegistry(),
		counters:   map[string]*prometheus.CounterVec{},
		gauges:     map[string]*prometheus.GaugeVec{},
		timers:     map[string]*prometheus.SummaryVec{},
	}

	for _, opt := range opts {
		opt(p)
	}

	// TODO: V4 Maybe disable this with a config flag.
	if err := p.reg.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})); err != nil {
		return nil, err
	}
	if err := p.reg.Register(collectors.NewGoCollector()); err != nil {
		return nil, err
	}

	var err error
	if p.pathMapping, err = newPathMapping(p.config.PathMapping, p.log); err != nil {
		return nil, fmt.Errorf("failed to init path mapping: %v", err)
	}

	if len(p.config.PushURL) > 0 {
		p.pusher = push.New(p.config.PushURL, p.config.PushJobName).Gatherer(p.reg)

		if len(p.config.PushBasicAuth.Username) > 0 && len(p.config.PushBasicAuth.Password) > 0 {
			p.pusher = p.pusher.BasicAuth(p.config.PushBasicAuth.Username, p.config.PushBasicAuth.Password)
		}

		if len(p.config.PushInterval) > 0 {
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
						if err = p.pusher.Push(); err != nil {
							p.log.Errorf("Failed to push metrics: %v\n", err)
						}
					}
				}
			}()
		}
	}

	return p, nil
}

//------------------------------------------------------------------------------

// HandlerFunc returns an http.HandlerFunc for scraping metrics.
func (p *Prometheus) HandlerFunc() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		promhttp.HandlerFor(p.reg, promhttp.HandlerOpts{}).ServeHTTP(w, r)
	}
}

//------------------------------------------------------------------------------

func (p *Prometheus) toPromName(dotSepName string) (outPath string, labelNames, labelValues []string) {
	dotSepName = strings.ReplaceAll(dotSepName, "_", "__")
	dotSepName = strings.ReplaceAll(dotSepName, "-", "__")
	return p.pathMapping.mapPathWithTags(strings.ReplaceAll(dotSepName, ".", "_"))
}

// GetCounter returns a stat counter object for a path.
func (p *Prometheus) GetCounter(path string) StatCounter {
	stat, labels, values := p.toPromName(path)
	if stat == "" {
		return DudStat{}
	}

	var ctr *prometheus.CounterVec

	p.mut.Lock()
	var exists bool
	if ctr, exists = p.counters[stat]; !exists {
		ctr = prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: p.prefix,
			Name:      stat,
			Help:      "Benthos Counter metric",
		}, labels)
		p.reg.MustRegister(ctr)
		p.counters[stat] = ctr
	}
	p.mut.Unlock()

	return &PromCounter{
		ctr: ctr.WithLabelValues(values...),
	}
}

// GetTimer returns a stat timer object for a path.
func (p *Prometheus) GetTimer(path string) StatTimer {
	stat, labels, values := p.toPromName(path)
	if stat == "" {
		return DudStat{}
	}

	var tmr *prometheus.SummaryVec

	p.mut.Lock()
	var exists bool
	if tmr, exists = p.timers[stat]; !exists {
		tmr = prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Namespace:  p.prefix,
			Name:       stat,
			Help:       "Benthos Timing metric",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}, labels)
		p.reg.MustRegister(tmr)
		p.timers[stat] = tmr
	}
	p.mut.Unlock()

	return &PromTiming{
		sum: tmr.WithLabelValues(values...),
	}
}

// GetGauge returns a stat gauge object for a path.
func (p *Prometheus) GetGauge(path string) StatGauge {
	stat, labels, values := p.toPromName(path)
	if stat == "" {
		return DudStat{}
	}

	var ctr *prometheus.GaugeVec

	p.mut.Lock()
	var exists bool
	if ctr, exists = p.gauges[stat]; !exists {
		ctr = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: p.prefix,
			Name:      stat,
			Help:      "Benthos Gauge metric",
		}, labels)
		p.reg.MustRegister(ctr)
		p.gauges[stat] = ctr
	}
	p.mut.Unlock()

	return &PromGauge{
		ctr: ctr.WithLabelValues(values...),
	}
}

// GetCounterVec returns an editable counter stat for a given path with labels,
// these labels must be consistent with any other metrics registered on the same
// path.
func (p *Prometheus) GetCounterVec(path string, labelNames []string) StatCounterVec {
	stat, labels, values := p.toPromName(path)
	if stat == "" {
		return fakeCounterVec(func([]string) StatCounter {
			return DudStat{}
		})
	}
	if len(labels) > 0 {
		labelNames = append(labels, labelNames...)
	}

	var ctr *prometheus.CounterVec

	p.mut.Lock()
	var exists bool
	if ctr, exists = p.counters[stat]; !exists {
		ctr = prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: p.prefix,
			Name:      stat,
			Help:      "Benthos Counter metric",
		}, labelNames)
		p.reg.MustRegister(ctr)
		p.counters[stat] = ctr
	}
	p.mut.Unlock()

	if len(labels) > 0 {
		return fakeCounterVec(func(vs []string) StatCounter {
			fvs := append([]string{}, values...)
			fvs = append(fvs, vs...)
			return (&PromCounterVec{
				ctr: ctr,
			}).With(fvs...)
		})
	}
	return &PromCounterVec{
		ctr: ctr,
	}
}

// GetTimerVec returns an editable timer stat for a given path with labels,
// these labels must be consistent with any other metrics registered on the same
// path.
func (p *Prometheus) GetTimerVec(path string, labelNames []string) StatTimerVec {
	stat, labels, values := p.toPromName(path)
	if stat == "" {
		return fakeTimerVec(func([]string) StatTimer {
			return DudStat{}
		})
	}
	if len(labels) > 0 {
		labelNames = append(labels, labelNames...)
	}

	var tmr *prometheus.SummaryVec

	p.mut.Lock()
	var exists bool
	if tmr, exists = p.timers[stat]; !exists {
		tmr = prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Namespace:  p.prefix,
			Name:       stat,
			Help:       "Benthos Timing metric",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}, labelNames)
		p.reg.MustRegister(tmr)
		p.timers[stat] = tmr
	}
	p.mut.Unlock()

	if len(labels) > 0 {
		return fakeTimerVec(func(vs []string) StatTimer {
			fvs := append([]string{}, values...)
			fvs = append(fvs, vs...)
			return (&PromTimingVec{
				sum: tmr,
			}).With(fvs...)
		})
	}
	return &PromTimingVec{
		sum: tmr,
	}
}

// GetGaugeVec returns an editable gauge stat for a given path with labels,
// these labels must be consistent with any other metrics registered on the same
// path.
func (p *Prometheus) GetGaugeVec(path string, labelNames []string) StatGaugeVec {
	stat, labels, values := p.toPromName(path)
	if stat == "" {
		return fakeGaugeVec(func([]string) StatGauge {
			return DudStat{}
		})
	}
	if len(labels) > 0 {
		labelNames = append(labels, labelNames...)
	}

	var ctr *prometheus.GaugeVec

	p.mut.Lock()
	var exists bool
	if ctr, exists = p.gauges[stat]; !exists {
		ctr = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: p.prefix,
			Name:      stat,
			Help:      "Benthos Gauge metric",
		}, labelNames)
		p.reg.MustRegister(ctr)
		p.gauges[stat] = ctr
	}
	p.mut.Unlock()

	if len(labels) > 0 {
		return fakeGaugeVec(func(vs []string) StatGauge {
			fvs := append([]string{}, values...)
			fvs = append(fvs, vs...)
			return (&PromGaugeVec{
				ctr: ctr,
			}).With(fvs...)
		})
	}
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
	if p.pusher != nil {
		return p.pusher.Push()
	}
	return nil
}

//------------------------------------------------------------------------------
