//go:build !wasm
// +build !wasm

package metrics

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/prometheus/common/model"
)

func init() {
	Constructors[TypePrometheus] = TypeSpec{
		constructor: newPrometheus,
		Summary: `
Host endpoints (` + "`/metrics` and `/stats`" + `) for Prometheus scraping.`,
		Description: `
Metrics paths will differ from [the standard list](/docs/components/metrics/about#metric_names) in order to comply with Prometheus naming restrictions, where dots are replaced with underscores (and underscores replaced with double underscores). This change is made _before_ the mapping from ` + "`path_mapping`" + ` is applied.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldBool("use_histogram_timing", "Whether to export timing metrics as a histogram, if `false` a summary is used instead. For more information on histograms and summaries refer to: https://prometheus.io/docs/practices/histograms/.").HasDefault(false).Advanced().AtVersion("3.63.0"),
			docs.FieldFloat("histogram_buckets", "Timing metrics histogram buckets (in seconds). If left empty defaults to DefBuckets (https://pkg.go.dev/github.com/prometheus/client_golang/prometheus#pkg-variables)").Array().HasDefault([]interface{}{}).Advanced().AtVersion("3.63.0"),
			docs.FieldBool("add_process_metrics", "Whether to export process metrics such as CPU and memory usage in addition to Benthos metrics.").Advanced().HasDefault(false),
			docs.FieldBool("add_go_metrics", "Whether to export Go runtime metrics such as GC pauses in addition to Benthos metrics.").Advanced().HasDefault(false),
			docs.FieldAdvanced("push_url", "An optional [Push Gateway URL](#push-gateway) to push metrics to."),
			docs.FieldAdvanced("push_interval", "The period of time between each push when sending metrics to a Push Gateway."),
			docs.FieldAdvanced("push_job_name", "An identifier for push jobs."),
			docs.FieldAdvanced("push_basic_auth", "The Basic Authentication credentials.").WithChildren(
				docs.FieldCommon("username", "The Basic Authentication username."),
				docs.FieldCommon("password", "The Basic Authentication password."),
			),
		},
		Footnotes: `
## Push Gateway

The field ` + "`push_url`" + ` is optional and when set will trigger a push of
metrics to a [Prometheus Push Gateway](https://prometheus.io/docs/instrumenting/pushing/)
once Benthos shuts down. It is also possible to specify a
` + "`push_interval`" + ` which results in periodic pushes.

The Push Gateway is useful for when Benthos instances are short lived. Do not
include the "/metrics/jobs/..." path in the push URL.

If the Push Gateway requires HTTP Basic Authentication it can be configured with
` + "`push_basic_auth`.",
	}
}

//------------------------------------------------------------------------------

// PrometheusConfig is config for the Prometheus metrics type.
type PrometheusConfig struct {
	UseHistogramTiming bool                          `json:"use_histogram_timing" yaml:"use_histogram_timing"`
	HistogramBuckets   []float64                     `json:"histogram_buckets" yaml:"histogram_buckets"`
	AddProcessMetrics  bool                          `json:"add_process_metrics" yaml:"add_process_metrics"`
	AddGoMetrics       bool                          `json:"add_go_metrics" yaml:"add_go_metrics"`
	PushURL            string                        `json:"push_url" yaml:"push_url"`
	PushBasicAuth      PrometheusPushBasicAuthConfig `json:"push_basic_auth" yaml:"push_basic_auth"`
	PushInterval       string                        `json:"push_interval" yaml:"push_interval"`
	PushJobName        string                        `json:"push_job_name" yaml:"push_job_name"`
}

// PrometheusPushBasicAuthConfig contains parameters for establishing basic
// authentication against a push service.
type PrometheusPushBasicAuthConfig struct {
	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`
}

// NewPrometheusPushBasicAuthConfig creates a new NewPrometheusPushBasicAuthConfig with default values.
func NewPrometheusPushBasicAuthConfig() PrometheusPushBasicAuthConfig {
	return PrometheusPushBasicAuthConfig{
		Username: "",
		Password: "",
	}
}

// NewPrometheusConfig creates an PrometheusConfig struct with default values.
func NewPrometheusConfig() PrometheusConfig {
	return PrometheusConfig{
		UseHistogramTiming: false,
		HistogramBuckets:   []float64{},
		PushURL:            "",
		PushBasicAuth:      NewPrometheusPushBasicAuthConfig(),
		PushInterval:       "",
		PushJobName:        "benthos_push",
	}
}

//------------------------------------------------------------------------------

type promGauge struct {
	ctr prometheus.Gauge
}

func (p *promGauge) Incr(count int64) {
	p.ctr.Add(float64(count))
}

func (p *promGauge) Decr(count int64) {
	p.ctr.Add(float64(-count))
}

func (p *promGauge) Set(value int64) {
	p.ctr.Set(float64(value))
}

type promCounter struct {
	ctr prometheus.Counter
}

func (p *promCounter) Incr(count int64) {
	p.ctr.Add(float64(count))
}

type promTiming struct {
	sum prometheus.Observer
}

func (p *promTiming) Timing(val int64) {
	p.sum.Observe(float64(val))
}

//------------------------------------------------------------------------------

type promCounterVec struct {
	ctr *prometheus.CounterVec
}

func (p *promCounterVec) With(labelValues ...string) StatCounter {
	return &promCounter{
		ctr: p.ctr.WithLabelValues(labelValues...),
	}
}

type promTimingVec struct {
	sum *prometheus.SummaryVec
}

func (p *promTimingVec) With(labelValues ...string) StatTimer {
	return &promTiming{
		sum: p.sum.WithLabelValues(labelValues...),
	}
}

type promTimingHistVec struct {
	sum *prometheus.HistogramVec
}

func (p *promTimingHistVec) With(labelValues ...string) StatTimer {
	return &promTiming{
		sum: p.sum.WithLabelValues(labelValues...),
	}
}

type promGaugeVec struct {
	ctr *prometheus.GaugeVec
}

func (p *promGaugeVec) With(labelValues ...string) StatGauge {
	return &promGauge{
		ctr: p.ctr.WithLabelValues(labelValues...),
	}
}

//------------------------------------------------------------------------------

type prometheusMetrics struct {
	log        log.Modular
	closedChan chan struct{}
	running    int32

	useHistogramTiming bool
	histogramBuckets   []float64

	pusher *push.Pusher
	reg    *prometheus.Registry

	counters   map[string]*prometheus.CounterVec
	gauges     map[string]*prometheus.GaugeVec
	timers     map[string]*prometheus.SummaryVec
	timersHist map[string]*prometheus.HistogramVec

	mut sync.Mutex
}

func newPrometheus(config Config, log log.Modular) (Type, error) {
	promConf := config.Prometheus
	p := &prometheusMetrics{
		log:                log,
		running:            1,
		closedChan:         make(chan struct{}),
		useHistogramTiming: promConf.UseHistogramTiming,
		histogramBuckets:   promConf.HistogramBuckets,
		reg:                prometheus.NewRegistry(),
		counters:           map[string]*prometheus.CounterVec{},
		gauges:             map[string]*prometheus.GaugeVec{},
		timers:             map[string]*prometheus.SummaryVec{},
		timersHist:         map[string]*prometheus.HistogramVec{},
	}

	if len(p.histogramBuckets) == 0 {
		p.histogramBuckets = prometheus.DefBuckets
	}

	if promConf.AddProcessMetrics {
		if err := p.reg.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})); err != nil {
			return nil, err
		}
	}
	if promConf.AddGoMetrics {
		if err := p.reg.Register(collectors.NewGoCollector()); err != nil {
			return nil, err
		}
	}

	if len(promConf.PushURL) > 0 {
		p.pusher = push.New(promConf.PushURL, promConf.PushJobName).Gatherer(p.reg)

		if len(promConf.PushBasicAuth.Username) > 0 && len(promConf.PushBasicAuth.Password) > 0 {
			p.pusher = p.pusher.BasicAuth(promConf.PushBasicAuth.Username, promConf.PushBasicAuth.Password)
		}

		if len(promConf.PushInterval) > 0 {
			interval, err := time.ParseDuration(promConf.PushInterval)
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

func (p *prometheusMetrics) HandlerFunc() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		promhttp.HandlerFor(p.reg, promhttp.HandlerOpts{}).ServeHTTP(w, r)
	}
}

func (p *prometheusMetrics) GetCounter(path string) StatCounter {
	return p.GetCounterVec(path).With()
}

func (p *prometheusMetrics) GetCounterVec(path string, labelNames ...string) StatCounterVec {
	if !model.IsValidMetricName(model.LabelValue(path)) {
		p.log.Errorf("Ignoring metric '%v' due to invalid name", path)
		return &fCounterVec{
			f: func(l ...string) StatCounter {
				return &DudStat{}
			},
		}
	}

	var ctr *prometheus.CounterVec

	p.mut.Lock()
	var exists bool
	if ctr, exists = p.counters[path]; !exists {
		ctr = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: path,
			Help: "Benthos Counter metric",
		}, labelNames)
		p.reg.MustRegister(ctr)
		p.counters[path] = ctr
	}
	p.mut.Unlock()

	return &promCounterVec{
		ctr: ctr,
	}
}

func (p *prometheusMetrics) GetTimer(path string) StatTimer {
	return p.GetTimerVec(path).With()
}

func (p *prometheusMetrics) GetTimerVec(path string, labelNames ...string) StatTimerVec {
	if !model.IsValidMetricName(model.LabelValue(path)) {
		p.log.Errorf("Ignoring metric '%v' due to invalid name", path)
		return &fTimerVec{
			f: func(l ...string) StatTimer {
				return &DudStat{}
			},
		}
	}

	if p.useHistogramTiming {
		return p.getTimerHistVec(path, labelNames...)
	}

	var tmr *prometheus.SummaryVec

	p.mut.Lock()
	var exists bool
	if tmr, exists = p.timers[path]; !exists {
		tmr = prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Name:       path,
			Help:       "Benthos Timing metric",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}, labelNames)
		p.reg.MustRegister(tmr)
		p.timers[path] = tmr
	}
	p.mut.Unlock()

	return &promTimingVec{
		sum: tmr,
	}
}

func (p *prometheusMetrics) getTimerHistVec(path string, labelNames ...string) StatTimerVec {
	var tmr *prometheus.HistogramVec

	p.mut.Lock()
	var exists bool
	if tmr, exists = p.timersHist[path]; !exists {
		tmr = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    path,
			Help:    "Benthos Timing metric",
			Buckets: p.histogramBuckets,
		}, labelNames)
		p.reg.MustRegister(tmr)
		p.timersHist[path] = tmr
	}
	p.mut.Unlock()

	return &promTimingHistVec{
		sum: tmr,
	}
}

func (p *prometheusMetrics) GetGauge(path string) StatGauge {
	return p.GetGaugeVec(path).With()
}

func (p *prometheusMetrics) GetGaugeVec(path string, labelNames ...string) StatGaugeVec {
	if !model.IsValidMetricName(model.LabelValue(path)) {
		p.log.Errorf("Ignoring metric '%v' due to invalid name", path)
		return &fGaugeVec{
			f: func(l ...string) StatGauge {
				return &DudStat{}
			},
		}
	}

	var ctr *prometheus.GaugeVec

	p.mut.Lock()
	var exists bool
	if ctr, exists = p.gauges[path]; !exists {
		ctr = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: path,
			Help: "Benthos Gauge metric",
		}, labelNames)
		p.reg.MustRegister(ctr)
		p.gauges[path] = ctr
	}
	p.mut.Unlock()

	return &promGaugeVec{
		ctr: ctr,
	}
}

func (p *prometheusMetrics) Close() error {
	if atomic.CompareAndSwapInt32(&p.running, 1, 0) {
		close(p.closedChan)
	}
	if p.pusher != nil {
		return p.pusher.Push()
	}
	return nil
}
