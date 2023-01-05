package prometheus

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/prometheus/common/model"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
)

func init() {
	_ = bundle.AllMetrics.Add(newPrometheus, docs.ComponentSpec{
		Name:   "prometheus",
		Type:   docs.TypeMetrics,
		Status: docs.StatusStable,
		Summary: `
Host endpoints (` + "`/metrics` and `/stats`" + `) for Prometheus scraping.`,
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
		Config: docs.FieldComponent().WithChildren(
			docs.FieldBool("use_histogram_timing", "Whether to export timing metrics as a histogram, if `false` a summary is used instead. When exporting histogram timings the delta values are converted from nanoseconds into seconds in order to better fit within bucket definitions. For more information on histograms and summaries refer to: https://prometheus.io/docs/practices/histograms/.").HasDefault(false).Advanced().AtVersion("3.63.0"),
			docs.FieldFloat("histogram_buckets", "Timing metrics histogram buckets (in seconds). If left empty defaults to DefBuckets (https://pkg.go.dev/github.com/prometheus/client_golang/prometheus#pkg-variables)").Array().HasDefault([]any{}).Advanced().AtVersion("3.63.0"),
			docs.FieldBool("add_process_metrics", "Whether to export process metrics such as CPU and memory usage in addition to Benthos metrics.").Advanced().HasDefault(false),
			docs.FieldBool("add_go_metrics", "Whether to export Go runtime metrics such as GC pauses in addition to Benthos metrics.").Advanced().HasDefault(false),
			docs.FieldURL("push_url", "An optional [Push Gateway URL](#push-gateway) to push metrics to.").Advanced().HasDefault(""),
			docs.FieldString("push_interval", "The period of time between each push when sending metrics to a Push Gateway.").Advanced().HasDefault(""),
			docs.FieldString("push_job_name", "An identifier for push jobs.").Advanced().HasDefault("benthos_push"),
			docs.FieldObject("push_basic_auth", "The Basic Authentication credentials.").WithChildren(
				docs.FieldString("username", "The Basic Authentication username.").HasDefault(""),
				docs.FieldString("password", "The Basic Authentication password.").HasDefault("").Secret(),
			).Advanced(),
			docs.FieldString("file_output_path", "An optional file path to write all prometheus metrics on service shutdown.").Advanced().HasDefault(""),
		),
	})
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
	sum       prometheus.Observer
	asSeconds bool
}

func (p *promTiming) Timing(val int64) {
	vFloat := float64(val)
	if p.asSeconds {
		vFloat /= 1_000_000_000
	}
	p.sum.Observe(vFloat)
}

//------------------------------------------------------------------------------

type promCounterVec struct {
	ctr   *prometheus.CounterVec
	count int
}

func (p *promCounterVec) With(labelValues ...string) metrics.StatCounter {
	return &promCounter{
		ctr: p.ctr.WithLabelValues(labelValues...),
	}
}

type promTimingVec struct {
	sum   *prometheus.SummaryVec
	count int
}

func (p *promTimingVec) With(labelValues ...string) metrics.StatTimer {
	return &promTiming{
		sum: p.sum.WithLabelValues(labelValues...),
	}
}

type promTimingHistVec struct {
	sum   *prometheus.HistogramVec
	count int
}

func (p *promTimingHistVec) With(labelValues ...string) metrics.StatTimer {
	return &promTiming{
		asSeconds: true,
		sum:       p.sum.WithLabelValues(labelValues...),
	}
}

type promGaugeVec struct {
	ctr   *prometheus.GaugeVec
	count int
}

func (p *promGaugeVec) With(labelValues ...string) metrics.StatGauge {
	return &promGauge{
		ctr: p.ctr.WithLabelValues(labelValues...),
	}
}

//------------------------------------------------------------------------------

type prometheusMetrics struct {
	log        log.Modular
	closedChan chan struct{}
	running    int32

	fileOutputPath string

	useHistogramTiming bool
	histogramBuckets   []float64

	pusher *push.Pusher
	reg    *prometheus.Registry

	counters   map[string]*promCounterVec
	gauges     map[string]*promGaugeVec
	timers     map[string]*promTimingVec
	timersHist map[string]*promTimingHistVec

	mut sync.Mutex
}

func newPrometheus(config metrics.Config, nm bundle.NewManagement) (metrics.Type, error) {
	promConf := config.Prometheus
	p := &prometheusMetrics{
		log:                nm.Logger(),
		running:            1,
		closedChan:         make(chan struct{}),
		useHistogramTiming: promConf.UseHistogramTiming,
		histogramBuckets:   promConf.HistogramBuckets,
		reg:                prometheus.NewRegistry(),
		counters:           map[string]*promCounterVec{},
		gauges:             map[string]*promGaugeVec{},
		timers:             map[string]*promTimingVec{},
		timersHist:         map[string]*promTimingHistVec{},
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

	if len(promConf.FileOutputPath) > 0 {
		p.fileOutputPath = promConf.FileOutputPath
	}

	return p, nil
}

//------------------------------------------------------------------------------

func (p *prometheusMetrics) HandlerFunc() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		promhttp.HandlerFor(p.reg, promhttp.HandlerOpts{}).ServeHTTP(w, r)
	}
}

func (p *prometheusMetrics) GetCounter(path string) metrics.StatCounter {
	return p.GetCounterVec(path).With()
}

func (p *prometheusMetrics) GetCounterVec(path string, labelNames ...string) metrics.StatCounterVec {
	if !model.IsValidMetricName(model.LabelValue(path)) {
		p.log.Errorf("Ignoring metric '%v' due to invalid name", path)
		return metrics.FakeCounterVec(func(l ...string) metrics.StatCounter {
			return metrics.DudStat{}
		})
	}

	var pv *promCounterVec

	p.mut.Lock()
	var exists bool
	if pv, exists = p.counters[path]; !exists {
		ctr := prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: path,
			Help: "Benthos Counter metric",
		}, labelNames)
		p.reg.MustRegister(ctr)

		pv = &promCounterVec{
			ctr:   ctr,
			count: len(labelNames),
		}
		p.counters[path] = pv
	}
	p.mut.Unlock()

	if pv.count != len(labelNames) {
		p.log.Errorf("Metrics label mismatch %v versus %v %v for name '%v', skipping metric", pv.count, len(labelNames), labelNames, path)
		return metrics.Noop().GetCounterVec(path, labelNames...)
	}
	return pv
}

func (p *prometheusMetrics) GetTimer(path string) metrics.StatTimer {
	return p.GetTimerVec(path).With()
}

func (p *prometheusMetrics) GetTimerVec(path string, labelNames ...string) metrics.StatTimerVec {
	if !model.IsValidMetricName(model.LabelValue(path)) {
		p.log.Errorf("Ignoring metric '%v' due to invalid name", path)
		return metrics.FakeTimerVec(func(l ...string) metrics.StatTimer {
			return &metrics.DudStat{}
		})
	}

	if p.useHistogramTiming {
		return p.getTimerHistVec(path, labelNames...)
	}

	var pv *promTimingVec

	p.mut.Lock()
	var exists bool
	if pv, exists = p.timers[path]; !exists {
		tmr := prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Name:       path,
			Help:       "Benthos Timing metric",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}, labelNames)
		p.reg.MustRegister(tmr)

		pv = &promTimingVec{
			sum:   tmr,
			count: len(labelNames),
		}
		p.timers[path] = pv
	}
	p.mut.Unlock()

	if pv.count != len(labelNames) {
		p.log.Errorf("Metrics label mismatch %v versus %v %v for name '%v', skipping metric", pv.count, len(labelNames), labelNames, path)
		return metrics.Noop().GetTimerVec(path, labelNames...)
	}
	return pv
}

func (p *prometheusMetrics) getTimerHistVec(path string, labelNames ...string) metrics.StatTimerVec {
	var pv *promTimingHistVec

	p.mut.Lock()
	var exists bool
	if pv, exists = p.timersHist[path]; !exists {
		tmr := prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    path,
			Help:    "Benthos Timing metric",
			Buckets: p.histogramBuckets,
		}, labelNames)
		p.reg.MustRegister(tmr)

		pv = &promTimingHistVec{
			sum:   tmr,
			count: len(labelNames),
		}
		p.timersHist[path] = pv
	}
	p.mut.Unlock()

	if pv.count != len(labelNames) {
		p.log.Errorf("Metrics label mismatch %v versus %v %v for name '%v', skipping metric", pv.count, len(labelNames), labelNames, path)
		return metrics.Noop().GetTimerVec(path, labelNames...)
	}
	return pv
}

func (p *prometheusMetrics) GetGauge(path string) metrics.StatGauge {
	return p.GetGaugeVec(path).With()
}

func (p *prometheusMetrics) GetGaugeVec(path string, labelNames ...string) metrics.StatGaugeVec {
	if !model.IsValidMetricName(model.LabelValue(path)) {
		p.log.Errorf("Ignoring metric '%v' due to invalid name", path)
		return metrics.FakeGaugeVec(func(l ...string) metrics.StatGauge {
			return &metrics.DudStat{}
		})
	}

	var pv *promGaugeVec

	p.mut.Lock()
	var exists bool
	if pv, exists = p.gauges[path]; !exists {
		ctr := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: path,
			Help: "Benthos Gauge metric",
		}, labelNames)
		p.reg.MustRegister(ctr)

		pv = &promGaugeVec{
			ctr:   ctr,
			count: len(labelNames),
		}
		p.gauges[path] = pv
	}
	p.mut.Unlock()

	if pv.count != len(labelNames) {
		p.log.Errorf("Metrics label mismatch %v versus %v %v for name '%v', skipping metric", pv.count, len(labelNames), labelNames, path)
		return metrics.Noop().GetGaugeVec(path, labelNames...)
	}
	return pv
}

func (p *prometheusMetrics) Close() error {
	if atomic.CompareAndSwapInt32(&p.running, 1, 0) {
		close(p.closedChan)
	}
	if p.pusher != nil {
		err := p.pusher.Push()
		if err != nil {
			return err
		}
	}
	if p.fileOutputPath != "" {
		return prometheus.WriteToTextfile(p.fileOutputPath, p.reg)
	}

	return nil
}
