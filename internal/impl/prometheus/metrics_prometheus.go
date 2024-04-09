package prometheus

import (
	"context"
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

	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	pmFieldUseHistogramTiming          = "use_histogram_timing"
	pmFieldHistogramBuckets            = "histogram_buckets"
	pmFieldSummaryQuantilesObj         = "summary_quantiles_objectives"
	pmFieldSummaryQuantilesObjQuantile = "quantile"
	pmFieldSummaryQuantilesObjError    = "error"
	pmFieldAddProcessMetrics           = "add_process_metrics"
	pmFieldAddGoMetrics                = "add_go_metrics"
	pmFieldPushURL                     = "push_url"
	pmFieldPushBasicAuth               = "push_basic_auth"
	pmFieldPushBasicAuthUsername       = "username"
	pmFieldPushBasicAuthPassword       = "password"
	pmFieldPushInterval                = "push_interval"
	pmFieldPushJobName                 = "push_job_name"
	pmFieldFileOutputPath              = "file_output_path"
)

func ConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Summary("Host endpoints (`/metrics` and `/stats`) for Prometheus scraping.").
		Footnotes(`
## Push Gateway

The field `+"`push_url`"+` is optional and when set will trigger a push of metrics to a [Prometheus Push Gateway](https://prometheus.io/docs/instrumenting/pushing/) once Benthos shuts down. It is also possible to specify a `+"`push_interval`"+` which results in periodic pushes.

The Push Gateway is useful for when Benthos instances are short lived. Do not include the "/metrics/jobs/..." path in the push URL.

If the Push Gateway requires HTTP Basic Authentication it can be configured with `+"`push_basic_auth`.").
		Fields(
			service.NewBoolField(pmFieldUseHistogramTiming).
				Description("Whether to export timing metrics as a histogram, if `false` a summary is used instead. When exporting histogram timings the delta values are converted from nanoseconds into seconds in order to better fit within bucket definitions. For more information on histograms and summaries refer to: https://prometheus.io/docs/practices/histograms/.").
				Version("3.63.0").
				Advanced().
				Default(false),
			service.NewFloatListField(pmFieldHistogramBuckets).
				Description("Timing metrics histogram buckets (in seconds). If left empty defaults to DefBuckets (https://pkg.go.dev/github.com/prometheus/client_golang/prometheus#pkg-variables). Applicable when `use_histogram_timing` is set to `true`.").
				Advanced().
				Version("3.63.0").
				Default([]any{}),
			service.NewObjectListField(pmFieldSummaryQuantilesObj,
				service.NewFloatField(pmFieldSummaryQuantilesObjQuantile).
					Description("Quantile value.").
					Default(0.0),
				service.NewFloatField(pmFieldSummaryQuantilesObjError).
					Description("Permissible margin of error for quantile calculations. Precise calculations in a streaming context (without prior knowledge of the full dataset) can be resource-intensive. To balance accuracy with computational efficiency, an error margin is introduced. For instance, if the 90th quantile (`0.9`) is determined to be `100ms` with a 1% error margin (`0.01`), the true value will fall within the `[99ms, 101ms]` range.)").
					Default(0.0),
			).
				Description("A list of timing metrics summary buckets (as quantiles). Applicable when `use_histogram_timing` is set to `false`.").
				Example([]map[string]float64{
					{"quantile": 0.5, "error": 0.05},
					{"quantile": 0.9, "error": 0.01},
					{"quantile": 0.99, "error": 0.001},
				}).
				Advanced().
				Version("4.23.0").
				Default([]map[string]float64{
					{"quantile": 0.5, "error": 0.05},
					{"quantile": 0.9, "error": 0.01},
					{"quantile": 0.99, "error": 0.001},
				}),
			service.NewBoolField(pmFieldAddProcessMetrics).
				Description("Whether to export process metrics such as CPU and memory usage in addition to Benthos metrics.").
				Advanced().
				Default(false),
			service.NewBoolField(pmFieldAddGoMetrics).
				Description("Whether to export Go runtime metrics such as GC pauses in addition to Benthos metrics.").
				Advanced().
				Default(false),
			service.NewURLField(pmFieldPushURL).
				Description("An optional [Push Gateway URL](#push-gateway) to push metrics to.").
				Advanced().
				Optional(),
			service.NewDurationField(pmFieldPushInterval).
				Description("The period of time between each push when sending metrics to a Push Gateway.").
				Advanced().
				Optional(),
			service.NewStringField(pmFieldPushJobName).
				Description("An identifier for push jobs.").
				Advanced().
				Default("benthos_push"),
			service.NewObjectField(pmFieldPushBasicAuth,
				service.NewStringField(pmFieldPushBasicAuthUsername).
					Description("The Basic Authentication username.").
					Default(""),
				service.NewStringField(pmFieldPushBasicAuthPassword).
					Description("The Basic Authentication password.").
					Secret().
					Default(""),
			).Description("The Basic Authentication credentials.").
				Advanced(),
			service.NewStringField(pmFieldFileOutputPath).
				Description("An optional file path to write all prometheus metrics on service shutdown.").
				Advanced().
				Default(""),
		)
}

func init() {
	err := service.RegisterMetricsExporter(
		"prometheus", ConfigSpec(),
		func(conf *service.ParsedConfig, log *service.Logger) (service.MetricsExporter, error) {
			return FromParsed(conf, log)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type promGauge struct {
	ctr prometheus.Gauge
}

func (p *promGauge) Incr(count int64) {
	p.ctr.Add(float64(count))
}

func (p *promGauge) IncrFloat64(count float64) {
	p.ctr.Add(count)
}

func (p *promGauge) Decr(count int64) {
	p.ctr.Add(float64(-count))
}

func (p *promGauge) DecrFloat64(count float64) {
	p.ctr.Add(-count)
}

func (p *promGauge) Set(value int64) {
	p.ctr.Set(float64(value))
}

func (p *promGauge) SetFloat64(value float64) {
	p.ctr.Set(value)
}

type promCounter struct {
	ctr prometheus.Counter
}

func (p *promCounter) Incr(count int64) {
	p.ctr.Add(float64(count))
}

func (p *promCounter) IncrFloat64(count float64) {
	p.ctr.Add(count)
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

type Metrics struct {
	log        *service.Logger
	closedChan chan struct{}
	running    int32

	fileOutputPath string

	useHistogramTiming bool
	histogramBuckets   []float64
	summaryQuantiles   map[float64]float64

	pusher *push.Pusher
	reg    *prometheus.Registry

	counters   map[string]*promCounterVec
	gauges     map[string]*promGaugeVec
	timers     map[string]*promTimingVec
	timersHist map[string]*promTimingHistVec

	mut sync.Mutex
}

func quantilesAsFloatMapFromParsed(confs []*service.ParsedConfig) (map[float64]float64, error) {
	resultFloatMap := map[float64]float64{}
	for _, c := range confs {
		quantile, err := c.FieldFloat(pmFieldSummaryQuantilesObjQuantile)
		if err != nil {
			return nil, err
		}
		fErr, err := c.FieldFloat(pmFieldSummaryQuantilesObjError)
		if err != nil {
			return nil, err
		}
		resultFloatMap[quantile] = fErr
	}
	return resultFloatMap, nil
}

func FromParsed(conf *service.ParsedConfig, log *service.Logger) (p *Metrics, err error) {
	p = &Metrics{
		log:        log,
		running:    1,
		closedChan: make(chan struct{}),
		reg:        prometheus.NewRegistry(),
		counters:   map[string]*promCounterVec{},
		gauges:     map[string]*promGaugeVec{},
		timers:     map[string]*promTimingVec{},
		timersHist: map[string]*promTimingHistVec{},
	}

	if p.useHistogramTiming, err = conf.FieldBool(pmFieldUseHistogramTiming); err != nil {
		return
	}

	if p.histogramBuckets, err = conf.FieldFloatList(pmFieldHistogramBuckets); err != nil {
		return
	}
	if len(p.histogramBuckets) == 0 {
		p.histogramBuckets = prometheus.DefBuckets
	}

	if quantilesParsedList, _ := conf.FieldObjectList(pmFieldSummaryQuantilesObj); len(quantilesParsedList) > 0 {
		if p.summaryQuantiles, err = quantilesAsFloatMapFromParsed(quantilesParsedList); err != nil {
			return
		}
	} else {
		p.summaryQuantiles = map[float64]float64{
			0.5:  0.05,
			0.9:  0.01,
			0.99: 0.001,
		}
	}

	if addProcMets, _ := conf.FieldBool(pmFieldAddProcessMetrics); addProcMets {
		if err := p.reg.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})); err != nil {
			return nil, err
		}
	}
	if addGoMets, _ := conf.FieldBool(pmFieldAddGoMetrics); addGoMets {
		if err := p.reg.Register(collectors.NewGoCollector()); err != nil {
			return nil, err
		}
	}

	if pushURL, _ := conf.FieldString(pmFieldPushURL); pushURL != "" {
		pushJobName, _ := conf.FieldString(pmFieldPushJobName)
		p.pusher = push.New(pushURL, pushJobName).Gatherer(p.reg)

		basicAuthUsername, _ := conf.FieldString(pmFieldPushBasicAuth, pmFieldPushBasicAuthUsername)
		basicAuthPassword, _ := conf.FieldString(pmFieldPushBasicAuth, pmFieldPushBasicAuthPassword)

		if basicAuthUsername != "" && basicAuthPassword != "" {
			p.pusher = p.pusher.BasicAuth(basicAuthUsername, basicAuthPassword)
		}

		pushInterval, _ := conf.FieldString(pmFieldPushInterval)
		if pushInterval != "" {
			interval, err := time.ParseDuration(pushInterval)
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

	p.fileOutputPath, _ = conf.FieldString(pmFieldFileOutputPath)
	return p, nil
}

//------------------------------------------------------------------------------

func (p *Metrics) HandlerFunc() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		promhttp.HandlerFor(p.reg, promhttp.HandlerOpts{}).ServeHTTP(w, r)
	}
}

func (p *Metrics) NewCounterCtor(path string, labelNames ...string) service.MetricsExporterCounterCtor {
	if !model.IsValidMetricName(model.LabelValue(path)) {
		p.log.Errorf("Ignoring metric '%v' due to invalid name", path)
		return func(labelValues ...string) service.MetricsExporterCounter {
			return metrics.DudStat{}
		}
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
		return func(labelValues ...string) service.MetricsExporterCounter {
			return metrics.DudStat{}
		}
	}
	return func(labelValues ...string) service.MetricsExporterCounter {
		return pv.With(labelValues...)
	}
}

func (p *Metrics) NewTimerCtor(path string, labelNames ...string) service.MetricsExporterTimerCtor {
	if !model.IsValidMetricName(model.LabelValue(path)) {
		p.log.Errorf("Ignoring metric '%v' due to invalid name", path)
		return func(labelValues ...string) service.MetricsExporterTimer {
			return metrics.DudStat{}
		}
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
			Objectives: p.summaryQuantiles,
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
		return func(labelValues ...string) service.MetricsExporterTimer {
			return metrics.DudStat{}
		}
	}
	return func(labelValues ...string) service.MetricsExporterTimer {
		return pv.With(labelValues...)
	}
}

func (p *Metrics) getTimerHistVec(path string, labelNames ...string) service.MetricsExporterTimerCtor {
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
		return func(labelValues ...string) service.MetricsExporterTimer {
			return metrics.DudStat{}
		}
	}
	return func(labelValues ...string) service.MetricsExporterTimer {
		return pv.With(labelValues...)
	}
}

func (p *Metrics) NewGaugeCtor(path string, labelNames ...string) service.MetricsExporterGaugeCtor {
	if !model.IsValidMetricName(model.LabelValue(path)) {
		p.log.Errorf("Ignoring metric '%v' due to invalid name", path)
		return func(labelValues ...string) service.MetricsExporterGauge {
			return &metrics.DudStat{}
		}
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
		return func(labelValues ...string) service.MetricsExporterGauge {
			return metrics.DudStat{}
		}
	}
	return func(labelValues ...string) service.MetricsExporterGauge {
		return pv.With(labelValues...)
	}
}

func (p *Metrics) Close(context.Context) error {
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
