package http_metrics

import (
	"net/http"
	"sync/atomic"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

/*
Transport is a wrapper around an http.RoundTripper that tracks request metrics.
*/
type Transport struct {
	base    http.RoundTripper
	metrics *service.Metrics
	ns      string

	inflight    *service.MetricGauge
	inflightVal int64 // track ourselves; gauge uses Set()
	total       *service.MetricCounter
	errors      *service.MetricCounter
	status2xx   *service.MetricCounter
	status4xx   *service.MetricCounter
	status5xx   *service.MetricCounter
	duration    *service.MetricTimer
}

/*
NewTransport creates new Transport with metrics instrumentation. It takes a metrics
instance, namespace string for metric names, and an optional HTTP transport (if nil, creates a new one).
The function returns Transport that tracks request metrics such as in-flight requests, response status codes, errors, and request duration.
*/
func NewTransport(m *service.Metrics, namespace string, base http.RoundTripper) *Transport {
	if base == nil {
		base = http.DefaultTransport
	}
	return &Transport{
		base:      base,
		metrics:   m,
		ns:        namespace,
		inflight:  m.NewGauge(namespace + "_in_flight"),
		total:     m.NewCounter(namespace + "_requests_total"),
		errors:    m.NewCounter(namespace + "_requests_errors"),
		status2xx: m.NewCounter(namespace + "_responses_2xx"),
		status4xx: m.NewCounter(namespace + "_responses_4xx"),
		status5xx: m.NewCounter(namespace + "_responses_5xx"),
		duration:  m.NewTimer(namespace + "_request_duration"),
	}
}

/*
RoundTrip implements the http.RoundTripper interface.
*/
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	start := time.Now()

	// in-flight ++
	atomic.AddInt64(&t.inflightVal, 1)
	t.inflight.Set(atomic.LoadInt64(&t.inflightVal))

	// always record end-of-request updates
	defer func() {
		// duration in nanoseconds (MetricTimer expects int64)
		t.duration.Timing(time.Since(start).Nanoseconds())
		// in-flight --
		atomic.AddInt64(&t.inflightVal, -1)
		t.inflight.Set(atomic.LoadInt64(&t.inflightVal))
	}()

	t.total.Incr(1)

	resp, err := t.base.RoundTrip(req)
	if err != nil {
		t.errors.Incr(1)
		return nil, err
	}

	switch code := resp.StatusCode; {
	case code >= 200 && code < 300:
		t.status2xx.Incr(1)
	case code >= 400 && code < 500:
		t.status4xx.Incr(1)
	case code >= 500:
		t.status5xx.Incr(1)
	}

	return resp, nil
}

/*
NewInstrumentedClient creates a new HTTP client with metrics instrumentation. It takes a metrics
instance, namespace string for metric names, and an optional HTTP client (if nil, creates a new one).
The function returns a clone of the input client with an instrumented transport that tracks request
metrics such as in-flight requests, response status codes, errors, and request duration.
*/
func NewInstrumentedClient(m *service.Metrics, namespace string, client *http.Client) *http.Client {
	if client == nil {
		client = &http.Client{}
	}
	clone := *client
	clone.Transport = NewTransport(m, namespace, client.Transport)
	return &clone
}
