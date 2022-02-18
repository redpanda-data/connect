package service

import (
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

// Metrics allows plugin authors to emit custom metrics from components that are
// exported the same way as native Benthos metrics. It's safe to pass around a
// nil pointer for testing components.
type Metrics struct {
	t metrics.Type
}

func newReverseAirGapMetrics(t metrics.Type) *Metrics {
	return &Metrics{t}
}

// NewCounter creates a new counter metric with a name and variant list of label
// keys.
func (m *Metrics) NewCounter(name string, labelKeys ...string) *MetricCounter {
	if m == nil {
		return nil
	}
	cv := m.t.GetCounterVec(name, labelKeys...)
	return &MetricCounter{cv}
}

// NewTimer creates a new timer metric with a name and variant list of label
// keys.
func (m *Metrics) NewTimer(name string, labelKeys ...string) *MetricTimer {
	if m == nil {
		return nil
	}
	tv := m.t.GetTimerVec(name, labelKeys...)
	return &MetricTimer{tv}
}

// NewGauge creates a new gauge metric with a name and variant list of label
// keys.
func (m *Metrics) NewGauge(name string, labelKeys ...string) *MetricGauge {
	if m == nil {
		return nil
	}
	gv := m.t.GetGaugeVec(name, labelKeys...)
	return &MetricGauge{gv}
}

//------------------------------------------------------------------------------

// MetricCounter represents a counter metric of a given name and labels.
type MetricCounter struct {
	cv metrics.StatCounterVec
}

// Incr increments a counter metric by an amount, the number of label values
// must match the number and order of labels specified when the counter was
// created.
func (c *MetricCounter) Incr(count int64, labelValues ...string) {
	if c == nil {
		return
	}
	c.cv.With(labelValues...).Incr(count)
}

// MetricTimer represents a timing metric of a given name and labels.
type MetricTimer struct {
	tv metrics.StatTimerVec
}

// Timing adds a delta to a timing metric, the number of label values must match
// the number and order of labels specified when the timing was created.
func (t *MetricTimer) Timing(delta int64, labelValues ...string) {
	if t == nil {
		return
	}
	t.tv.With(labelValues...).Timing(delta)
}

// MetricGauge represents a gauge metric of a given name and labels.
type MetricGauge struct {
	gv metrics.StatGaugeVec
}

// Set a gauge metric, the number of label values must match the number and
// order of labels specified when the gauge was created.
func (g *MetricGauge) Set(value int64, labelValues ...string) {
	if g == nil {
		return
	}
	g.gv.With(labelValues...).Set(value)
}
