package metrics

import (
	"net/http"
)

// StatCounter is a representation of a single counter metric stat. Interactions
// with this stat are thread safe.
type StatCounter interface {
	// Incr increments a counter by an amount.
	Incr(count int64)
}

// StatTimer is a representation of a single timer metric stat, timing values
// should be presented in nanoseconds for consistency. Interactions with this
// stat are thread safe.
type StatTimer interface {
	// Timing sets a timing metric.
	Timing(delta int64)
}

// StatGauge is a representation of a single gauge metric stat. Interactions
// with this stat are thread safe.
type StatGauge interface {
	StatGaugeInter
	StatGaugeFloater
}

// StatGaugeInter is a representation of a single integer gauge metric stat. Interactions
// with this stat are thread safe.
type StatGaugeInter interface {
	// Set sets the value of a gauge metric.
	Set(value int64)

	// Incr increments a gauge by an amount.
	Incr(count int64)

	// Decr decrements a gauge by an amount.
	Decr(count int64)
}

// StatGaugeFloater is a representation of a single decimal gauge metric stat. Interactions
// with this stat are thread safe.
type StatGaugeFloater interface {
	// SetFloat64 sets the value of a gauge metric.
	SetFloat64(value float64)

	// IncrFloat64 increments a gauge by an amount.
	IncrFloat64(count float64)

	// DecrFloat64 decrements a gauge by an amount.
	DecrFloat64(count float64)
}

//------------------------------------------------------------------------------

// StatCounterVec creates StatCounters with dynamic labels.
type StatCounterVec interface {
	// With returns a StatCounter with a set of label values.
	With(labelValues ...string) StatCounter
}

// StatTimerVec creates StatTimers with dynamic labels.
type StatTimerVec interface {
	// With returns a StatTimer with a set of label values.
	With(labelValues ...string) StatTimer
}

// StatGaugeVec creates StatGauges with dynamic labels.
type StatGaugeVec interface {
	// With returns a StatGauge with a set of label values.
	With(labelValues ...string) StatGauge
}

//------------------------------------------------------------------------------

// Type is an interface for metrics aggregation.
type Type interface {
	// GetCounter returns an editable counter stat for a given path.
	GetCounter(path string) StatCounter

	// GetCounterVec returns an editable counter stat for a given path with labels,
	// these labels must be consistent with any other metrics registered on the
	// same path.
	GetCounterVec(path string, labelNames ...string) StatCounterVec

	// GetTimer returns an editable timer stat for a given path.
	GetTimer(path string) StatTimer

	// GetTimerVec returns an editable timer stat for a given path with labels,
	// these labels must be consistent with any other metrics registered on the
	// same path.
	GetTimerVec(path string, labelNames ...string) StatTimerVec

	// GetGauge returns an editable gauge stat for a given path.
	GetGauge(path string) StatGauge

	// GetGaugeVec returns an editable gauge stat for a given path with labels,
	// these labels must be consistent with any other metrics registered on the
	// same path.
	GetGaugeVec(path string, labelNames ...string) StatGaugeVec

	// HandlerFunc returns an optional HTTP request handler that exposes metrics
	// from the implementation. If nil is returned then no endpoint will be
	// registered.
	HandlerFunc() http.HandlerFunc

	// Close stops aggregating stats and cleans up resources.
	Close() error
}
