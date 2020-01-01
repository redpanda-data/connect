package metrics

import (
	"github.com/Jeffail/benthos/v3/lib/log"
)

//------------------------------------------------------------------------------

// flatStat is a representation of a single metric stat. Interactions with this
// stat are thread safe.
type flatStat struct {
	path string
	f    Flat
}

// Incr increments a metric by an amount.
func (f *flatStat) Incr(count int64) error {
	f.f.Incr(f.path, count)
	return nil
}

// Decr decrements a metric by an amount.
func (f *flatStat) Decr(count int64) error {
	f.f.Decr(f.path, count)
	return nil
}

// Timing sets a timing metric.
func (f *flatStat) Timing(delta int64) error {
	f.f.Timing(f.path, delta)
	return nil
}

// Set sets a gauge metric.
func (f *flatStat) Set(value int64) error {
	f.f.Gauge(f.path, value)
	return nil
}

//------------------------------------------------------------------------------

// wrappedFlat implements the entire Type interface around a Flat type.
type wrappedFlat struct {
	f Flat
}

// WrapFlat creates a Type around a Flat implementation.
func WrapFlat(f Flat) Type {
	return &wrappedFlat{
		f: f,
	}
}

//------------------------------------------------------------------------------

// GetCounter returns a stat counter object for a path.
func (h *wrappedFlat) GetCounter(path string) StatCounter {
	return &flatStat{
		path: path,
		f:    h.f,
	}
}

// GetCounterVec returns a stat counter object for a path with the labels
// discarded.
func (h *wrappedFlat) GetCounterVec(path string, n []string) StatCounterVec {
	return fakeCounterVec(func([]string) StatCounter {
		return &flatStat{
			path: path,
			f:    h.f,
		}
	})
}

// GetTimer returns a stat timer object for a path.
func (h *wrappedFlat) GetTimer(path string) StatTimer {
	return &flatStat{
		path: path,
		f:    h.f,
	}
}

// GetTimerVec returns a stat timer object for a path with the labels
// discarded.
func (h *wrappedFlat) GetTimerVec(path string, n []string) StatTimerVec {
	return fakeTimerVec(func([]string) StatTimer {
		return &flatStat{
			path: path,
			f:    h.f,
		}
	})
}

// GetGauge returns a stat gauge object for a path.
func (h *wrappedFlat) GetGauge(path string) StatGauge {
	return &flatStat{
		path: path,
		f:    h.f,
	}
}

// GetGaugeVec returns a stat timer object for a path with the labels
// discarded.
func (h *wrappedFlat) GetGaugeVec(path string, n []string) StatGaugeVec {
	return fakeGaugeVec(func([]string) StatGauge {
		return &flatStat{
			path: path,
			f:    h.f,
		}
	})
}

// SetLogger does nothing.
func (h *wrappedFlat) SetLogger(log log.Modular) {
}

// Close stops the wrappedFlat object from aggregating metrics and cleans up
// resources.
func (h *wrappedFlat) Close() error {
	return h.f.Close()
}

//------------------------------------------------------------------------------
