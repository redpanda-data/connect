package metrics

import "github.com/Jeffail/benthos/v3/lib/log"

// DudStat implements the Stat interface but doesn't actual do anything.
type DudStat struct{}

// Incr does nothing.
func (d DudStat) Incr(count int64) error { return nil }

// Decr does nothing.
func (d DudStat) Decr(count int64) error { return nil }

// Timing does nothing.
func (d DudStat) Timing(delta int64) error { return nil }

// Set does nothing.
func (d DudStat) Set(value int64) error { return nil }

//------------------------------------------------------------------------------

var _ Type = DudType{}

// DudType implements the Type interface but doesn't actual do anything.
type DudType struct {
	ID int
}

// Noop returns a DudType for discarding metrics.
func Noop() DudType {
	return DudType{}
}

// GetCounter returns a DudStat.
func (d DudType) GetCounter(path string) StatCounter { return DudStat{} }

// GetCounterVec returns a DudStat.
func (d DudType) GetCounterVec(path string, n []string) StatCounterVec {
	return fakeCounterVec(func([]string) StatCounter {
		return DudStat{}
	})
}

// GetTimer returns a DudStat.
func (d DudType) GetTimer(path string) StatTimer { return DudStat{} }

// GetTimerVec returns a DudStat.
func (d DudType) GetTimerVec(path string, n []string) StatTimerVec {
	return fakeTimerVec(func([]string) StatTimer {
		return DudStat{}
	})
}

// GetGauge returns a DudStat.
func (d DudType) GetGauge(path string) StatGauge { return DudStat{} }

// GetGaugeVec returns a DudStat.
func (d DudType) GetGaugeVec(path string, n []string) StatGaugeVec {
	return fakeGaugeVec(func([]string) StatGauge {
		return DudStat{}
	})
}

// SetLogger does nothing.
func (d DudType) SetLogger(log log.Modular) {}

// Close does nothing.
func (d DudType) Close() error { return nil }

//------------------------------------------------------------------------------
