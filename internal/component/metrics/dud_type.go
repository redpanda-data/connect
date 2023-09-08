package metrics

import "net/http"

// DudStat implements the Stat interface but doesn't actual do anything.
type DudStat struct{}

// Incr does nothing.
func (d DudStat) Incr(count int64) {}

// Decr does nothing.
func (d DudStat) Decr(count int64) {}

// Timing does nothing.
func (d DudStat) Timing(delta int64) {}

// Set does nothing.
func (d DudStat) Set(value int64) {}

// SetFloat64 does nothing
func (d DudStat) SetFloat64(value float64) {}

// IncrFloat64 does nothing
func (d DudStat) IncrFloat64(count float64) {}

// DecrFloat64 does nothing
func (d DudStat) DecrFloat64(count float64) {}

//------------------------------------------------------------------------------

var _ Type = DudType{}

// DudType implements the Type interface but doesn't actual do anything.
type DudType struct {
	ID int
}

// GetCounter returns a DudStat.
func (d DudType) GetCounter(path string) StatCounter {
	return DudStat{}
}

// GetCounterVec returns a DudStat.
func (d DudType) GetCounterVec(path string, n ...string) StatCounterVec {
	return FakeCounterVec(func(...string) StatCounter {
		return DudStat{}
	})
}

// GetTimer returns a DudStat.
func (d DudType) GetTimer(path string) StatTimer {
	return DudStat{}
}

// GetTimerVec returns a DudStat.
func (d DudType) GetTimerVec(path string, n ...string) StatTimerVec {
	return FakeTimerVec(func(...string) StatTimer {
		return DudStat{}
	})
}

// GetGauge returns a DudStat.
func (d DudType) GetGauge(path string) StatGauge {
	return DudStat{}
}

// HandlerFunc returns nil.
func (d DudType) HandlerFunc() http.HandlerFunc {
	return nil
}

// GetGaugeVec returns a DudStat.
func (d DudType) GetGaugeVec(path string, n ...string) StatGaugeVec {
	return FakeGaugeVec(func(...string) StatGauge {
		return DudStat{}
	})
}

// Close does nothing.
func (d DudType) Close() error { return nil }
