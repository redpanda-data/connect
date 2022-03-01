package metrics

import "github.com/Jeffail/benthos/v3/internal/old/metrics"

type fCounterVec struct {
	f func([]string) metrics.StatCounter
}

func (f *fCounterVec) With(labels ...string) metrics.StatCounter {
	return f.f(labels)
}

func fakeCounterVec(f func([]string) metrics.StatCounter) metrics.StatCounterVec {
	return &fCounterVec{
		f: f,
	}
}

//------------------------------------------------------------------------------

type fTimerVec struct {
	f func([]string) metrics.StatTimer
}

func (f *fTimerVec) With(labels ...string) metrics.StatTimer {
	return f.f(labels)
}

func fakeTimerVec(f func([]string) metrics.StatTimer) metrics.StatTimerVec {
	return &fTimerVec{
		f: f,
	}
}

//------------------------------------------------------------------------------

type fGaugeVec struct {
	f func([]string) metrics.StatGauge
}

func (f *fGaugeVec) With(labels ...string) metrics.StatGauge {
	return f.f(labels)
}

func fakeGaugeVec(f func([]string) metrics.StatGauge) metrics.StatGaugeVec {
	return &fGaugeVec{
		f: f,
	}
}
