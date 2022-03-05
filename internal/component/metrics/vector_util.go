package metrics

type fCounterVec struct {
	f func(...string) StatCounter
}

func (f *fCounterVec) With(labels ...string) StatCounter {
	return f.f(labels...)
}

// FakeCounterVec returns a counter vec implementation that ignores labels.
func FakeCounterVec(f func(...string) StatCounter) StatCounterVec {
	return &fCounterVec{
		f: f,
	}
}

//------------------------------------------------------------------------------

type fTimerVec struct {
	f func(...string) StatTimer
}

func (f *fTimerVec) With(labels ...string) StatTimer {
	return f.f(labels...)
}

// FakeTimerVec returns a timer vec implementation that ignores labels.
func FakeTimerVec(f func(...string) StatTimer) StatTimerVec {
	return &fTimerVec{
		f: f,
	}
}

//------------------------------------------------------------------------------

type fGaugeVec struct {
	f func(...string) StatGauge
}

func (f *fGaugeVec) With(labels ...string) StatGauge {
	return f.f(labels...)
}

// FakeGaugeVec returns a gauge vec implementation that ignores labels.
func FakeGaugeVec(f func(...string) StatGauge) StatGaugeVec {
	return &fGaugeVec{
		f: f,
	}
}
