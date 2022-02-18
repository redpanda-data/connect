package metrics

type fCounterVec struct {
	f func(...string) StatCounter
}

func (f *fCounterVec) With(labels ...string) StatCounter {
	return f.f(labels...)
}

func fakeCounterVec(f func(...string) StatCounter) StatCounterVec {
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

func fakeTimerVec(f func(...string) StatTimer) StatTimerVec {
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

func fakeGaugeVec(f func(...string) StatGauge) StatGaugeVec {
	return &fGaugeVec{
		f: f,
	}
}
