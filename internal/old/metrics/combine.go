package metrics

import "net/http"

type combinedWrapper struct {
	t1 Type
	t2 Type
}

// Combine returns a Type implementation that feeds metrics into two underlying
// Type implementations.
func Combine(t1, t2 Type) Type {
	return &combinedWrapper{
		t1: t1,
		t2: t2,
	}
}

func unwrapMetric(t Type) Type {
	u, ok := t.(interface {
		Unwrap() Type
	})
	if ok {
		t = u.Unwrap()
	}
	return t
}

// Unwrap to the underlying metrics type.
func (c *combinedWrapper) Unwrap() Type {
	t1 := unwrapMetric(c.t1)
	t2 := unwrapMetric(c.t2)
	if t1 == t2 {
		return t1
	}
	return &combinedWrapper{
		t1: t1,
		t2: t2,
	}
}

//------------------------------------------------------------------------------

type combinedCounter struct {
	c1 StatCounter
	c2 StatCounter
}

func (c *combinedCounter) Incr(count int64) {
	c.c1.Incr(count)
	c.c2.Incr(count)
}

type combinedTimer struct {
	c1 StatTimer
	c2 StatTimer
}

func (c *combinedTimer) Timing(delta int64) {
	c.c1.Timing(delta)
	c.c2.Timing(delta)
}

type combinedGauge struct {
	c1 StatGauge
	c2 StatGauge
}

func (c *combinedGauge) Set(value int64) {
	c.c1.Set(value)
	c.c2.Set(value)
}

func (c *combinedGauge) Incr(count int64) {
	c.c1.Incr(count)
	c.c2.Incr(count)
}

func (c *combinedGauge) Decr(count int64) {
	c.c1.Decr(count)
	c.c2.Decr(count)
}

//------------------------------------------------------------------------------

type combinedCounterVec struct {
	c1 StatCounterVec
	c2 StatCounterVec
}

func (c *combinedCounterVec) With(labelValues ...string) StatCounter {
	return &combinedCounter{
		c1: c.c1.With(labelValues...),
		c2: c.c2.With(labelValues...),
	}
}

type combinedTimerVec struct {
	c1 StatTimerVec
	c2 StatTimerVec
}

func (c *combinedTimerVec) With(labelValues ...string) StatTimer {
	return &combinedTimer{
		c1: c.c1.With(labelValues...),
		c2: c.c2.With(labelValues...),
	}
}

type combinedGaugeVec struct {
	c1 StatGaugeVec
	c2 StatGaugeVec
}

func (c *combinedGaugeVec) With(labelValues ...string) StatGauge {
	return &combinedGauge{
		c1: c.c1.With(labelValues...),
		c2: c.c2.With(labelValues...),
	}
}

//------------------------------------------------------------------------------

func (c *combinedWrapper) GetCounter(path string) StatCounter {
	return &combinedCounter{
		c1: c.t1.GetCounter(path),
		c2: c.t2.GetCounter(path),
	}
}

func (c *combinedWrapper) GetCounterVec(path string, n ...string) StatCounterVec {
	return &combinedCounterVec{
		c1: c.t1.GetCounterVec(path, n...),
		c2: c.t2.GetCounterVec(path, n...),
	}
}

func (c *combinedWrapper) GetTimer(path string) StatTimer {
	return &combinedTimer{
		c1: c.t1.GetTimer(path),
		c2: c.t2.GetTimer(path),
	}
}

func (c *combinedWrapper) GetTimerVec(path string, n ...string) StatTimerVec {
	return &combinedTimerVec{
		c1: c.t1.GetTimerVec(path, n...),
		c2: c.t2.GetTimerVec(path, n...),
	}
}

func (c *combinedWrapper) GetGauge(path string) StatGauge {
	return &combinedGauge{
		c1: c.t1.GetGauge(path),
		c2: c.t2.GetGauge(path),
	}
}

func (c *combinedWrapper) GetGaugeVec(path string, n ...string) StatGaugeVec {
	return &combinedGaugeVec{
		c1: c.t1.GetGaugeVec(path, n...),
		c2: c.t2.GetGaugeVec(path, n...),
	}
}

func (c *combinedWrapper) HandlerFunc() http.HandlerFunc {
	if h := c.t1.HandlerFunc(); h != nil {
		return h
	}
	return c.t2.HandlerFunc()
}

func (c *combinedWrapper) Close() error {
	c.t1.Close()
	c.t2.Close()
	return nil
}
