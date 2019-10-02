package metrics

import "testing"

func TestCounter(t *testing.T) {
	path := "testing.label"
	local := NewLocal()
	label := "tested"
	counter := local.GetCounterVec(path, []string{label})
	value := "true"

	counter.With(value).Incr(1)

	counters := local.GetCountersWithLabels()
	c, ok := counters[path]
	if !ok {
		t.Fatal("did not find counter for path")
	}

	if *c.Value != 1 {
		t.Fatalf("value for counter: got %d, wanted 1", *c.Value)
	}

	// test second call results in the same value
	counters = local.GetCountersWithLabels()
	c, ok = counters[path]
	if !ok {
		t.Fatal("did not find counter for path")
	}

	if *c.Value != 1 {
		t.Fatalf("value for counter: got %d, wanted 1", *c.Value)
	}
}

func TestFlushCounter(t *testing.T) {
	path := "testing.label"
	local := NewLocal()
	label := "tested"
	counter := local.GetCounterVec(path, []string{label})
	value := "true"

	counter.With(value).Incr(1)

	counters := local.FlushCounters()
	c, ok := counters[path]
	if !ok {
		t.Fatal("did not find counter for path")
	}

	if c != 1 {
		t.Fatalf("value for counter: got %d, wanted 1", c)
	}

	// test second call results in a reset counter
	counters = local.FlushCounters()
	c, ok = counters[path]
	if !ok {
		t.Fatal("did not find counter for path")
	}

	if c != 0 {
		t.Fatalf("value for flushed counter: got %d, wanted 0", c)
	}

}

func TestCounterWithLabelsAndValues(t *testing.T) {
	path := "testing.label"
	local := NewLocal()
	label := "tested"
	counter := local.GetCounterVec(path, []string{label})
	value := "true"

	counter.With(value).Incr(1)

	counters := local.GetCountersWithLabels()
	c, ok := counters[path]
	if !ok {
		t.Fatal("did not find counter for path")
	}

	if !c.HasLabelWithValue(label, value) {
		t.Fatalf("counter does not have label with value %s - %#v", value, c)
	}

	if c.HasLabelWithValue(label, "unknown") {
		t.Fatal("counter has label with value unknown")
	}
}

func TestTimer(t *testing.T) {
	path := "testing.label"
	local := NewLocal()
	label := "tested"
	counter := local.GetTimerVec(path, []string{label})
	value := "true"

	counter.With(value).Timing(1)

	counters := local.GetTimingsWithLabels()
	c, ok := counters[path]
	if !ok {
		t.Fatal("did not find counter for path")
	}

	if *c.Value != 1 {
		t.Fatalf("value for counter: got %d, wanted 1", *c.Value)
	}

	// test second call results in the same value
	counters = local.GetTimingsWithLabels()
	c, ok = counters[path]
	if !ok {
		t.Fatal("did not find counter for path")
	}

	if *c.Value != 1 {
		t.Fatalf("value for counter: got %d, wanted 1", *c.Value)
	}
}

func TestFlushTimer(t *testing.T) {
	path := "testing.label"
	local := NewLocal()
	label := "tested"
	counter := local.GetTimerVec(path, []string{label})
	value := "true"

	counter.With(value).Timing(1)

	counters := local.FlushTimings()
	c, ok := counters[path]
	if !ok {
		t.Fatal("did not find counter for path")
	}

	if c != 1 {
		t.Fatalf("value for counter: got %d, wanted 1", c)
	}

	// test second call results in a reset counter
	counters = local.FlushTimings()
	c, ok = counters[path]
	if !ok {
		t.Fatal("did not find counter for path")
	}

	if c != 0 {
		t.Fatalf("value for counter: got %d, wanted 0", c)
	}
}

func TestTimerWithLabelsAndValues(t *testing.T) {
	path := "testing.label"
	local := NewLocal()
	label := "tested"
	counter := local.GetTimerVec(path, []string{label})
	value := "true"

	counter.With(value).Timing(1)

	counters := local.GetTimingsWithLabels()
	c, ok := counters[path]
	if !ok {
		t.Fatal("did not find counter for path")
	}

	if !c.HasLabelWithValue(label, value) {
		t.Fatalf("counter does not have label with value %s - %#v", value, c)
	}

	if c.HasLabelWithValue(label, "unknown") {
		t.Fatal("counter has label with value unknown")
	}
}

func TestGauge(t *testing.T) {
	path := "testing.label"
	local := NewLocal()
	label := "tested"
	counter := local.GetGaugeVec(path, []string{label})
	value := "true"

	counter.With(value).Incr(1)

	counters := local.GetCountersWithLabels()
	c, ok := counters[path]
	if !ok {
		t.Fatal("did not find counter for path")
	}

	if *c.Value != 1 {
		t.Fatalf("value for counter: got %d, wanted 1", *c.Value)
	}
}

func TestGaugeWithLabelsAndValues(t *testing.T) {
	path := "testing.label"
	local := NewLocal()
	label := "tested"
	counter := local.GetGaugeVec(path, []string{label})
	value := "true"

	counter.With(value).Incr(1)

	counters := local.GetCountersWithLabels()
	c, ok := counters[path]
	if !ok {
		t.Fatal("did not find counter for path")
	}

	if !c.HasLabelWithValue(label, value) {
		t.Fatalf("counter does not have label with value %s - %#v", value, c)
	}

	if c.HasLabelWithValue(label, "unknown") {
		t.Fatal("counter has label with value unknown")
	}
}
