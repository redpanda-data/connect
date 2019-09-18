package metrics

import "testing"

func TestCounter(t *testing.T) {
	path := "testing.label"
	local := NewLocal()
	label := "tested"
	counter := local.GetCounterVec(path, []string{label})
	value := "true"

	counter.With(value).Incr(1)

	counters := local.GetCounters()
	c, ok := counters[path]
	if !ok {
		t.Fatal("did not find counter for path")
	}

	if *c.Value != 1 {
		t.Fatalf("value for counter: got %d, wanted 1", *c.Value)
	}
}

func TestCounterWithLabelsAndValues(t *testing.T) {
	path := "testing.label"
	local := NewLocal()
	label := "tested"
	counter := local.GetCounterVec(path, []string{label})
	value := "true"

	counter.With(value).Incr(1)

	counters := local.GetCounters()
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

	counters := local.GetTimings()
	c, ok := counters[path]
	if !ok {
		t.Fatal("did not find counter for path")
	}

	if *c.Value != 1 {
		t.Fatalf("value for counter: got %d, wanted 1", *c.Value)
	}
}

func TestTimerWithLabelsAndValues(t *testing.T) {
	path := "testing.label"
	local := NewLocal()
	label := "tested"
	counter := local.GetTimerVec(path, []string{label})
	value := "true"

	counter.With(value).Timing(1)

	counters := local.GetTimings()
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
