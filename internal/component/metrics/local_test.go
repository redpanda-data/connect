package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCounter(t *testing.T) {
	nm := NewLocal()

	ctr := nm.GetCounter("counterone")
	ctr.Incr(10)
	ctr.Incr(11)

	gge := nm.GetGauge("gaugeone")
	gge.Set(12)

	tmr := nm.GetTimer("timerone")
	tmr.Timing(13)

	ctrTwo := nm.GetCounterVec("countertwo", "label1")
	ctrTwo.With("value1").Incr(10)
	ctrTwo.With("value2").Incr(11)

	ggeTwo := nm.GetGaugeVec("gaugetwo", "label2")
	ggeTwo.With("value3").Set(12)

	tmrTwo := nm.GetTimerVec("timertwo", "label3", "label4")
	tmrTwo.With("value4", "value5").Timing(13)

	ggeTwo.With("value6").Set(24)

	expCounters := map[string]int64{
		"counterone":                    21,
		"countertwo{label1=\"value1\"}": 10,
		"countertwo{label1=\"value2\"}": 11,
		"gaugeone":                      12,
		"gaugetwo{label2=\"value3\"}":   12,
		"gaugetwo{label2=\"value6\"}":   24,
	}
	assert.Equal(t, expCounters, nm.GetCounters())
	// Check twice to ensure we didn't flush
	assert.Equal(t, expCounters, nm.GetCounters())

	assert.Equal(t, expCounters, nm.FlushCounters())
	expCounters = map[string]int64{
		"counterone":                    0,
		"countertwo{label1=\"value1\"}": 0,
		"countertwo{label1=\"value2\"}": 0,
		"gaugeone":                      0,
		"gaugetwo{label2=\"value3\"}":   0,
		"gaugetwo{label2=\"value6\"}":   0,
	}
	assert.Equal(t, expCounters, nm.GetCounters())

	expTimingAvgs := map[string]float64{
		"timerone": 13,
		"timertwo{label3=\"value4\",label4=\"value5\"}": 13,
	}
	actTimingAvgs := map[string]float64{}
	for k, v := range nm.GetTimings() {
		actTimingAvgs[k] = v.Mean()
	}

	assert.Equal(t, expTimingAvgs, actTimingAvgs)

	// Check twice to ensure we didn't flush
	actTimingAvgs = map[string]float64{}
	for k, v := range nm.GetTimings() {
		actTimingAvgs[k] = v.Mean()
	}
	assert.Equal(t, expTimingAvgs, actTimingAvgs)

	actTimingAvgs = map[string]float64{}
	for k, v := range nm.FlushTimings() {
		actTimingAvgs[k] = v.Mean()
	}
	assert.Equal(t, expTimingAvgs, actTimingAvgs)

	expTimingAvgs = map[string]float64{
		"timerone": 0,
		"timertwo{label3=\"value4\",label4=\"value5\"}": 0,
	}

	actTimingAvgs = map[string]float64{}
	for k, v := range nm.GetTimings() {
		actTimingAvgs[k] = v.Mean()
	}
	assert.Equal(t, expTimingAvgs, actTimingAvgs)
}

func TestReverseName(t *testing.T) {
	tests := map[string]struct {
		input     string
		name      string
		tagNames  []string
		tagValues []string
	}{
		"no labels": {
			input: "hello world",
			name:  "hello world",
		},
		"single label": {
			input:     `hello world{foo="bar"}`,
			name:      "hello world",
			tagNames:  []string{"foo"},
			tagValues: []string{"bar"},
		},
		"empty label": {
			input:     `hello world{foo=""}`,
			name:      "hello world",
			tagNames:  []string{"foo"},
			tagValues: []string{""},
		},
		"multiple labels": {
			input:     `hello world{foo="first",bar="second",baz="third"}`,
			name:      "hello world",
			tagNames:  []string{"foo", "bar", "baz"},
			tagValues: []string{"first", "second", "third"},
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			res, tagNames, tagValues := ReverseLabelledPath(test.input)
			assert.Equal(t, test.name, res)
			assert.Equal(t, test.tagNames, tagNames)
			assert.Equal(t, test.tagValues, tagValues)
		})
	}
}
