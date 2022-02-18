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

	expTimings := map[string]int64{
		"timerone": 13,
		"timertwo{label3=\"value4\",label4=\"value5\"}": 13,
	}
	assert.Equal(t, expTimings, nm.GetTimings())
	// Check twice to ensure we didn't flush
	assert.Equal(t, expTimings, nm.GetTimings())

	assert.Equal(t, expTimings, nm.FlushTimings())
	expTimings = map[string]int64{
		"timerone": 0,
		"timertwo{label3=\"value4\",label4=\"value5\"}": 0,
	}
	assert.Equal(t, expTimings, nm.GetTimings())
}
