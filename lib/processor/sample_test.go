package processor

import (
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestSample10Percent(t *testing.T) {
	conf := NewConfig()
	conf.Sample.Retain = 10.0

	testLog := log.Noop()
	proc, err := NewSample(conf, nil, testLog, metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	total := 100000
	totalSampled := 0
	margin := 0.01
	for i := 0; i < total; i++ {
		msgIn := message.New(nil)
		msgs, _ := proc.ProcessMessage(msgIn)
		if len(msgs) > 0 {
			if !reflect.DeepEqual(msgIn, msgs[0]) {
				t.Error("Message told to propagate but not given")
			}
			totalSampled++
		}
	}

	act, exp := (float64(totalSampled)/float64(total))*100.0, conf.Sample.Retain
	var sampleError float64
	if exp > act {
		sampleError = (exp - act) / exp
	} else {
		sampleError = (act - exp) / exp
	}
	if sampleError > margin {
		t.Errorf("Sample error greater than margin: %v != %v", act, exp)
	}
}

func TestSample24Percent(t *testing.T) {
	conf := NewConfig()
	conf.Sample.Retain = 24.0

	testLog := log.Noop()
	proc, err := NewSample(conf, nil, testLog, metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	total := 100000
	totalSampled := 0
	margin := 0.01
	for i := 0; i < total; i++ {
		msgIn := message.New(nil)
		msgs, _ := proc.ProcessMessage(msgIn)
		if len(msgs) == 1 {
			if !reflect.DeepEqual(msgIn, msgs[0]) {
				t.Error("Message told to propagate but not given")
			}
			totalSampled++
		}
	}

	act, exp := (float64(totalSampled)/float64(total))*100.0, conf.Sample.Retain
	var sampleError float64
	if exp > act {
		sampleError = (exp - act) / exp
	} else {
		sampleError = (act - exp) / exp
	}
	if sampleError > margin {
		t.Errorf("Sample error greater than margin: %v != %v", act, exp)
	}
}
