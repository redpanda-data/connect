// Copyright (c) 2017 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package processor

import (
	"os"
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestSample10Percent(t *testing.T) {
	conf := NewConfig()
	conf.Sample.Retain = 10.0

	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	proc, err := NewSample(conf, nil, testLog, metrics.DudType{})
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

	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	proc, err := NewSample(conf, nil, testLog, metrics.DudType{})
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
