// Copyright (c) 2019 Ashley Jeffs
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
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestSyslogCases(t *testing.T) {
	type testCase struct {
		name   string
		input  string
		output string
	}
	tests := []testCase{
		{
			name:   "basic 1",
			input:  `<42>4 2049-10-11T22:14:15.003Z toaster.smarthome myapp - 2 [home01 device_id="43"] failed to make a toast.`,
			output: `{"appname":"myapp","hostname":"toaster.smarthome","message":"failed to make a toast.","msgid":"2","structureddata":{"home01":{"device_id":"43"}},"timestamp":"2049-10-11T22:14:15.003Z"}`,
		},
	}

	conf := NewConfig()
	proc, err := NewSyslog(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			msgsOut, res := proc.ProcessMessage(message.New([][]byte{[]byte(test.input)}))
			if res != nil {
				tt.Fatal(res.Error())
			}
			if len(msgsOut) != 1 {
				tt.Fatalf("Wrong count of result messages: %v != 1", len(msgsOut))
			}
			if exp, act := test.output, string(msgsOut[0].Get(0).Get()); exp != act {
				tt.Errorf("Wrong result: %v != %v", act, exp)
			}
		})
	}
}
