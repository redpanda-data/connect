// Copyright (c) 2018 Ashley Jeffs
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

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
)

func TestGrokAllParts(t *testing.T) {
	conf := NewConfig()
	conf.Grok.Parts = []int{}
	conf.Grok.Patterns = []string{
		"%{WORD:first},%{INT:second:int}",
	}

	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})

	gSet, err := NewGrok(conf, nil, testLog, metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}

	msgIn := types.NewMessage([][]byte{
		[]byte(`foo,0`),
		[]byte(`foo,1`),
		[]byte(`foo,2`),
	})
	msgs, res := gSet.ProcessMessage(msgIn)
	if len(msgs) != 1 {
		t.Fatal("Wrong count of messages")
	}
	if res != nil {
		t.Fatal("Non-nil result")
	}

	exp := [][]byte{
		[]byte(`{"first":"foo","second":0}`),
		[]byte(`{"first":"foo","second":1}`),
		[]byte(`{"first":"foo","second":2}`),
	}
	act := msgs[0].GetAll()
	if !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong output from grok: %s != %s", act, exp)
	}
}

func TestGrok(t *testing.T) {
	tLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	type gTest struct {
		name    string
		pattern string
		input   string
		output  string
	}

	tests := []gTest{
		{
			name:    "Common apache parsing",
			pattern: "%{COMMONAPACHELOG}",
			input:   `127.0.0.1 - - [23/Apr/2014:22:58:32 +0200] "GET /index.php HTTP/1.1" 404 207`,
			output:  `{"auth":"-","bytes":"207","clientip":"127.0.0.1","httpversion":"1.1","ident":"-","request":"/index.php","response":"404","timestamp":"23/Apr/2014:22:58:32 +0200","verb":"GET"}`,
		},
	}

	for _, test := range tests {
		conf := NewConfig()
		conf.Grok.Parts = []int{0}
		conf.Grok.Patterns = []string{test.pattern}

		gSet, err := NewGrok(conf, nil, tLog, tStats)
		if err != nil {
			t.Fatalf("Error for test '%v': %v", test.name, err)
		}

		inMsg := types.NewMessage(
			[][]byte{
				[]byte(test.input),
			},
		)
		msgs, _ := gSet.ProcessMessage(inMsg)
		if len(msgs) != 1 {
			t.Fatalf("Test '%v' did not succeed", test.name)
		}

		if exp, act := test.output, string(msgs[0].GetAll()[0]); exp != act {
			t.Errorf("Wrong result '%v': %v != %v", test.name, act, exp)
		}
	}
}
