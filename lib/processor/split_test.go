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

func TestSplitParts(t *testing.T) {
	conf := NewConfig()

	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	proc, err := NewSplit(conf, nil, testLog, metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	tests := [][][]byte{
		{},
		{
			[]byte("foo"),
		},
		{
			[]byte("foo"),
			[]byte("bar"),
		},
		{
			[]byte("foo"),
			[]byte("bar"),
			[]byte("baz"),
		},
	}

	for _, tIn := range tests {
		msgs, _ := proc.ProcessMessage(types.NewMessage(tIn))
		if exp, act := len(tIn), len(msgs); exp != act {
			t.Errorf("Wrong count of messages: %v != %v", act, exp)
			continue
		}
		for i, exp := range tIn {
			if act := msgs[i].GetAll()[0]; !reflect.DeepEqual(exp, act) {
				t.Errorf("Wrong contents: %s != %s", act, exp)
			}
		}
	}
}
