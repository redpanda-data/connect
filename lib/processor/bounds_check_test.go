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
	"testing"

	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/benthos/lib/util/service/log"
	"github.com/jeffail/benthos/lib/util/service/metrics"
)

func TestBoundsCheck(t *testing.T) {
	conf := NewConfig()
	conf.BoundsCheck.MinParts = 2
	conf.BoundsCheck.MaxParts = 3
	conf.BoundsCheck.MaxPartSize = 10

	testLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})
	proc, err := NewBoundsCheck(conf, testLog, metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	goodParts := [][][]byte{
		{
			[]byte("hello"),
			[]byte("world"),
		},
		{
			[]byte("helloworld"),
			[]byte("helloworld"),
		},
		{
			[]byte("hello"),
			[]byte("world"),
			[]byte("!"),
		},
		{
			[]byte("helloworld"),
			[]byte("helloworld"),
			[]byte("helloworld"),
		},
	}

	badParts := [][][]byte{
		{
			[]byte("hello world"),
		},
		{
			[]byte("hello world"),
			[]byte("hello world this exceeds max part size"),
		},
		{
			[]byte("hello"),
			[]byte("world"),
			[]byte("this"),
			[]byte("exceeds"),
			[]byte("max"),
			[]byte("num"),
			[]byte("parts"),
		},
	}

	for _, parts := range goodParts {
		msg := &types.Message{Parts: parts}
		if res, _, check := proc.ProcessMessage(msg); !check {
			t.Errorf("Bounds check failed on: %s", parts)
		} else if res != msg {
			t.Error("Wrong message returned (expected same)")
		}
	}

	for _, parts := range badParts {
		if _, res, check := proc.ProcessMessage(&types.Message{Parts: parts}); check {
			t.Errorf("Bounds check didnt fail on: %s", parts)
		} else if _, ok := res.(types.SimpleResponse); !ok {
			t.Error("Expected simple response from bad message")
		}
	}
}
