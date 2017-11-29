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

	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/benthos/lib/util/service/log"
	"github.com/jeffail/benthos/lib/util/service/metrics"
)

func TestBlobToMulti(t *testing.T) {
	conf := NewConfig()

	testLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})
	proc, err := NewBlobToMulti(conf, testLog, metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	if _, res, succeeded := proc.ProcessMessage(&types.Message{}); succeeded {
		t.Error("Expected fail on bad message")
	} else if _, ok := res.(types.SimpleResponse); !ok {
		t.Error("Expected simple response from bad message")
	}
	if _, _, res := proc.ProcessMessage(
		&types.Message{Parts: [][]byte{[]byte("wat this isnt good")}},
	); res {
		t.Error("Expected fail on bad message")
	}

	testMsg := types.Message{Parts: [][]byte{[]byte("hello"), []byte("world")}}
	testMsgBlob := testMsg.Bytes()

	if res, _, ok := proc.ProcessMessage(&types.Message{Parts: [][]byte{testMsgBlob}}); ok {
		if !reflect.DeepEqual(testMsg, *res) {
			t.Errorf("Returned message did not match: %s != %s", *res, testMsg)
		}
	} else {
		t.Error("Failed on good message")
	}
}
