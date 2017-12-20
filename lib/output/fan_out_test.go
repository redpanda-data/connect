// Copyright (c) 2014 Ashley Jeffs
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

package output

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-mangos/mangos/protocol/pull"
	"github.com/go-mangos/mangos/transport/tcp"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

func TestFanOutWithScaleProto(t *testing.T) {
	nTestLoops := 1000

	conf := NewConfig()

	scaleOne, scaleTwo := NewConfig(), NewConfig()
	scaleOne.Type, scaleTwo.Type = "scalability_protocols", "scalability_protocols"
	scaleOne.ScaleProto.Bind, scaleTwo.ScaleProto.Bind = true, true
	scaleOne.ScaleProto.Addresses = []string{"tcp://localhost:1241"}
	scaleTwo.ScaleProto.Addresses = []string{"tcp://localhost:1242"}
	scaleOne.ScaleProto.SocketType, scaleTwo.ScaleProto.SocketType = "PUSH", "PUSH"

	conf.FanOut.Outputs = append(conf.FanOut.Outputs, scaleOne)
	conf.FanOut.Outputs = append(conf.FanOut.Outputs, scaleTwo)

	s, err := NewFanOut(conf, log.NewLogger(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	defer func() {
		s.CloseAsync()
		if err := s.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	sendChan := make(chan types.Message)

	if err = s.StartReceiving(sendChan); err != nil {
		t.Error(err)
		return
	}

	socketOne, err := pull.NewSocket()
	if err != nil {
		t.Error(err)
		return
	}
	socketTwo, err := pull.NewSocket()
	if err != nil {
		t.Error(err)
		return
	}

	socketOne.AddTransport(tcp.NewTransport())
	socketTwo.AddTransport(tcp.NewTransport())

	if err = socketOne.Dial("tcp://localhost:1241"); err != nil {
		t.Error(err)
		return
	}
	if err = socketTwo.Dial("tcp://localhost:1242"); err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		testMsg := types.Message{Parts: [][]byte{[]byte(testStr)}}

		select {
		case sendChan <- testMsg:
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
			return
		}

		data, err := socketOne.Recv()
		if err != nil {
			t.Error(err)
			return
		}
		if res := string(data); res != testStr {
			t.Errorf("Wrong value on output: %v != %v", res, testStr)
		}

		data, err = socketTwo.Recv()
		if err != nil {
			t.Error(err)
			return
		}
		if res := string(data); res != testStr {
			t.Errorf("Wrong value on output: %v != %v", res, testStr)
		}

		select {
		case res := <-s.ResponseChan():
			if res.Error() != nil {
				t.Error(res.Error())
				return
			}
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
			return
		}
	}
}
