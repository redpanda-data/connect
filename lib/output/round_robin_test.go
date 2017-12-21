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

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
	"github.com/go-mangos/mangos/protocol/pull"
	"github.com/go-mangos/mangos/transport/tcp"
)

func TestRoundRobinWithScaleProto(t *testing.T) {
	nTestLoops := 1000

	conf := NewConfig()

	scaleOne, scaleTwo := NewConfig(), NewConfig()
	scaleOne.Type, scaleTwo.Type = "scalability_protocols", "scalability_protocols"
	scaleOne.ScaleProto.Bind, scaleTwo.ScaleProto.Bind = true, true
	scaleOne.ScaleProto.Addresses = []string{"tcp://localhost:1245"}
	scaleTwo.ScaleProto.Addresses = []string{"tcp://localhost:1246"}
	scaleOne.ScaleProto.SocketType, scaleTwo.ScaleProto.SocketType = "PUSH", "PUSH"

	conf.RoundRobin.Outputs = append(conf.RoundRobin.Outputs, scaleOne)
	conf.RoundRobin.Outputs = append(conf.RoundRobin.Outputs, scaleTwo)

	s, err := NewRoundRobin(conf, log.NewLogger(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	sendChan := make(chan types.Message)

	if err = s.StartReceiving(sendChan); err != nil {
		t.Error(err)
		return
	}

	defer func() {
		s.CloseAsync()
		if err := s.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

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

	if err = socketOne.Dial("tcp://localhost:1245"); err != nil {
		t.Error(err)
		return
	}
	if err = socketTwo.Dial("tcp://localhost:1246"); err != nil {
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

		if i%2 == 0 {
			data, err := socketOne.Recv()
			if err != nil {
				t.Error(err)
				return
			}
			if res := string(data); res != testStr {
				t.Errorf("Wrong value on output: %v != %v", res, testStr)
			}
		} else {
			data, err := socketTwo.Recv()
			if err != nil {
				t.Error(err)
				return
			}
			if res := string(data); res != testStr {
				t.Errorf("Wrong value on output: %v != %v", res, testStr)
			}
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
