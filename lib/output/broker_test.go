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

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"nanomsg.org/go-mangos/protocol/pull"
	"nanomsg.org/go-mangos/transport/tcp"
)

func TestBrokerWithScaleProto(t *testing.T) {
	nTestLoops := 1000

	conf := NewConfig()

	scaleOne, scaleTwo := NewConfig(), NewConfig()
	scaleOne.Type, scaleTwo.Type = "scalability_protocols", "scalability_protocols"
	scaleOne.ScaleProto.Bind, scaleTwo.ScaleProto.Bind = true, true
	scaleOne.ScaleProto.URLs = []string{"tcp://localhost:1241"}
	scaleTwo.ScaleProto.URLs = []string{"tcp://localhost:1242"}
	scaleOne.ScaleProto.SocketType, scaleTwo.ScaleProto.SocketType = "PUSH", "PUSH"

	conf.Broker.Outputs = append(conf.Broker.Outputs, scaleOne)
	conf.Broker.Outputs = append(conf.Broker.Outputs, scaleTwo)

	s, err := NewBroker(conf, nil, log.New(os.Stdout, logConfig), metrics.DudType{})
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

	sendChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	if err = s.Consume(sendChan); err != nil {
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
		testMsg := types.NewMessage([][]byte{[]byte(testStr)})

		select {
		case sendChan <- types.NewTransaction(testMsg, resChan):
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
		case res := <-resChan:
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

func TestRoundRobinWithScaleProto(t *testing.T) {
	nTestLoops := 1000

	conf := NewConfig()
	conf.Broker.Pattern = "round_robin"

	scaleOne, scaleTwo := NewConfig(), NewConfig()
	scaleOne.Type, scaleTwo.Type = "scalability_protocols", "scalability_protocols"
	scaleOne.ScaleProto.Bind, scaleTwo.ScaleProto.Bind = true, true
	scaleOne.ScaleProto.URLs = []string{"tcp://localhost:1245"}
	scaleTwo.ScaleProto.URLs = []string{"tcp://localhost:1246"}
	scaleOne.ScaleProto.SocketType, scaleTwo.ScaleProto.SocketType = "PUSH", "PUSH"

	conf.Broker.Outputs = append(conf.Broker.Outputs, scaleOne)
	conf.Broker.Outputs = append(conf.Broker.Outputs, scaleTwo)

	s, err := NewBroker(conf, nil, log.New(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	sendChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	if err = s.Consume(sendChan); err != nil {
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
		testMsg := types.NewMessage([][]byte{[]byte(testStr)})

		select {
		case sendChan <- types.NewTransaction(testMsg, resChan):
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
		case res := <-resChan:
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
