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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-mangos/mangos"
	"github.com/go-mangos/mangos/protocol/pull"
	"github.com/go-mangos/mangos/protocol/rep"
	"github.com/go-mangos/mangos/transport/tcp"

	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
)

//------------------------------------------------------------------------------

func TestScaleProtoBasic(t *testing.T) {
	nTestLoops := 1000

	sendChan := make(chan types.Message)

	conf := NewConfig()
	conf.ScaleProto.Addresses = []string{"tcp://localhost:1324"}
	conf.ScaleProto.Bind = true
	conf.ScaleProto.PollTimeoutMS = 100
	conf.ScaleProto.RepTimeoutMS = 100
	conf.ScaleProto.SocketType = "PUSH"

	s, err := NewScaleProto(conf, log.NewLogger(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	defer func() {
		s.CloseAsync()
		if err = s.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	if err = s.StartReceiving(sendChan); err != nil {
		t.Error(err)
		return
	}

	socket, err := pull.NewSocket()
	if err != nil {
		t.Error(err)
		return
	}
	defer socket.Close()

	socket.AddTransport(tcp.NewTransport())
	socket.SetOption(mangos.OptionRecvDeadline, time.Second)

	if err = socket.Dial("tcp://localhost:1324"); err != nil {
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

		data, err := socket.Recv()
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

func TestScaleProtoMultiServer(t *testing.T) {
	nTestLoops := 1000

	sendChan := make(chan types.Message)

	conf := NewConfig()
	conf.ScaleProto.Addresses = []string{"tcp://localhost:1324", "tcp://localhost:1325"}
	conf.ScaleProto.Bind = true
	conf.ScaleProto.PollTimeoutMS = 100
	conf.ScaleProto.RepTimeoutMS = 100
	conf.ScaleProto.SocketType = "REQ"

	s, err := NewScaleProto(conf, log.NewLogger(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	defer func() {
		s.CloseAsync()
		if err = s.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	if err = s.StartReceiving(sendChan); err != nil {
		t.Error(err)
		return
	}

	socket, err := rep.NewSocket()
	if err != nil {
		t.Error(err)
		return
	}
	defer socket.Close()

	socket.AddTransport(tcp.NewTransport())
	socket.SetOption(mangos.OptionRecvDeadline, time.Second)

	if err = socket.Dial("tcp://localhost:1324"); err != nil {
		t.Error(err)
		return
	}

	socket2, err := rep.NewSocket()
	if err != nil {
		t.Error(err)
		return
	}
	defer socket2.Close()

	socket2.AddTransport(tcp.NewTransport())
	socket2.SetOption(mangos.OptionRecvDeadline, time.Second)

	if err = socket2.Dial("tcp://localhost:1325"); err != nil {
		t.Error(err)
		return
	}

	wg := sync.WaitGroup{}
	wg.Add(2)

	var s1Msgs, s2Msgs uint32
	go func() {
		defer wg.Done()
		for {
			if _, err := socket.Recv(); err != nil {
				if err != mangos.ErrClosed {
					t.Error(err)
				}
				return
			} else if err := socket.Send([]byte("SUCCESS")); err != nil {
				t.Error(err)
			}
			atomic.AddUint32(&s1Msgs, 1)
		}
	}()
	go func() {
		defer wg.Done()
		for {
			if _, err := socket2.Recv(); err != nil {
				if err != mangos.ErrClosed {
					t.Error(err)
				}
				return
			} else if err := socket2.Send([]byte("SUCCESS")); err != nil {
				t.Error(err)
			}
			atomic.AddUint32(&s2Msgs, 1)
		}
	}()

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		testMsg := types.Message{Parts: [][]byte{[]byte(testStr)}}

		select {
		case sendChan <- testMsg:
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
			return
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

	socket.Close()
	socket2.Close()

	wg.Wait()

	if exp, act := s1Msgs+s2Msgs, uint32(nTestLoops); exp != act {
		t.Errorf("Wrong count of test loops: %v != %v", act, exp)
	}
	if s1Msgs == 0 {
		t.Error("No messages for socket 1")
	}
	if s2Msgs == 0 {
		t.Error("No messages for socket 2")
	}
}

func TestScaleProtoReqRep(t *testing.T) {
	nTestLoops := 1000

	sendChan := make(chan types.Message)

	conf := NewConfig()
	conf.ScaleProto.Addresses = []string{"tcp://localhost:1324"}
	conf.ScaleProto.SuccessStr = "FOO_SUCCESS"
	conf.ScaleProto.RepTimeoutMS = 100
	conf.ScaleProto.PollTimeoutMS = 100
	conf.ScaleProto.Bind = true
	conf.ScaleProto.SocketType = "REQ"

	s, err := NewScaleProto(conf, log.NewLogger(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	defer func() {
		s.CloseAsync()
		if err = s.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	if err = s.StartReceiving(sendChan); err != nil {
		t.Error(err)
		return
	}

	socket, err := rep.NewSocket()
	if err != nil {
		t.Error(err)
		return
	}
	defer socket.Close()

	socket.AddTransport(tcp.NewTransport())
	socket.SetOption(mangos.OptionRecvDeadline, time.Second)

	if err = socket.Dial("tcp://localhost:1324"); err != nil {
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

		data, err := socket.Recv()
		if err != nil {
			t.Error(err)
			return
		}
		if res := string(data); res != testStr {
			t.Errorf("Wrong value on output: %v != %v", res, testStr)
		}

		if err = socket.Send([]byte("FOO_SUCCESS")); err != nil {
			t.Error(err)
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

	select {
	case sendChan <- types.Message{Parts: [][]byte{[]byte("foo")}}:
	case <-time.After(time.Second):
		t.Errorf("Action timed out")
		return
	}

	data, err := socket.Recv()
	if err != nil {
		t.Error(err)
		return
	}
	if res := string(data); res != "foo" {
		t.Errorf("Wrong value on output: %v != %v", res, "foo")
	}

	if err = socket.Send([]byte("FOO_ERROR")); err != nil {
		t.Error(err)
	}

	select {
	case res := <-s.ResponseChan():
		if res.Error() != ErrBadReply {
			t.Error(res.Error())
			return
		}
	case <-time.After(time.Second):
		t.Errorf("Action timed out")
		return
	}

	// Send message again but do not response, we want to ensure the output can
	// still close down gracefully.
	select {
	case sendChan <- types.Message{Parts: [][]byte{[]byte("foo")}}:
	case <-time.After(time.Second):
		t.Errorf("Action timed out")
		return
	}

	data, err = socket.Recv()
	if err != nil {
		t.Error(err)
		return
	}
	if res := string(data); res != "foo" {
		t.Errorf("Wrong value on output: %v != %v", res, "foo")
	}
}

//------------------------------------------------------------------------------
