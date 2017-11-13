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

package input

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-mangos/mangos/protocol/pub"
	"github.com/go-mangos/mangos/protocol/push"
	"github.com/go-mangos/mangos/protocol/req"
	"github.com/go-mangos/mangos/transport/tcp"

	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
)

func TestScaleProtoBasic(t *testing.T) {
	nTestLoops := 1000

	conf := NewConfig()
	conf.ScaleProto.Addresses = []string{"tcp://localhost:1238", "tcp://localhost:1239"}
	conf.ScaleProto.Bind = true
	conf.ScaleProto.SocketType = "PULL"
	conf.ScaleProto.PollTimeoutMS = 100

	s, err := NewScaleProto(conf, log.NewLogger(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	resChan := make(chan types.Response)

	if err = s.StartListening(resChan); err != nil {
		t.Error(err)
		return
	}

	defer func() {
		s.CloseAsync()
		if err := s.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	socket, err := push.NewSocket()
	if err != nil {
		t.Error(err)
		return
	}
	defer socket.Close()

	socket.AddTransport(tcp.NewTransport())

	if err = socket.Dial("tcp://localhost:1238"); err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		if err = socket.Send([]byte(testStr)); err != nil {
			t.Error(err)
			return
		}
		select {
		case resMsg := <-s.MessageChan():
			if res := string(resMsg.Parts[0]); res != testStr {
				t.Errorf("Wrong result, %v != %v", res, testStr)
			}
		case <-time.After(time.Second):
			t.Error("Timed out waiting for message")
		}
		select {
		case resChan <- types.NewSimpleResponse(nil):
		case <-time.After(time.Second):
			t.Error("Timed out waiting for response")
		}
	}

	socket2, err := push.NewSocket()
	if err != nil {
		t.Error(err)
		return
	}
	defer socket2.Close()

	socket2.AddTransport(tcp.NewTransport())

	if err = socket2.Dial("tcp://localhost:1239"); err != nil {
		t.Error(err)
		return
	}

	if err = socket2.Send([]byte("second sock")); err != nil {
		t.Error(err)
		return
	}
	select {
	case resMsg := <-s.MessageChan():
		if res := string(resMsg.Parts[0]); res != "second sock" {
			t.Errorf("Wrong result, %v != %v", res, "second sock")
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for message")
	}
	select {
	case resChan <- types.NewSimpleResponse(nil):
	case <-time.After(time.Second):
		t.Error("Timed out waiting for response")
	}
}

func TestScaleProtoReqRep(t *testing.T) {
	nTestLoops := 1000

	conf := NewConfig()
	conf.ScaleProto.Addresses = []string{"tcp://localhost:1238"}
	conf.ScaleProto.Bind = true
	conf.ScaleProto.SocketType = "REP"
	conf.ScaleProto.PollTimeoutMS = 100
	conf.ScaleProto.RepTimeoutMS = 100
	conf.ScaleProto.SuccessStr = "FOO_SUCCESS"
	conf.ScaleProto.ErrorStr = "FOO_ERROR"

	s, err := NewScaleProto(conf, log.NewLogger(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	resChan := make(chan types.Response)

	if err = s.StartListening(resChan); err != nil {
		t.Error(err)
		return
	}

	defer func() {
		s.CloseAsync()
		if err := s.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	socket, err := req.NewSocket()
	if err != nil {
		t.Error(err)
		return
	}
	defer socket.Close()

	socket.AddTransport(tcp.NewTransport())

	if err = socket.Dial("tcp://localhost:1238"); err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		if err = socket.Send([]byte(testStr)); err != nil {
			t.Error(err)
			return
		}
		select {
		case resMsg := <-s.MessageChan():
			if res := string(resMsg.Parts[0]); res != testStr {
				t.Errorf("Wrong result, %v != %v", res, testStr)
			}
		case <-time.After(time.Second):
			t.Error("Timed out waiting for message")
		}
		select {
		case resChan <- types.NewSimpleResponse(nil):
		case <-time.After(time.Second):
			t.Error("Timed out waiting for response")
		}
		var reply []byte
		if reply, err = socket.Recv(); err != nil {
			t.Error(err)
			return
		} else if exp, act := "FOO_SUCCESS", string(reply); exp != act {
			t.Errorf("Wrong reply: %v != %v", act, exp)
		}
	}

	if err = socket.Send([]byte("foo")); err != nil {
		t.Error(err)
		return
	}
	select {
	case resMsg := <-s.MessageChan():
		if exp, act := "foo", string(resMsg.Parts[0]); exp != act {
			t.Errorf("Wrong result, %v != %v", act, exp)
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for message")
	}
	select {
	case resChan <- types.NewSimpleResponse(errors.New("foo err")):
	case <-time.After(time.Second):
		t.Error("Timed out waiting for response")
	}
	var reply []byte
	if reply, err = socket.Recv(); err != nil {
		t.Error(err)
		return
	} else if exp, act := "FOO_ERROR", string(reply); exp != act {
		t.Errorf("Wrong reply: %v != %v", act, exp)
	}

	if err = socket.Send([]byte("foo")); err != nil {
		t.Error(err)
		return
	}
	select {
	case resMsg := <-s.MessageChan():
		if exp, act := "foo", string(resMsg.Parts[0]); exp != act {
			t.Errorf("Wrong result, %v != %v", act, exp)
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for message")
	}
	select {
	case resChan <- types.NewSimpleResponse(errors.New("foo err")):
	case <-time.After(time.Second):
		t.Error("Timed out waiting for response")
	}

	// Do not receive result from nanomsg and instead make sure the input times
	// out and closes.
}

func TestScaleProtoPubSub(t *testing.T) {
	nTestLoops := 1000

	conf := NewConfig()
	conf.ScaleProto.Addresses = []string{"tcp://localhost:1239"}
	conf.ScaleProto.Bind = true
	conf.ScaleProto.SocketType = "SUB"
	conf.ScaleProto.SubFilters = []string{"testTopic"}
	conf.ScaleProto.PollTimeoutMS = 100

	s, err := NewScaleProto(conf, log.NewLogger(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	resChan := make(chan types.Response)

	if err = s.StartListening(resChan); err != nil {
		t.Error(err)
		return
	}

	defer func() {
		s.CloseAsync()
		if err := s.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	socket, err := pub.NewSocket()
	if err != nil {
		t.Error(err)
		return
	}
	defer socket.Close()

	socket.AddTransport(tcp.NewTransport())

	if err = socket.Dial("tcp://localhost:1239"); err != nil {
		t.Error(err)
		return
	}

	<-time.After(time.Millisecond * 100)

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		if err = socket.Send([]byte("testTopic" + testStr)); err != nil {
			t.Error(err)
			return
		}
		if err = socket.Send([]byte("DO_NOT_WANT")); err != nil {
			t.Error(err)
			return
		}
		select {
		case resMsg := <-s.MessageChan():
			if res := string(resMsg.Parts[0][9:]); res != testStr {
				t.Errorf("Wrong result, %v != %v", res, testStr)
			}
		case <-time.After(time.Second):
			t.Error("Timed out waiting for message")
		}
		select {
		case resChan <- types.NewSimpleResponse(nil):
		case <-time.After(time.Second):
			t.Error("Timed out waiting for response")
		}
	}
}
