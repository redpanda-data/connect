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

package reader

import (
	"fmt"
	"os"
	"testing"
	"time"

	"nanomsg.org/go-mangos/protocol/pub"
	"nanomsg.org/go-mangos/protocol/push"
	"nanomsg.org/go-mangos/transport/tcp"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

func TestScaleProtoBasic(t *testing.T) {
	nTestLoops := 1000

	conf := NewScaleProtoConfig()
	conf.URLs = []string{"tcp://localhost:1238", "tcp://localhost:1239"}
	conf.Bind = true
	conf.SocketType = "PULL"
	conf.PollTimeout = "100ms"

	s, err := NewScaleProto(conf, log.New(os.Stdout, log.Config{LogLevel: "NONE"}), metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}

	if err = s.Connect(); err != nil {
		t.Fatal(err)
	}

	defer func() {
		s.CloseAsync()
		if err := s.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	socket, err := push.NewSocket()
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()

	socket.AddTransport(tcp.NewTransport())
	if err = socket.Dial("tcp://localhost:1238"); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		go func() {
			if sockErr := socket.Send([]byte(testStr)); sockErr != nil {
				t.Fatal(sockErr)
			}
		}()

		var resMsg types.Message
		resMsg, err = s.Read()
		if err != nil {
			t.Fatal(err)
		}
		if exp, act := testStr, string(resMsg.Get(0).Get()); exp != act {
			t.Errorf("Wrong result, %v != %v", act, exp)
		}

		if err = s.Acknowledge(nil); err != nil {
			t.Error(err)
		}
	}

	socket2, err := push.NewSocket()
	if err != nil {
		t.Fatal(err)
	}
	defer socket2.Close()

	socket2.AddTransport(tcp.NewTransport())
	if err = socket2.Dial("tcp://localhost:1239"); err != nil {
		t.Fatal(err)
	}

	go func() {
		if sockErr := socket2.Send([]byte("second sock")); sockErr != nil {
			t.Fatal(err)
		}
	}()

	var resMsg types.Message
	resMsg, err = s.Read()
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := "second sock", string(resMsg.Get(0).Get()); exp != act {
		t.Errorf("Wrong result, %v != %v", act, exp)
	}

	if err = s.Acknowledge(nil); err != nil {
		t.Error(err)
	}
}

func TestScaleProtoPubSub(t *testing.T) {
	nTestLoops := 1000

	conf := NewScaleProtoConfig()
	conf.URLs = []string{"tcp://localhost:1250"}
	conf.Bind = true
	conf.SocketType = "SUB"
	conf.SubFilters = []string{"testTopic"}
	conf.PollTimeout = "100ms"

	s, err := NewScaleProto(conf, log.New(os.Stdout, log.Config{LogLevel: "NONE"}), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	if err = s.Connect(); err != nil {
		t.Fatal(err)
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

	if err = socket.Dial("tcp://localhost:1250"); err != nil {
		t.Error(err)
		return
	}

	<-time.After(time.Millisecond * 200)

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		go func() {
			if err := socket.Send([]byte("testTopic" + testStr)); err != nil {
				t.Error(err)
				return
			}
		}()
		go func() {
			if err := socket.Send([]byte("DO_NOT_WANT")); err != nil {
				t.Error(err)
				return
			}
		}()

		var resMsg types.Message
		if resMsg, err = s.Read(); err != nil {
			t.Fatal(err)
		}
		if res := string(resMsg.Get(0).Get()[9:]); res != testStr {
			t.Errorf("Wrong result, %v != %v", res, testStr)
		}

		if err = s.Acknowledge(nil); err != nil {
			t.Error(err)
		}
	}
}
