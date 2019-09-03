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

package writer

import (
	"fmt"
	"testing"
	"time"

	"nanomsg.org/go-mangos"
	"nanomsg.org/go-mangos/protocol/pull"
	"nanomsg.org/go-mangos/transport/tcp"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

//------------------------------------------------------------------------------

func TestNanomsgBasic(t *testing.T) {
	nTestLoops := 1000

	conf := NewNanomsgConfig()
	conf.URLs = []string{"tcp://localhost:1324"}
	conf.Bind = true
	conf.PollTimeout = "100ms"
	conf.SocketType = "PUSH"

	s, err := NewNanomsg(conf, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	if err = s.Connect(); err != nil {
		t.Fatal(err)
	}

	defer func() {
		s.CloseAsync()
		if err = s.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	socket, err := pull.NewSocket()
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()

	socket.AddTransport(tcp.NewTransport())
	socket.SetOption(mangos.OptionRecvDeadline, time.Second)

	if err = socket.Dial("tcp://localhost:1324"); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		testMsg := message.New([][]byte{[]byte(testStr)})

		go func() {
			if serr := s.Write(testMsg); serr != nil {
				t.Error(serr)
			}
		}()

		data, err := socket.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if res := string(data); res != testStr {
			t.Errorf("Wrong value on output: %v != %v", res, testStr)
		}
	}
}

//------------------------------------------------------------------------------
