// +build ZMQ4

/*
Copyright (c) 2014 Ashley Jeffs

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package input

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
	"github.com/pebbe/zmq4"
)

func TestZMQ4Basic(t *testing.T) {
	nTestLoops := 1000

	conf := NewConfig()
	conf.ZMQ4.Addresses = []string{"tcp://*:1234"}
	conf.ZMQ4.Bind = true
	conf.ZMQ4.SocketType = "PULL"
	conf.ZMQ4.PollTimeoutMS = 1000

	z, err := NewZMQ4(conf, log.NewLogger(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	resChan := make(chan types.Response)

	if err = z.StartListening(resChan); err != nil {
		t.Error(err)
		return
	}

	defer z.CloseAsync()
	defer z.WaitForClose(time.Second)

	ctx, err := zmq4.NewContext()
	if nil != err {
		t.Error(err)
		return
	}

	socket, err := ctx.NewSocket(zmq4.PUSH)
	if nil != err {
		t.Error(err)
		return
	}

	if err = socket.Connect("tcp://localhost:1234"); err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		if _, err = socket.Send(testStr, 0); err != nil {
			t.Error(err)
			return
		}
		select {
		case resMsg := <-z.MessageChan():
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
}

func TestZMQ4PubSub(t *testing.T) {
	nTestLoops := 1000

	conf := NewConfig()
	conf.ZMQ4.Addresses = []string{"tcp://*:1236"}
	conf.ZMQ4.Bind = true
	conf.ZMQ4.SocketType = "SUB"
	conf.ZMQ4.SubFilters = []string{"testTopic"}
	conf.ZMQ4.PollTimeoutMS = 1000

	z, err := NewZMQ4(conf, log.NewLogger(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	resChan := make(chan types.Response)

	if err = z.StartListening(resChan); err != nil {
		t.Error(err)
		return
	}

	defer z.CloseAsync()
	defer z.WaitForClose(time.Second)

	ctx, err := zmq4.NewContext()
	if nil != err {
		t.Error(err)
		return
	}

	socket, err := ctx.NewSocket(zmq4.PUB)
	if nil != err {
		t.Error(err)
		return
	}

	if err = socket.Connect("tcp://localhost:1236"); err != nil {
		t.Error(err)
		return
	}

	<-time.After(time.Second)

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		if _, err = socket.Send("testTopic", zmq4.SNDMORE); err != nil {
			t.Error(err)
			return
		}
		if _, err = socket.Send(testStr, 0); err != nil {
			t.Error(err)
			return
		}
		if _, err = socket.Send("DO_NOT_WANT", zmq4.SNDMORE); err != nil {
			t.Error(err)
			return
		}
		if _, err = socket.Send("DONT WANT THIS", 0); err != nil {
			t.Error(err)
			return
		}
		select {
		case resMsg := <-z.MessageChan():
			if len(resMsg.Parts) != 2 {
				t.Errorf("Wrong # parts: %v != 2", len(resMsg.Parts))
				return
			}
			if string(resMsg.Parts[0]) != "testTopic" {
				t.Errorf("Wrong topic: %s != testTopic", resMsg.Parts[0])
				return
			}
			if res := string(resMsg.Parts[1]); res != testStr {
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
