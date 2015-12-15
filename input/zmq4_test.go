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

	"github.com/jeffail/benthos/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
	"github.com/pebbe/zmq4"
)

var logConfig = log.LoggerConfig{
	LogLevel: "NONE",
}

func TestZMQ4Basic(t *testing.T) {
	nTestLoops := 1000

	conf := NewConfig()
	conf.ZMQ4.Addresses = []string{"tcp://*:1234"}
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
				t.Errorf("Wrong result, %v != %v", resMsg, res)
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
