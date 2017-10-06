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
	"bytes"
	"io"
	"os"
	"testing"
	"time"

	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
)

func getTestSTDIN(handle io.ReadWriter, conf Config) *STDIN {
	s := STDIN{
		running:          1,
		handle:           handle,
		conf:             conf,
		log:              log.NewLogger(os.Stdout, logConfig),
		stats:            metrics.DudType{},
		internalMessages: make(chan [][]byte),
		messages:         make(chan types.Message),
		responses:        nil,
		closeChan:        make(chan struct{}),
		closedChan:       make(chan struct{}),
	}

	go s.readLoop()

	return &s
}

func TestSTDINClose(t *testing.T) {
	s, err := NewSTDIN(NewConfig(), log.NewLogger(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	c := make(chan types.Response)
	if err := s.StartListening(c); err != nil {
		t.Error(err)
	}
	if err := s.StartListening(c); err == nil {
		t.Error("Expected error from second listener")
	}

	s.CloseAsync()
	if err := s.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestSTDINSinglePart(t *testing.T) {
	messages := []string{
		"first message",
		"second message",
		"third message",
	}

	var handle bytes.Buffer

	for _, msg := range messages {
		handle.Write([]byte(msg))
		handle.Write([]byte("\n"))
		handle.Write([]byte("\n")) // Try some empty messages
	}

	f := getTestSTDIN(&handle, NewConfig())

	defer func() {
		f.CloseAsync()
		if err := f.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	resChan := make(chan types.Response)

	if err := f.StartListening(resChan); err != nil {
		t.Error(err)
		return
	}

	for _, msg := range messages {
		select {
		case resMsg, open := <-f.MessageChan():
			if !open {
				t.Error("channel closed early")
			} else if res := string(resMsg.Parts[0]); res != msg {
				t.Errorf("Wrong result, %v != %v", res, msg)
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

	select {
	case _, open := <-f.MessageChan():
		if open {
			t.Error("Channel not closed at end of messages")
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for channel close")
	}
}

func TestSTDINMultiPart(t *testing.T) {
	var handle bytes.Buffer

	messages := [][]string{
		{
			"first message",
			"1",
			"2",
		},
		{
			"second message",
			"1",
			"2",
		},
		{
			"third message",
			"1",
			"2",
		},
	}

	for _, msg := range messages {
		for _, part := range msg {
			handle.Write([]byte(part))
			handle.Write([]byte("\n"))
		}
		handle.Write([]byte("\n"))
	}

	conf := NewConfig()
	conf.STDIN.Multipart = true

	f := getTestSTDIN(&handle, conf)

	defer func() {
		f.CloseAsync()
		if err := f.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	resChan := make(chan types.Response)

	if err := f.StartListening(resChan); err != nil {
		t.Error(err)
		return
	}

	for _, msg := range messages {
		select {
		case resMsg, open := <-f.MessageChan():
			if !open {
				t.Error("channel closed early")
			} else {
				for i, part := range msg {
					if res := string(resMsg.Parts[i]); res != part {
						t.Errorf("Wrong result, %v != %v", res, part)
					}
				}
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

	select {
	case _, open := <-f.MessageChan():
		if open {
			t.Error("Channel not closed at end of messages")
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for channel close")
	}
}
