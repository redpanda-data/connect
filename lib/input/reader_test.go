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
	"bufio"
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/benthos/lib/util/service/log"
	"github.com/jeffail/benthos/lib/util/service/metrics"
)

func TestReaderSinglePart(t *testing.T) {
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

	f, err := newReader(
		&handle,
		bufio.MaxScanTokenSize,
		false,
		[]byte("\n"),
		log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		f.CloseAsync()
		if err := f.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	resChan := make(chan types.Response)

	if err = f.StartListening(resChan); err != nil {
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

func TestReaderSinglePartCustomDelim(t *testing.T) {
	messages := []string{
		"first message",
		"second message",
		"third message",
	}

	var handle bytes.Buffer

	for _, msg := range messages {
		handle.Write([]byte(msg))
		handle.Write([]byte("<FOO>"))
		handle.Write([]byte("<FOO>")) // Try some empty messages
	}

	f, err := newReader(
		&handle,
		bufio.MaxScanTokenSize,
		false,
		[]byte("<FOO>"),
		log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		f.CloseAsync()
		if err := f.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	resChan := make(chan types.Response)

	if err = f.StartListening(resChan); err != nil {
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

func TestReaderMultiPart(t *testing.T) {
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

	f, err := newReader(
		&handle,
		bufio.MaxScanTokenSize,
		true,
		[]byte("\n"),
		log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Fatal(err)
	}

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

func TestReaderMultiPartCustomDelim(t *testing.T) {
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
			handle.Write([]byte("<FOO>"))
		}
		handle.Write([]byte("<FOO>"))
	}

	f, err := newReader(
		&handle,
		bufio.MaxScanTokenSize,
		true,
		[]byte("<FOO>"),
		log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Fatal(err)
	}

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
