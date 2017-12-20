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
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

type testBuffer struct {
	bytes.Buffer

	closed bool
}

func (t *testBuffer) Close() error {
	t.closed = true
	return nil
}

func TestWriterBasic(t *testing.T) {
	var buf testBuffer

	msgChan := make(chan types.Message)

	writer, err := newWriter(&buf, []byte{}, log.NewLogger(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	if err = writer.StartReceiving(msgChan); err != nil {
		t.Error(err)
	}
	if err = writer.StartReceiving(msgChan); err == nil {
		t.Error("Expected error from duplicate receiver call")
	}

	testCases := []struct {
		message        []string
		expectedOutput string
	}{
		{
			[]string{`hello world`},
			"hello world\n",
		},
		{
			[]string{`hello world`, `part 2`},
			"hello world\npart 2\n\n",
		},
	}

	for _, c := range testCases {
		msg := types.Message{}
		for _, part := range c.message {
			msg.Parts = append(msg.Parts, []byte(part))
		}

		select {
		case msgChan <- msg:
		case <-time.After(time.Second):
			t.Error("Timed out sending message")
		}

		select {
		case res, open := <-writer.ResponseChan():
			if !open {
				t.Error("writer closed early")
				return
			}
			if res.Error() != nil {
				t.Error(res.Error())
			}
		case <-time.After(time.Second):
			t.Error("Timed out waiting for response")
		}

		if exp, act := c.expectedOutput, buf.String(); exp != act {
			t.Errorf("Unexpected output from writer: %v != %v", exp, act)
		}
		buf.Reset()
	}

	writer.CloseAsync()
	if err = writer.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	if !buf.closed {
		t.Error("Buffer was not closed by writer")
	}
}

func TestWriterCustomDelim(t *testing.T) {
	var buf testBuffer

	msgChan := make(chan types.Message)

	writer, err := newWriter(&buf, []byte("<FOO>"), log.NewLogger(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	if err = writer.StartReceiving(msgChan); err != nil {
		t.Error(err)
	}
	if err = writer.StartReceiving(msgChan); err == nil {
		t.Error("Expected error from duplicate receiver call")
	}

	testCases := []struct {
		message        []string
		expectedOutput string
	}{
		{
			[]string{`hello world`},
			"hello world<FOO>",
		},
		{
			[]string{`hello world`, `part 2`},
			"hello world<FOO>part 2<FOO><FOO>",
		},
	}

	for _, c := range testCases {
		msg := types.Message{}
		for _, part := range c.message {
			msg.Parts = append(msg.Parts, []byte(part))
		}

		select {
		case msgChan <- msg:
		case <-time.After(time.Second):
			t.Error("Timed out sending message")
		}

		select {
		case res, open := <-writer.ResponseChan():
			if !open {
				t.Error("writer closed early")
				return
			}
			if res.Error() != nil {
				t.Error(res.Error())
			}
		case <-time.After(time.Second):
			t.Error("Timed out waiting for response")
		}

		if exp, act := c.expectedOutput, buf.String(); exp != act {
			t.Errorf("Unexpected output from writer: %v != %v", exp, act)
		}
		buf.Reset()
	}

	writer.CloseAsync()
	if err = writer.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	if !buf.closed {
		t.Error("Buffer was not closed by writer")
	}
}
