package output

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

type testBuffer struct {
	bytes.Buffer

	closed bool
}

func (t *testBuffer) Close() error {
	t.closed = true
	return nil
}

func TestLineWriterBasic(t *testing.T) {
	var buf testBuffer

	msgChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	writer, err := NewLineWriter(&buf, true, []byte{}, "foo", log.New(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	if err = writer.Consume(msgChan); err != nil {
		t.Error(err)
	}
	if err = writer.Consume(msgChan); err == nil {
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
		msg := message.New(nil)
		for _, part := range c.message {
			msg.Append(message.NewPart([]byte(part)))
		}

		select {
		case msgChan <- types.NewTransaction(msg, resChan):
		case <-time.After(time.Second):
			t.Error("Timed out sending message")
		}

		select {
		case res, open := <-resChan:
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

func TestLineWriterCustomDelim(t *testing.T) {
	var buf testBuffer

	msgChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	writer, err := NewLineWriter(&buf, true, []byte("<FOO>"), "foo", log.New(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	if err = writer.Consume(msgChan); err != nil {
		t.Error(err)
	}
	if err = writer.Consume(msgChan); err == nil {
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
		msg := message.New(nil)
		for _, part := range c.message {
			msg.Append(message.NewPart([]byte(part)))
		}

		select {
		case msgChan <- types.NewTransaction(msg, resChan):
		case <-time.After(time.Second):
			t.Error("Timed out sending message")
		}

		select {
		case res, open := <-resChan:
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
