package input

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

func TestFileSinglePart(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "benthos_file_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	messages := []string{
		"first message",
		"second message",
		"third message",
	}

	for _, msg := range messages {
		tmpfile.Write([]byte(msg))
		tmpfile.Write([]byte("\n"))
		tmpfile.Write([]byte("\n")) // Try some empty messages
	}

	conf := NewConfig()
	conf.File.Path = tmpfile.Name()

	f, err := NewFile(conf, nil, log.Noop(), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	defer func() {
		f.CloseAsync()
		if err := f.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	for _, msg := range messages {
		var ts types.Transaction
		var open bool
		select {
		case ts, open = <-f.TransactionChan():
			if !open {
				t.Error("channel closed early")
			} else if res := string(ts.Payload.Get(0).Get()); res != msg {
				t.Errorf("Wrong result, %v != %v", res, msg)
			}
		case <-time.After(time.Second):
			t.Error("Timed out waiting for message")
		}
		select {
		case ts.ResponseChan <- response.NewAck():
		case <-time.After(time.Second):
			t.Error("Timed out waiting for response")
		}
	}

	select {
	case _, open := <-f.TransactionChan():
		if open {
			t.Error("Channel not closed at end of messages")
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for channel close")
	}
}

func TestFileMultiPart(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "benthos_file_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

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
			tmpfile.Write([]byte(part))
			tmpfile.Write([]byte("\n"))
		}
		tmpfile.Write([]byte("\n"))
	}

	conf := NewConfig()
	conf.File.Path = tmpfile.Name()
	conf.File.Multipart = true

	f, err := NewFile(conf, nil, log.Noop(), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	defer func() {
		f.CloseAsync()
		if err := f.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	for _, msg := range messages {
		var ts types.Transaction
		var open bool
		select {
		case ts, open = <-f.TransactionChan():
			if !open {
				t.Error("channel closed early")
			} else {
				for i, part := range msg {
					if res := string(ts.Payload.Get(i).Get()); res != part {
						t.Errorf("Wrong result, %v != %v", res, part)
					}
				}
			}
		case <-time.After(time.Second):
			t.Error("Timed out waiting for message")
		}
		select {
		case ts.ResponseChan <- response.NewAck():
		case <-time.After(time.Second):
			t.Error("Timed out waiting for response")
		}
	}

	select {
	case _, open := <-f.TransactionChan():
		if open {
			t.Error("Channel not closed at end of messages")
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for channel close")
	}
}
