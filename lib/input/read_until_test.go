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

package input

import (
	"errors"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

func TestReadUntilInput(t *testing.T) {
	content := []byte(`foo
bar
baz`)

	tmpfile, err := ioutil.TempFile("", "benthos_read_until_test")
	if err != nil {
		t.Fatal(err)
	}

	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}

	inconf := NewConfig()
	inconf.Type = "file"
	inconf.File.Path = tmpfile.Name()
	inconf.File.Multipart = false

	t.Run("ReadUntilBasic", func(te *testing.T) {
		testReadUntilBasic(inconf, te)
	})
	t.Run("ReadUntilRetry", func(te *testing.T) {
		testReadUntilRetry(inconf, te)
	})
	t.Run("ReadUntilEarlyClose", func(te *testing.T) {
		testReadUntilEarlyClose(inconf, te)
	})
	t.Run("ReadUntilInputClose", func(te *testing.T) {
		testReadUntilInputClose(inconf, te)
	})
	t.Run("ReadUntilInputCloseRestart", func(te *testing.T) {
		testReadUntilInputCloseRestart(inconf, te)
	})
}

func testReadUntilBasic(inConf Config, t *testing.T) {
	cond := condition.NewConfig()
	cond.Type = "text"
	cond.Text.Operator = "equals"
	cond.Text.Arg = "bar"

	rConf := NewConfig()
	rConf.Type = "read_until"
	rConf.ReadUntil.Input = &inConf
	rConf.ReadUntil.Condition = cond

	in, err := New(rConf, nil, log.Noop(), metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}

	expMsgs := []string{
		"foo",
		"bar",
	}

	for i, expMsg := range expMsgs {
		var tran types.Transaction
		var open bool
		select {
		case tran, open = <-in.TransactionChan():
			if !open {
				t.Fatal("transaction chan closed")
			}
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}

		if exp, act := expMsg, string(tran.Payload.Get(0).Get()); exp != act {
			t.Errorf("Wrong message contents: %v != %v", act, exp)
		}
		if i == len(expMsgs)-1 {
			if exp, act := "final", tran.Payload.Get(0).Metadata().Get("benthos_read_until"); exp != act {
				t.Errorf("Metadata missing from final message: %v != %v", act, exp)
			}
		} else if exp, act := "", tran.Payload.Get(0).Metadata().Get("benthos_read_until"); exp != act {
			t.Errorf("Metadata final message metadata added to non-final message: %v", act)
		}

		select {
		case tran.ResponseChan <- response.NewAck():
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}
	}

	// Should close automatically now
	select {
	case _, open := <-in.TransactionChan():
		if open {
			t.Fatal("transaction chan not closed")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	if err = in.WaitForClose(time.Second); err != nil {
		t.Fatal(err)
	}
}

func testReadUntilRetry(inConf Config, t *testing.T) {
	cond := condition.NewConfig()
	cond.Type = "text"
	cond.Text.Operator = "equals"
	cond.Text.Arg = "bar"

	rConf := NewConfig()
	rConf.Type = "read_until"
	rConf.ReadUntil.Input = &inConf
	rConf.ReadUntil.Condition = cond

	in, err := New(rConf, nil, log.Noop(), metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}

	expMsgs := map[string]struct{}{
		"foo": {},
		"bar": {},
	}

	var tran types.Transaction
	var open bool

	resChans := []chan<- types.Response{}
	i := 0
	for len(expMsgs) > 0 && i < 10 {
		// First try
		select {
		case tran, open = <-in.TransactionChan():
			if !open {
				t.Fatalf("transaction chan closed at %v", i)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}

		i++
		act := string(tran.Payload.Get(0).Get())
		if _, exists := expMsgs[act]; !exists {
			t.Errorf("Unexpected message contents '%v': %v", i, act)
		} else {
			delete(expMsgs, act)
		}
		resChans = append(resChans, tran.ResponseChan)
	}

	select {
	case <-in.TransactionChan():
		t.Error("Unexpected transaction")
		return
	case <-time.After(time.Millisecond * 500):
	}

	for _, rChan := range resChans {
		select {
		case rChan <- response.NewError(errors.New("failed")):
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}
	}

	expMsgs = map[string]struct{}{
		"foo": {},
		"bar": {},
		"baz": {},
	}

remainingLoop:
	for len(expMsgs) > 0 {
		// Second try
		select {
		case tran, open = <-in.TransactionChan():
			if !open {
				break remainingLoop
			}
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}

		act := string(tran.Payload.Get(0).Get())
		if _, exists := expMsgs[act]; !exists {
			t.Errorf("Unexpected message contents '%v': %v", i, act)
		} else {
			delete(expMsgs, act)
		}

		select {
		case tran.ResponseChan <- response.NewAck():
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}
	}
	if len(expMsgs) == 3 {
		t.Error("Expected at least one extra message")
	}

	// Should close automatically now
	select {
	case _, open := <-in.TransactionChan():
		if open {
			t.Fatal("transaction chan not closed")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	if err = in.WaitForClose(time.Second); err != nil {
		t.Fatal(err)
	}
}

func testReadUntilEarlyClose(inConf Config, t *testing.T) {
	cond := condition.NewConfig()
	cond.Type = "text"
	cond.Text.Operator = "equals"
	cond.Text.Arg = "bar"

	rConf := NewConfig()
	rConf.Type = "read_until"
	rConf.ReadUntil.Input = &inConf
	rConf.ReadUntil.Condition = cond

	in, err := New(rConf, nil, log.Noop(), metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}

	var tran types.Transaction
	var open bool

	select {
	case tran, open = <-in.TransactionChan():
		if !open {
			t.Fatal("transaction chan closed")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	if act, exp := string(tran.Payload.Get(0).Get()), "foo"; exp != act {
		t.Errorf("Wrong message contents: %v != %v", act, exp)
	}

	select {
	case tran.ResponseChan <- response.NewAck():
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	in.CloseAsync()
	if err = in.WaitForClose(time.Second * 5); err != nil {
		t.Fatal(err)
	}
}

func testReadUntilInputClose(inConf Config, t *testing.T) {
	cond := condition.NewConfig()
	cond.Type = "text"
	cond.Text.Operator = "equals"
	cond.Text.Arg = "this never resolves"

	rConf := NewConfig()
	rConf.Type = "read_until"
	rConf.ReadUntil.Input = &inConf
	rConf.ReadUntil.Condition = cond

	in, err := New(rConf, nil, log.Noop(), metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}

	expMsgs := []string{
		"foo",
		"bar",
		"baz",
	}

	for _, exp := range expMsgs {
		var tran types.Transaction
		var open bool
		select {
		case tran, open = <-in.TransactionChan():
			if !open {
				t.Fatal("transaction chan closed")
			}
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}

		if act := string(tran.Payload.Get(0).Get()); exp != act {
			t.Errorf("Wrong message contents: %v != %v", act, exp)
		}

		select {
		case tran.ResponseChan <- response.NewAck():
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}
	}

	// Should close automatically now
	select {
	case _, open := <-in.TransactionChan():
		if open {
			t.Fatal("transaction chan not closed")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	if err = in.WaitForClose(time.Second); err != nil {
		t.Fatal(err)
	}
}

func testReadUntilInputCloseRestart(inConf Config, t *testing.T) {
	cond := condition.NewConfig()
	cond.Type = "static"
	cond.Static = false

	rConf := NewConfig()
	rConf.Type = "read_until"
	rConf.ReadUntil.Input = &inConf
	rConf.ReadUntil.Condition = cond
	rConf.ReadUntil.Restart = true

	in, err := New(rConf, nil, log.Noop(), metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}

	expMsgs := []string{
		"foo",
		"bar",
		"baz",
	}

	// Each loop results in the input being recreated.
	for i := 0; i < 3; i++ {
		for _, exp := range expMsgs {
			var tran types.Transaction
			var open bool
			select {
			case tran, open = <-in.TransactionChan():
				if !open {
					t.Fatal("transaction chan closed")
				}
			case <-time.After(time.Second):
				t.Fatal("timed out")
			}

			if act := string(tran.Payload.Get(0).Get()); exp != act {
				t.Errorf("Wrong message contents: %v != %v", act, exp)
			}

			select {
			case tran.ResponseChan <- response.NewAck():
			case <-time.After(time.Second):
				t.Fatal("timed out")
			}
		}
	}

	in.CloseAsync()
	if err = in.WaitForClose(time.Second); err != nil {
		t.Fatal(err)
	}
}
