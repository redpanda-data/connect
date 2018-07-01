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

package log

import (
	"bytes"
	"testing"
)

func TestFanOutWriter(t *testing.T) {
	var bufMain, buf2, buf3 bytes.Buffer

	fow := NewFanOutWriter(&bufMain)

	conf := NewConfig()
	conf.AddTimeStamp = false
	l := New(fow, conf)

	l.Infoln("Foo bar 1")

	fow.Add(&buf2)
	l.Infoln("Foo bar 2")

	fow.Remove(&buf2)
	fow.Add(&buf3)
	l.Infoln("Foo bar 3")

	fow.Close()

	exp := `{"level":"INFO","@service":"benthos","message":"Foo bar 1"}
{"level":"INFO","@service":"benthos","message":"Foo bar 2"}
{"level":"INFO","@service":"benthos","message":"Foo bar 3"}` + "\n"
	if act := bufMain.String(); exp != act {
		t.Errorf("Wrong logged output: %v != %v", act, exp)
	}

	exp = `{"level":"INFO","@service":"benthos","message":"Foo bar 2"}` + "\n"
	if act := buf2.String(); exp != act {
		t.Errorf("Wrong logged output: %v != %v", act, exp)
	}

	exp = `{"level":"INFO","@service":"benthos","message":"Foo bar 3"}` + "\n"
	if act := buf3.String(); exp != act {
		t.Errorf("Wrong logged output: %v != %v", act, exp)
	}
}

type closeBuf struct {
	buf       bytes.Buffer
	hasClosed bool
}

func (c *closeBuf) Write(b []byte) (int, error) {
	return c.buf.Write(b)
}

func (c *closeBuf) Close() error {
	if c.hasClosed {
		panic("Closed twice")
	}
	c.hasClosed = true
	return nil
}

func TestFanOutWriterCloser(t *testing.T) {
	var bufMain, buf2, buf3 closeBuf

	fow := NewFanOutWriter(&bufMain)

	conf := NewConfig()
	conf.AddTimeStamp = false
	l := New(fow, conf)

	l.Infoln("Foo bar 1")

	fow.Add(&buf2)
	l.Infoln("Foo bar 2")

	fow.Remove(&buf2)
	fow.Add(&buf3)
	l.Infoln("Foo bar 3")

	fow.Close()

	exp := `{"level":"INFO","@service":"benthos","message":"Foo bar 1"}
{"level":"INFO","@service":"benthos","message":"Foo bar 2"}
{"level":"INFO","@service":"benthos","message":"Foo bar 3"}` + "\n"
	if act := bufMain.buf.String(); exp != act {
		t.Errorf("Wrong logged output: %v != %v", act, exp)
	}

	exp = `{"level":"INFO","@service":"benthos","message":"Foo bar 2"}` + "\n"
	if act := buf2.buf.String(); exp != act {
		t.Errorf("Wrong logged output: %v != %v", act, exp)
	}

	exp = `{"level":"INFO","@service":"benthos","message":"Foo bar 3"}` + "\n"
	if act := buf3.buf.String(); exp != act {
		t.Errorf("Wrong logged output: %v != %v", act, exp)
	}

	if bufMain.hasClosed {
		t.Error("bufMain was closed")
	}
	if !buf2.hasClosed {
		t.Error("buf2 not closed")
	}
	if !buf3.hasClosed {
		t.Error("buf3 not closed")
	}
}
