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

package reader

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/types"
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

	ctored := false
	f, err := NewLines(
		func() (io.Reader, error) {
			if ctored {
				return nil, io.EOF
			}
			ctored = true
			return &handle, nil
		},
		func() {},
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

	if err = f.Connect(); err != nil {
		t.Fatal(err)
	}

	for _, msg := range messages {
		var resMsg types.Message
		if resMsg, err = f.Read(); err != nil {
			t.Fatal(err)
		}
		if res := string(resMsg.Get(0).Get()); res != msg {
			t.Errorf("Wrong result, %v != %v", res, msg)
		}
		if err = f.Acknowledge(nil); err != nil {
			t.Error(err)
		}
	}

	if _, err = f.Read(); err != types.ErrNotConnected {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrNotConnected)
	}

	if err = f.Connect(); err != types.ErrTypeClosed {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrTypeClosed)
	}
}

func TestReaderSinglePartAppended(t *testing.T) {
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

	ctored := false
	f, err := NewLines(
		func() (io.Reader, error) {
			if ctored {
				return nil, io.EOF
			}
			ctored = true
			return &handle, nil
		},
		func() {},
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

	if err = f.Connect(); err != nil {
		t.Fatal(err)
	}

	parts := [][]byte{}
	for _, msg := range messages {
		var resMsg types.Message
		if resMsg, err = f.Read(); err != nil {
			t.Fatal(err)
		}
		rBytes := resMsg.Get(0).Get()
		if res := string(rBytes); res != msg {
			t.Errorf("Wrong result, %v != %v", res, msg)
		}
		parts = append(parts, rBytes)
	}
	if err = f.Acknowledge(nil); err != nil {
		t.Error(err)
	}

	for i, msg := range messages {
		parts[i] = append(parts[i], []byte(" foo")...)
		if exp, act := msg+" foo", string(parts[i]); act != exp {
			t.Errorf("Wrong appended result, %v != %v", act, exp)
		}
	}

	if _, err = f.Read(); err != types.ErrNotConnected {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrNotConnected)
	}

	if err = f.Connect(); err != types.ErrTypeClosed {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrTypeClosed)
	}
}

func TestReaderSinglePartMultiReaders(t *testing.T) {
	messages := []string{
		"first message",
		"second message",
		"third message",
	}

	var handle1, handle2 bytes.Buffer

	for _, msg := range messages {
		handle1.Write([]byte(msg))
		handle1.Write([]byte("\n"))
		handle2.Write([]byte(msg))
		handle2.Write([]byte("\n"))
	}

	ctored1, ctored2 := false, false
	f, err := NewLines(
		func() (io.Reader, error) {
			if ctored2 {
				return nil, io.EOF
			}
			if ctored1 {
				ctored2 = true
				return &handle2, nil
			}
			ctored1 = true
			return &handle1, nil
		},
		func() {},
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

	if err = f.Connect(); err != nil {
		t.Fatal(err)
	}

	for _, msg := range messages {
		var resMsg types.Message
		if resMsg, err = f.Read(); err != nil {
			t.Error(err)
		} else if res := string(resMsg.Get(0).Get()); res != msg {
			t.Errorf("Wrong result, %v != %v", res, msg)
		}
		if err = f.Acknowledge(nil); err != nil {
			t.Error(err)
		}
	}

	if _, err = f.Read(); err != types.ErrNotConnected {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrNotConnected)
	}

	if err = f.Connect(); err != nil {
		t.Error(err)
	}

	for _, msg := range messages {
		var resMsg types.Message
		if resMsg, err = f.Read(); err != nil {
			t.Error(err)
		} else if res := string(resMsg.Get(0).Get()); res != msg {
			t.Errorf("Wrong result, %v != %v", res, msg)
		}
		if err = f.Acknowledge(nil); err != nil {
			t.Error(err)
		}
	}

	if _, err = f.Read(); err != types.ErrNotConnected {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrNotConnected)
	}

	if err = f.Connect(); err != types.ErrTypeClosed {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrTypeClosed)
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

	ctored := false
	f, err := NewLines(
		func() (io.Reader, error) {
			if ctored {
				return nil, io.EOF
			}
			ctored = true
			return &handle, nil
		},
		func() {},
		OptLinesSetDelimiter("<FOO>"),
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

	if err = f.Connect(); err != nil {
		t.Fatal(err)
	}

	for _, msg := range messages {
		var resMsg types.Message
		if resMsg, err = f.Read(); err != nil {
			t.Error(err)
		} else if res := string(resMsg.Get(0).Get()); res != msg {
			t.Errorf("Wrong result, %v != %v", res, msg)
		}
		if err = f.Acknowledge(nil); err != nil {
			t.Error(err)
		}
	}

	if _, err = f.Read(); err != types.ErrNotConnected {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrNotConnected)
	}

	if err = f.Connect(); err != types.ErrTypeClosed {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrTypeClosed)
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
			"3",
			"4",
		},
		{
			"third message",
			"5",
			"6",
		},
	}

	for _, msg := range messages {
		for _, part := range msg {
			handle.Write([]byte(part))
			handle.Write([]byte("\n"))
		}
		handle.Write([]byte("\n"))
	}

	ctored := false
	f, err := NewLines(
		func() (io.Reader, error) {
			if ctored {
				return nil, io.EOF
			}
			ctored = true
			return &handle, nil
		},
		func() {},
		OptLinesSetMultipart(true),
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

	if err = f.Connect(); err != nil {
		t.Fatal(err)
	}

	for _, msg := range messages {
		var resMsg types.Message
		if resMsg, err = f.Read(); err != nil {
			t.Error(err)
		} else {
			for i, part := range msg {
				if res := string(resMsg.Get(i).Get()); res != part {
					t.Errorf("Wrong result, %v != %v", res, part)
				}
			}
		}
		if err = f.Acknowledge(nil); err != nil {
			t.Error(err)
		}
	}

	if _, err = f.Read(); err != types.ErrNotConnected {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrNotConnected)
	}

	if err = f.Connect(); err != types.ErrTypeClosed {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrTypeClosed)
	}
}

func TestReaderMultiPartNoAck(t *testing.T) {
	var handle bytes.Buffer

	messages := [][]string{
		{
			"first message",
			"1",
			"2",
		},
		{
			"second message",
			"3",
			"4",
		},
		{
			"third message",
			"5",
			"6",
		},
	}

	for _, msg := range messages {
		for _, part := range msg {
			handle.Write([]byte(part))
			handle.Write([]byte("\n"))
		}
		handle.Write([]byte("\n"))
	}

	ctored := false
	f, err := NewLines(
		func() (io.Reader, error) {
			if ctored {
				return nil, io.EOF
			}
			ctored = true
			return &handle, nil
		},
		func() {},
		OptLinesSetMultipart(true),
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

	if err = f.Connect(); err != nil {
		t.Fatal(err)
	}

	for _, msg := range messages {
		var resMsg types.Message
		if resMsg, err = f.Read(); err != nil {
			t.Error(err)
		} else {
			for i, part := range msg {
				if res := string(resMsg.Get(i).Get()); res != part {
					t.Errorf("Wrong result, %v != %v", res, part)
				}
			}
		}
	}
	if err = f.Acknowledge(nil); err != nil {
		t.Error(err)
	}

	if _, err = f.Read(); err != types.ErrNotConnected {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrNotConnected)
	}

	if err = f.Connect(); err != types.ErrTypeClosed {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrTypeClosed)
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
			"3",
			"4",
		},
		{
			"third message",
			"5",
			"6",
		},
	}

	for _, msg := range messages {
		for _, part := range msg {
			handle.Write([]byte(part))
			handle.Write([]byte("<FOO>"))
		}
		handle.Write([]byte("<FOO>"))
	}

	ctored := false
	f, err := NewLines(
		func() (io.Reader, error) {
			if ctored {
				return nil, io.EOF
			}
			ctored = true
			return &handle, nil
		},
		func() {},
		OptLinesSetMultipart(true),
		OptLinesSetDelimiter("<FOO>"),
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

	if err = f.Connect(); err != nil {
		t.Fatal(err)
	}

	for _, msg := range messages {
		var resMsg types.Message
		if resMsg, err = f.Read(); err != nil {
			t.Error(err)
		} else {
			for i, part := range msg {
				if res := string(resMsg.Get(i).Get()); res != part {
					t.Errorf("Wrong result, %v != %v", res, part)
				}
			}
		}
		if err = f.Acknowledge(nil); err != nil {
			t.Error(err)
		}
	}

	if _, err = f.Read(); err != types.ErrNotConnected {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrNotConnected)
	}

	if err = f.Connect(); err != types.ErrTypeClosed {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrTypeClosed)
	}
}
