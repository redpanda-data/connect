// Copyright (c) 2019 Ashley Jeffs
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

package integration

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

func checkALOSynchronousAsync(
	outputCtr func() (writer.Type, error),
	inputCtr func() (reader.Async, error),
	t *testing.T,
) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*60)
	defer done()

	output, err := outputCtr()
	if err != nil {
		t.Fatal(err)
	}
	input, err := inputCtr()
	if err != nil {
		t.Fatal(err)
	}

	N := 100

	testMsgs := map[string]struct{}{}
	for i := 0; i < N; i++ {
		str := fmt.Sprintf("hello world: %v", i)
		testMsgs[str] = struct{}{}
		msg := message.New([][]byte{
			[]byte(str),
		})
		if err = output.Write(msg); err != nil {
			t.Fatal(err)
		}
	}

	receivedMsgs := map[string]struct{}{}
	for i := 0; i < len(testMsgs); i++ {
		var actM types.Message
		var ackFn reader.AsyncAckFn
		if actM, ackFn, err = input.ReadWithContext(ctx); err != nil {
			if err == types.ErrNotConnected {
				if err = input.ConnectWithContext(ctx); err != nil {
					t.Fatal(err)
				}
				actM, ackFn, err = input.ReadWithContext(ctx)
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		ackErr := false
		var res types.Response = response.NewAck()
		if i%10 == 0 {
			res = response.NewError(errors.New("nah"))
			ackErr = true
		}
		if !ackErr {
			actM.Iter(func(i int, part types.Part) error {
				act := string(part.Get())

				if _, exists := receivedMsgs[act]; exists {
					t.Errorf("Duplicate message: %v", act)
				} else {
					receivedMsgs[act] = struct{}{}
				}
				if _, exists := testMsgs[act]; !exists {
					t.Errorf("Unexpected message: %v", act)
				}
				delete(testMsgs, act)
				return nil
			})
		}
		if err = ackFn(ctx, res); err != nil {
			t.Error(err)
		}
	}

	lMsgs := len(testMsgs)
	if lMsgs == 0 {
		t.Error("Expected remaining messages")
	}

	for lMsgs > 0 {
		var actM types.Message
		var ackFn reader.AsyncAckFn
		if actM, ackFn, err = input.ReadWithContext(ctx); err != nil {
			if err == types.ErrNotConnected {
				if err = input.ConnectWithContext(ctx); err != nil {
					t.Fatal(err)
				}
				actM, ackFn, err = input.ReadWithContext(ctx)
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		actM.Iter(func(i int, part types.Part) error {
			act := string(part.Get())
			if _, exists := receivedMsgs[act]; exists {
				t.Errorf("Duplicate message: %v", act)
			} else {
				receivedMsgs[act] = struct{}{}
			}
			if _, exists := testMsgs[act]; !exists {
				t.Errorf("Unexpected message: %v", act)
			}
			delete(testMsgs, act)
			return nil
		})
		if err = ackFn(ctx, response.NewAck()); err != nil {
			t.Error(err)
		}
		lMsgs = len(testMsgs)
	}

	input.CloseAsync()
	output.CloseAsync()
	if err = input.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
	if err = output.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func checkALOSynchronousAndDieAsync(
	outputCtr func() (writer.Type, error),
	inputCtr func() (reader.Async, error),
	t *testing.T,
) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*60)
	defer done()

	output, err := outputCtr()
	if err != nil {
		t.Fatal(err)
	}
	input, err := inputCtr()
	if err != nil {
		t.Fatal(err)
	}

	N := 100

	testMsgs := map[string]struct{}{}
	for i := 0; i < N; i++ {
		str := fmt.Sprintf("hello world: %v", i)
		testMsgs[str] = struct{}{}
		msg := message.New([][]byte{
			[]byte(str),
		})
		if err = output.Write(msg); err != nil {
			t.Fatal(err)
		}
	}

	receivedMsgs := map[string]struct{}{}
	for i := 0; i < len(testMsgs); i++ {
		var actM types.Message
		var ackFn reader.AsyncAckFn
		if actM, ackFn, err = input.ReadWithContext(ctx); err != nil {
			if err == types.ErrNotConnected {
				if err = input.ConnectWithContext(ctx); err != nil {
					t.Fatal(err)
				}
				actM, ackFn, err = input.ReadWithContext(ctx)
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		ackErr := false
		var res types.Response = response.NewAck()
		if i%10 == 0 {
			res = response.NewError(errors.New("nah"))
			ackErr = true
		}
		if !ackErr {
			actM.Iter(func(i int, part types.Part) error {
				act := string(part.Get())

				if _, exists := receivedMsgs[act]; exists {
					t.Errorf("Duplicate message: %v", act)
				} else {
					receivedMsgs[act] = struct{}{}
				}
				if _, exists := testMsgs[act]; !exists {
					t.Errorf("Unexpected message: %v", act)
				}
				delete(testMsgs, act)
				return nil
			})
		}
		if err = ackFn(ctx, res); err != nil {
			t.Error(err)
		}
	}

	lMsgs := len(testMsgs)
	if lMsgs == 0 {
		t.Error("Expected remaining messages")
	}

	input.CloseAsync()
	if err = input.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
	if input, err = inputCtr(); err != nil {
		t.Fatal(err)
	}

	for lMsgs > 0 {
		var actM types.Message
		var ackFn reader.AsyncAckFn
		if actM, ackFn, err = input.ReadWithContext(ctx); err != nil {
			if err == types.ErrNotConnected {
				if err = input.ConnectWithContext(ctx); err != nil {
					t.Fatal(err)
				}
				actM, ackFn, err = input.ReadWithContext(ctx)
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		actM.Iter(func(i int, part types.Part) error {
			act := string(part.Get())
			if _, exists := receivedMsgs[act]; exists {
				t.Errorf("Duplicate message: %v", act)
			} else {
				receivedMsgs[act] = struct{}{}
			}
			if _, exists := testMsgs[act]; !exists {
				t.Errorf("Unexpected message: %v", act)
			}
			delete(testMsgs, act)
			return nil
		})
		if err = ackFn(ctx, response.NewAck()); err != nil {
			t.Error(err)
		}
		lMsgs = len(testMsgs)
	}

	input.CloseAsync()
	output.CloseAsync()
	if err = input.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
	if err = output.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func checkALOParallelAsync(
	outputCtr func() (writer.Type, error),
	inputCtr func() (reader.Async, error),
	N int,
	t *testing.T,
) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*60)
	defer done()

	output, err := outputCtr()
	if err != nil {
		t.Fatal(err)
	}
	input, err := inputCtr()
	if err != nil {
		t.Fatal(err)
	}

	testMsgs := map[string]struct{}{}
	for i := 0; i < N; i++ {
		str := fmt.Sprintf("hello world: %v", i)
		testMsgs[str] = struct{}{}
		msg := message.New([][]byte{
			[]byte(str),
		})
		if err = output.Write(msg); err != nil {
			t.Fatal(err)
		}
	}

	receivedMsgs := map[string]struct{}{}
	ackFns := []reader.AsyncAckFn{}
	for i := 0; i < len(testMsgs); i++ {
		var actM types.Message
		var ackFn reader.AsyncAckFn
		if actM, ackFn, err = input.ReadWithContext(ctx); err != nil {
			if err == types.ErrNotConnected {
				if err = input.ConnectWithContext(ctx); err != nil {
					t.Fatalf("Failed at '%v' read: %v", i, err)
				}
				actM, ackFn, err = input.ReadWithContext(ctx)
			}
			if err != nil {
				t.Fatalf("Failed at '%v' read: %v", i, err)
			}
		}
		ackFns = append(ackFns, ackFn)
		if i%10 != 0 {
			actM.Iter(func(i int, part types.Part) error {
				act := string(part.Get())

				if _, exists := receivedMsgs[act]; exists {
					t.Errorf("Duplicate message: %v", act)
				} else {
					receivedMsgs[act] = struct{}{}
				}
				if _, exists := testMsgs[act]; !exists {
					t.Errorf("Unexpected message: %v", act)
				}
				delete(testMsgs, act)
				return nil
			})
		}
	}

	for i, ackFn := range ackFns {
		var res types.Response = response.NewAck()
		if i%10 == 0 {
			res = response.NewError(errors.New("nah"))
		}
		if err = ackFn(ctx, res); err != nil {
			t.Error(err)
		}
	}

	lMsgs := len(testMsgs)
	if lMsgs == 0 {
		t.Error("Expected remaining messages")
	}

	for lMsgs > 0 {
		var actM types.Message
		var ackFn reader.AsyncAckFn
		if actM, ackFn, err = input.ReadWithContext(ctx); err != nil {
			if err == types.ErrNotConnected {
				if err = input.ConnectWithContext(ctx); err != nil {
					t.Fatal(err)
				}
				actM, ackFn, err = input.ReadWithContext(ctx)
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		actM.Iter(func(i int, part types.Part) error {
			act := string(part.Get())
			if _, exists := receivedMsgs[act]; exists {
				t.Errorf("Duplicate message: %v", act)
			} else {
				receivedMsgs[act] = struct{}{}
			}
			if _, exists := testMsgs[act]; !exists {
				t.Errorf("Unexpected message: %v", act)
			}
			delete(testMsgs, act)
			return nil
		})
		if err = ackFn(ctx, response.NewAck()); err != nil {
			t.Error(err)
		}
		lMsgs = len(testMsgs)
	}

	input.CloseAsync()
	output.CloseAsync()
	if err = input.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
	if err = output.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}
