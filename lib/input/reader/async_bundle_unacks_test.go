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
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func TestAsyncBundleUnacksClose(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	readerImpl := newMockAsyncReader()
	pres := NewAsyncBundleUnacks(readerImpl)

	exp := errors.New("foo error")

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		if err := pres.ConnectWithContext(ctx); err != nil {
			t.Error(err)
		}
		pres.CloseAsync()
		if act := pres.WaitForClose(time.Second); act != exp {
			t.Errorf("Wrong error returned: %v != %v", act, exp)
		}
		wg.Done()
	}()

	select {
	case readerImpl.connChan <- nil:
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	select {
	case readerImpl.closeAsyncChan <- struct{}{}:
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	select {
	case readerImpl.waitForCloseChan <- exp:
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	wg.Wait()
}

func TestAsyncBundleUnacksBasic(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	readerImpl := newMockAsyncReader()
	pres := NewAsyncBundleUnacks(readerImpl)

	expMsgs := [][]byte{
		[]byte("foo"),
		[]byte("bar"),
	}

	go func() {
		select {
		case readerImpl.connChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		for _, p := range expMsgs {
			readerImpl.msgsToSnd = []types.Message{message.New([][]byte{p})}
			select {
			case readerImpl.readChan <- nil:
			case <-time.After(time.Second):
				t.Error("Timed out")
			}
			select {
			case readerImpl.ackChan <- nil:
			case <-time.After(time.Second):
				t.Error("Timed out")
			}
		}
	}()

	if err := pres.ConnectWithContext(ctx); err != nil {
		t.Error(err)
	}

	msg, ackFn, err := pres.ReadWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if act, exp := msg.Get(0).Get(), expMsgs[0]; !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong message returned: %s != %s", act, exp)
	}
	if err := ackFn(ctx, response.NewAck()); err != nil {
		t.Error(err)
	}

	msg, ackFn, err = pres.ReadWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if act, exp := msg.Get(0).Get(), expMsgs[1]; !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong message returned: %s != %s", act, exp)
	}
	if err := ackFn(ctx, response.NewAck()); err != nil {
		t.Error(err)
	}

	if exp, act := []error{nil, nil}, readerImpl.ackRcvd; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong acks returned: %v != %v", act, exp)
	}
}

func TestAsyncBundleUnacksHappy(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	readerImpl := newMockAsyncReader()
	pres := NewAsyncBundleUnacks(readerImpl)

	expMsgs := [][]byte{
		[]byte("foo"),
		[]byte("bar"),
	}
	for _, p := range expMsgs {
		readerImpl.msgsToSnd = append(readerImpl.msgsToSnd, message.New([][]byte{p}))
	}

	go func() {
		select {
		case readerImpl.connChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		for range expMsgs {
			select {
			case readerImpl.readChan <- nil:
			case <-time.After(time.Second):
				t.Error("Timed out")
			}
		}
		for range expMsgs {
			select {
			case readerImpl.ackChan <- nil:
			case <-time.After(time.Second):
				t.Error("Timed out")
			}
		}
	}()

	if err := pres.ConnectWithContext(ctx); err != nil {
		t.Error(err)
	}

	msg, ackFn, err := pres.ReadWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if act, exp := msg.Get(0).Get(), expMsgs[0]; !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong message returned: %s != %s", act, exp)
	}
	if err := ackFn(ctx, response.NewUnack()); err != nil {
		t.Error(err)
	}

	msg, ackFn, err = pres.ReadWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if act, exp := msg.Get(0).Get(), expMsgs[1]; !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong message returned: %s != %s", act, exp)
	}
	if err := ackFn(ctx, response.NewAck()); err != nil {
		t.Error(err)
	}

	if exp, act := []error{nil, nil}, readerImpl.ackRcvd; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong acks returned: %v != %v", act, exp)
	}
}

func TestAsyncBundleUnacksSad(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	readerImpl := newMockAsyncReader()
	pres := NewAsyncBundleUnacks(readerImpl)

	expMsgs := [][]byte{
		[]byte("foo"),
		[]byte("bar"),
	}
	for _, p := range expMsgs {
		readerImpl.msgsToSnd = append(readerImpl.msgsToSnd, message.New([][]byte{p}))
	}

	go func() {
		select {
		case readerImpl.connChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		for range expMsgs {
			select {
			case readerImpl.readChan <- nil:
			case <-time.After(time.Second):
				t.Error("Timed out")
			}
		}
		for range expMsgs {
			select {
			case readerImpl.ackChan <- nil:
			case <-time.After(time.Second):
				t.Error("Timed out")
			}
		}
	}()

	if err := pres.ConnectWithContext(ctx); err != nil {
		t.Error(err)
	}

	msg, ackFn, err := pres.ReadWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if act, exp := msg.Get(0).Get(), expMsgs[0]; !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong message returned: %s != %s", act, exp)
	}
	if err := ackFn(ctx, response.NewUnack()); err != nil {
		t.Error(err)
	}

	errTest := errors.New("test error")

	msg, ackFn, err = pres.ReadWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if act, exp := msg.Get(0).Get(), expMsgs[1]; !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong message returned: %s != %s", act, exp)
	}
	if err := ackFn(ctx, response.NewError(errTest)); err != nil {
		t.Error(err)
	}

	if exp, act := []error{errTest, errTest}, readerImpl.ackRcvd; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong acks returned: %v != %v", act, exp)
	}
}

func TestAsyncBundleUnacksSadTwo(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	readerImpl := newMockAsyncReader()
	pres := NewAsyncBundleUnacks(readerImpl)

	expMsgs := [][]byte{
		[]byte("foo"),
		[]byte("bar"),
	}
	for _, p := range expMsgs {
		readerImpl.msgsToSnd = append(readerImpl.msgsToSnd, message.New([][]byte{p}))
	}

	errFirstAck := errors.New("error returned by first ack")
	errSecondAck := errors.New("error returned by second ack")

	go func() {
		select {
		case readerImpl.connChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		for range expMsgs {
			select {
			case readerImpl.readChan <- nil:
			case <-time.After(time.Second):
				t.Error("Timed out")
			}
		}
		select {
		case readerImpl.ackChan <- errFirstAck:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		select {
		case readerImpl.ackChan <- errSecondAck:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}()

	if err := pres.ConnectWithContext(ctx); err != nil {
		t.Error(err)
	}

	msg, ackFn, err := pres.ReadWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if act, exp := msg.Get(0).Get(), expMsgs[0]; !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong message returned: %s != %s", act, exp)
	}
	if err := ackFn(ctx, response.NewUnack()); err != nil {
		t.Error(err)
	}

	errTest := errors.New("test error")

	msg, ackFn, err = pres.ReadWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if act, exp := msg.Get(0).Get(), expMsgs[1]; !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong message returned: %s != %s", act, exp)
	}
	if act, exp := ackFn(ctx, response.NewError(errTest)).Error(), "failed to send grouped acknowledgements: [error returned by first ack error returned by second ack]"; act != exp {
		t.Errorf("Wrong error returned: %v != %v", act, exp)
	}

	if exp, act := []error{errTest, errTest}, readerImpl.ackRcvd; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong acks returned: %v != %v", act, exp)
	}
}

func TestAsyncBundleUnacksSadThree(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	readerImpl := newMockAsyncReader()
	pres := NewAsyncBundleUnacks(readerImpl)

	expMsgs := [][]byte{
		[]byte("foo"),
		[]byte("bar"),
	}
	for _, p := range expMsgs {
		readerImpl.msgsToSnd = append(readerImpl.msgsToSnd, message.New([][]byte{p}))
	}

	errFirstAck := errors.New("error returned by first ack")

	go func() {
		select {
		case readerImpl.connChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		for range expMsgs {
			select {
			case readerImpl.readChan <- nil:
			case <-time.After(time.Second):
				t.Error("Timed out")
			}
		}
		select {
		case readerImpl.ackChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		select {
		case readerImpl.ackChan <- errFirstAck:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}()

	if err := pres.ConnectWithContext(ctx); err != nil {
		t.Error(err)
	}

	msg, ackFn, err := pres.ReadWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if act, exp := msg.Get(0).Get(), expMsgs[0]; !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong message returned: %s != %s", act, exp)
	}
	if err := ackFn(ctx, response.NewUnack()); err != nil {
		t.Error(err)
	}

	errTest := errors.New("test error")

	msg, ackFn, err = pres.ReadWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if act, exp := msg.Get(0).Get(), expMsgs[1]; !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong message returned: %s != %s", act, exp)
	}
	if act, exp := ackFn(ctx, response.NewError(errTest)).Error(), "error returned by first ack"; act != exp {
		t.Errorf("Wrong error returned: %v != %v", act, exp)
	}

	if exp, act := []error{errTest, errTest}, readerImpl.ackRcvd; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong acks returned: %v != %v", act, exp)
	}
}

//------------------------------------------------------------------------------
