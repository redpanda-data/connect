// Copyright (c) 2017 Ashley Jeffs
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
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/pipeline"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

type mockOutput struct {
	ts <-chan types.Transaction
}

func (m *mockOutput) Consume(ts <-chan types.Transaction) error {
	m.ts = ts
	return nil
}

func (m *mockOutput) Connected() bool {
	return true
}

func (m *mockOutput) CloseAsync() {
	// NOT EXPECTING TO HIT THIS
}

func (m *mockOutput) WaitForClose(dur time.Duration) error {
	select {
	case _, open := <-m.ts:
		if open {
			return errors.New("messages chan still open")
		}
	case <-time.After(dur):
		return errors.New("timed out")
	}
	return nil
}

//------------------------------------------------------------------------------

type mockPipe struct {
	tsIn <-chan types.Transaction
	ts   chan types.Transaction
}

func (m *mockPipe) Consume(ts <-chan types.Transaction) error {
	m.tsIn = ts
	return nil
}

func (m *mockPipe) TransactionChan() <-chan types.Transaction {
	return m.ts
}

func (m *mockPipe) CloseAsync() {
	close(m.ts)
}

func (m *mockPipe) WaitForClose(time.Duration) error {
	return errors.New("not expecting to see this")
}

//------------------------------------------------------------------------------

func TestBasicWrapPipeline(t *testing.T) {
	mockOut := &mockOutput{}
	mockPi := &mockPipe{
		ts: make(chan types.Transaction),
	}

	procs := 0
	newOutput, err := WrapWithPipeline(&procs, mockOut, func(i *int) (types.Pipeline, error) {
		return nil, errors.New("nope")
	})
	if err == nil {
		t.Error("expected error from back constructor")
	}

	newOutput, err = WrapWithPipeline(&procs, mockOut, func(i *int) (types.Pipeline, error) {
		return mockPi, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	dudMsgChan := make(chan types.Transaction)
	if err = newOutput.Consume(dudMsgChan); err != nil {
		t.Error(err)
	}

	if mockPi.tsIn != dudMsgChan {
		t.Error("Wrong message chan in mock pipe")
	}

	if mockOut.ts != mockPi.ts {
		t.Error("Wrong messages chan in mock pipe")
	}

	newOutput.CloseAsync()
	if err = newOutput.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestBasicWrapPipelinesOrdering(t *testing.T) {
	mockOut := &mockOutput{}

	firstProc := processor.NewConfig()
	firstProc.Type = "insert_part"
	firstProc.InsertPart.Content = "foo"
	firstProc.InsertPart.Index = 0

	secondProc := processor.NewConfig()
	secondProc.Type = "select_parts"

	conf := NewConfig()
	conf.Processors = append(conf.Processors, firstProc)

	newOutput, err := WrapWithPipelines(
		mockOut,
		func(i *int) (types.Pipeline, error) {
			proc, err := processor.New(
				firstProc, nil,
				log.New(os.Stdout, log.Config{LogLevel: "NONE"}),
				metrics.DudType{},
			)
			if err != nil {
				return nil, err
			}
			return pipeline.NewProcessor(
				log.New(os.Stdout, log.Config{LogLevel: "NONE"}),
				metrics.DudType{},
				proc,
			), nil
		},
		func(i *int) (types.Pipeline, error) {
			proc, err := processor.New(
				secondProc, nil,
				log.New(os.Stdout, log.Config{LogLevel: "NONE"}),
				metrics.DudType{},
			)
			if err != nil {
				return nil, err
			}
			return pipeline.NewProcessor(
				log.New(os.Stdout, log.Config{LogLevel: "NONE"}),
				metrics.DudType{},
				proc,
			), nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	tChan := make(chan types.Transaction)
	resChan := make(chan types.Response)
	if err = newOutput.Consume(tChan); err != nil {
		t.Error(err)
	}

	select {
	case <-time.After(time.Second):
		t.Fatal("timed out")
	case tChan <- types.NewTransaction(
		message.New([][]byte{[]byte("bar")}), resChan,
	):
	}

	var tran types.Transaction
	select {
	case <-time.After(time.Second):
		t.Fatal("timed out")
	case tran = <-mockOut.ts:
	}

	exp := [][]byte{
		[]byte("foo"),
	}
	if act := message.GetAllBytes(tran.Payload); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong contents: %s != %s", act, exp)
	}

	go func() {
		select {
		case <-time.After(time.Second):
			t.Fatal("timed out")
		case tran.ResponseChan <- response.NewAck():
		}
	}()

	select {
	case <-time.After(time.Second):
		t.Fatal("timed out")
	case res := <-resChan:
		if res.Error() != nil {
			t.Error(res.Error())
		}
	}

	newOutput.CloseAsync()
	if err = newOutput.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

//------------------------------------------------------------------------------
