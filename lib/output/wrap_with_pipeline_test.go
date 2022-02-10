package output

import (
	"errors"
	"reflect"
	"testing"
	"time"

	iprocessor "github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/pipeline"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/response"
)

//------------------------------------------------------------------------------

type mockOutput struct {
	ts <-chan message.Transaction
}

func (m *mockOutput) Consume(ts <-chan message.Transaction) error {
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
	tsIn <-chan message.Transaction
	ts   chan message.Transaction
}

func (m *mockPipe) Consume(ts <-chan message.Transaction) error {
	m.tsIn = ts
	return nil
}

func (m *mockPipe) TransactionChan() <-chan message.Transaction {
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
		ts: make(chan message.Transaction),
	}

	procs := 0
	_, err := WrapWithPipeline(&procs, mockOut, func(i *int) (iprocessor.Pipeline, error) {
		return nil, errors.New("nope")
	})
	if err == nil {
		t.Error("expected error from back constructor")
	}

	newOutput, err := WrapWithPipeline(&procs, mockOut, func(i *int) (iprocessor.Pipeline, error) {
		return mockPi, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	dudMsgChan := make(chan message.Transaction)
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
		func(i *int) (iprocessor.Pipeline, error) {
			proc, err := processor.New(
				firstProc, nil,
				log.Noop(),
				metrics.Noop(),
			)
			if err != nil {
				return nil, err
			}
			return pipeline.NewProcessor(
				log.Noop(),
				metrics.Noop(),
				proc,
			), nil
		},
		func(i *int) (iprocessor.Pipeline, error) {
			proc, err := processor.New(
				secondProc, nil,
				log.Noop(),
				metrics.Noop(),
			)
			if err != nil {
				return nil, err
			}
			return pipeline.NewProcessor(
				log.Noop(),
				metrics.Noop(),
				proc,
			), nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	tChan := make(chan message.Transaction)
	resChan := make(chan response.Error)
	if err = newOutput.Consume(tChan); err != nil {
		t.Error(err)
	}

	select {
	case <-time.After(time.Second):
		t.Fatal("timed out")
	case tChan <- message.NewTransaction(
		message.QuickBatch([][]byte{[]byte("bar")}), resChan,
	):
	}

	var tran message.Transaction
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
			t.Error("timed out")
		case tran.ResponseChan <- response.NewError(nil):
		}
	}()

	select {
	case <-time.After(time.Second):
		t.Fatal("timed out")
	case res := <-resChan:
		if res.AckError() != nil {
			t.Error(res.AckError())
		}
	}

	newOutput.CloseAsync()
	if err = newOutput.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

//------------------------------------------------------------------------------
