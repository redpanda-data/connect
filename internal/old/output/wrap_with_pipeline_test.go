package output

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	iprocessor "github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/old/processor"
	"github.com/benthosdev/benthos/v4/internal/pipeline"
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

	_, err := WrapWithPipeline(mockOut, func() (iprocessor.Pipeline, error) {
		return nil, errors.New("nope")
	})
	if err == nil {
		t.Error("expected error from back constructor")
	}

	newOutput, err := WrapWithPipeline(mockOut, func() (iprocessor.Pipeline, error) {
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
	secondProc.SelectParts.Parts = []int{0}

	conf := NewConfig()
	conf.Processors = append(conf.Processors, firstProc)

	newOutput, err := WrapWithPipelines(
		mockOut,
		func() (iprocessor.Pipeline, error) {
			proc, err := processor.New(firstProc, mock.NewManager(), log.Noop(), metrics.Noop())
			if err != nil {
				return nil, err
			}
			return pipeline.NewProcessor(proc), nil
		},
		func() (iprocessor.Pipeline, error) {
			proc, err := processor.New(secondProc, mock.NewManager(), log.Noop(), metrics.Noop())
			if err != nil {
				return nil, err
			}
			return pipeline.NewProcessor(proc), nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	tChan := make(chan message.Transaction)
	resChan := make(chan error)
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
		case tran.ResponseChan <- nil:
		}
	}()

	select {
	case <-time.After(time.Second):
		t.Fatal("timed out")
	case res := <-resChan:
		if res != nil {
			t.Error(res)
		}
	}

	newOutput.CloseAsync()
	if err = newOutput.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

//------------------------------------------------------------------------------
