package output

import (
	"testing"
	"time"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestRetryConfigErrs(t *testing.T) {
	conf := NewConfig()
	conf.Type = "retry"

	if _, err := New(conf, mock.NewManager(), log.Noop(), metrics.Noop()); err == nil {
		t.Error("Expected error from bad retry output")
	}

	oConf := NewConfig()
	conf.Retry.Output = &oConf
	conf.Retry.Backoff.InitialInterval = "not a time period"

	if _, err := New(conf, mock.NewManager(), log.Noop(), metrics.Noop()); err == nil {
		t.Error("Expected error from bad initial period")
	}
}

func TestRetryBasic(t *testing.T) {
	conf := NewConfig()

	childConf := NewConfig()
	conf.Retry.Output = &childConf

	output, err := NewRetry(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	ret, ok := output.(*Retry)
	if !ok {
		t.Fatal("Failed to cast")
	}

	mOut := &mockOutput{
		ts: make(chan message.Transaction),
	}
	ret.wrapped = mOut

	tChan := make(chan message.Transaction)
	resChan := make(chan error)

	if err = ret.Consume(tChan); err != nil {
		t.Fatal(err)
	}

	testMsg := message.QuickBatch(nil)
	go func() {
		select {
		case tChan <- message.NewTransaction(testMsg, resChan):
		case <-time.After(time.Second):
			t.Error("timed out")
		}
	}()

	var tran message.Transaction
	select {
	case tran = <-mOut.ts:
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	if tran.Payload != testMsg {
		t.Error("Wrong payload returned")
	}

	select {
	case tran.ResponseChan <- nil:
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	select {
	case res := <-resChan:
		if err = res; err != nil {
			t.Error(err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	output.CloseAsync()
	if err = output.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestRetrySadPath(t *testing.T) {
	conf := NewConfig()

	childConf := NewConfig()
	conf.Retry.Output = &childConf
	conf.Retry.Backoff.InitialInterval = "10us"
	conf.Retry.Backoff.MaxInterval = "10us"

	output, err := NewRetry(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	ret, ok := output.(*Retry)
	if !ok {
		t.Fatal("Failed to cast")
	}

	mOut := &mockOutput{
		ts: make(chan message.Transaction),
	}
	ret.wrapped = mOut

	tChan := make(chan message.Transaction)
	resChan := make(chan error)

	if err = ret.Consume(tChan); err != nil {
		t.Fatal(err)
	}

	testMsg := message.QuickBatch(nil)
	tran := message.NewTransaction(testMsg, resChan)

	go func() {
		select {
		case tChan <- tran:
		case <-time.After(time.Second):
			t.Error("timed out")
		}
	}()

	for i := 0; i < 100; i++ {
		select {
		case tran = <-mOut.ts:
		case <-resChan:
			t.Fatal("Received response not retry")
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}

		if tran.Payload != testMsg {
			t.Error("Wrong payload returned")
		}

		select {
		case tran.ResponseChan <- component.ErrFailedSend:
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}
	}

	select {
	case tran = <-mOut.ts:
	case <-resChan:
		t.Fatal("Received response not retry")
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	if tran.Payload != testMsg {
		t.Error("Wrong payload returned")
	}

	select {
	case tran.ResponseChan <- nil:
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	select {
	case res := <-resChan:
		if err = res; err != nil {
			t.Error(err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	output.CloseAsync()
	if err = output.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func expectFromRetry(
	resReturn error,
	tChan <-chan message.Transaction,
	t *testing.T,
	responsesSlice ...string) {
	t.Helper()

	responses := map[string]struct{}{}
	for _, k := range responsesSlice {
		responses[k] = struct{}{}
	}

	resChans := []chan<- error{}

	for len(responses) > 0 {
		select {
		case tran := <-tChan:
			act := string(tran.Payload.Get(0).Get())
			if _, exists := responses[act]; exists {
				delete(responses, act)
			} else {
				t.Errorf("Wrong result: %v", act)
			}
			resChans = append(resChans, tran.ResponseChan)
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}
	}

	for _, resChan := range resChans {
		select {
		case resChan <- resReturn:
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}
	}
}

func sendForRetry(
	value string,
	tChan chan message.Transaction,
	resChan chan error,
	t *testing.T,
) {
	t.Helper()

	select {
	case tChan <- message.NewTransaction(message.QuickBatch(
		[][]byte{[]byte(value)},
	), resChan):
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
}

func ackForRetry(
	exp error,
	resChan <-chan error,
	t *testing.T,
) {
	t.Helper()

	select {
	case res := <-resChan:
		if res != exp {
			t.Errorf("Unexpected response error: %v != %v", res, exp)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
}

func TestRetryParallel(t *testing.T) {
	conf := NewConfig()

	childConf := NewConfig()
	conf.Retry.Output = &childConf
	conf.Retry.Backoff.InitialInterval = "10us"
	conf.Retry.Backoff.MaxInterval = "10us"

	output, err := NewRetry(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	ret, ok := output.(*Retry)
	if !ok {
		t.Fatal("Failed to cast")
	}

	mOut := &mockOutput{
		ts: make(chan message.Transaction),
	}
	ret.wrapped = mOut

	tChan := make(chan message.Transaction)
	if err = ret.Consume(tChan); err != nil {
		t.Fatal(err)
	}

	resChan1, resChan2 := make(chan error), make(chan error)
	sendForRetry("first", tChan, resChan1, t)
	expectFromRetry(component.ErrFailedSend, mOut.ts, t, "first")

	sendForRetry("second", tChan, resChan2, t)
	expectFromRetry(component.ErrFailedSend, mOut.ts, t, "first", "second")

	select {
	case tChan <- message.NewTransaction(nil, nil):
		t.Fatal("Accepted transaction during retry loop")
	default:
	}
	expectFromRetry(nil, mOut.ts, t, "first", "second")
	ackForRetry(nil, resChan1, t)
	ackForRetry(nil, resChan2, t)

	sendForRetry("third", tChan, resChan1, t)
	expectFromRetry(nil, mOut.ts, t, "third")
	ackForRetry(nil, resChan1, t)

	sendForRetry("fourth", tChan, resChan2, t)
	expectFromRetry(component.ErrFailedSend, mOut.ts, t, "fourth")

	expectFromRetry(nil, mOut.ts, t, "fourth")
	ackForRetry(nil, resChan2, t)

	output.CloseAsync()
	if err = output.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}
