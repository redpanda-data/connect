package output

import (
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

func TestRetryConfigErrs(t *testing.T) {
	conf := NewConfig()
	conf.Type = "retry"

	if _, err := New(conf, nil, log.Noop(), metrics.Noop()); err == nil {
		t.Error("Expected error from bad retry output")
	}

	oConf := NewConfig()
	conf.Retry.Output = &oConf
	conf.Retry.Backoff.InitialInterval = "not a time period"

	if _, err := New(conf, nil, log.Noop(), metrics.Noop()); err == nil {
		t.Error("Expected error from bad initial period")
	}
}

func TestRetryBasic(t *testing.T) {
	conf := NewConfig()

	childConf := NewConfig()
	conf.Retry.Output = &childConf

	output, err := NewRetry(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	ret, ok := output.(*Retry)
	if !ok {
		t.Fatal("Failed to cast")
	}

	mOut := &mockOutput{
		ts: make(chan types.Transaction),
	}
	ret.wrapped = mOut

	tChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	if err = ret.Consume(tChan); err != nil {
		t.Fatal(err)
	}

	testMsg := message.QuickBatch(nil)
	go func() {
		select {
		case tChan <- types.NewTransaction(testMsg, resChan):
		case <-time.After(time.Second):
			t.Error("timed out")
		}
	}()

	var tran types.Transaction
	select {
	case tran = <-mOut.ts:
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	if tran.Payload != testMsg {
		t.Error("Wrong payload returned")
	}

	select {
	case tran.ResponseChan <- response.NewAck():
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	select {
	case res := <-resChan:
		if err = res.Error(); err != nil {
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

	output, err := NewRetry(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	ret, ok := output.(*Retry)
	if !ok {
		t.Fatal("Failed to cast")
	}

	mOut := &mockOutput{
		ts: make(chan types.Transaction),
	}
	ret.wrapped = mOut

	tChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	if err = ret.Consume(tChan); err != nil {
		t.Fatal(err)
	}

	testMsg := message.QuickBatch(nil)
	tran := types.NewTransaction(testMsg, resChan)

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
		case tran.ResponseChan <- response.NewNoack():
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
	case tran.ResponseChan <- response.NewAck():
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	select {
	case res := <-resChan:
		if err = res.Error(); err != nil {
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
	resReturn types.Response,
	tChan <-chan types.Transaction,
	t *testing.T,
	responsesSlice ...string) {
	t.Helper()

	responses := map[string]struct{}{}
	for _, k := range responsesSlice {
		responses[k] = struct{}{}
	}

	resChans := []chan<- types.Response{}

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
	tChan chan types.Transaction,
	resChan chan types.Response,
	t *testing.T,
) {
	t.Helper()

	select {
	case tChan <- types.NewTransaction(message.QuickBatch(
		[][]byte{[]byte(value)},
	), resChan):
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
}

func ackForRetry(
	exp types.Response,
	resChan <-chan types.Response,
	t *testing.T,
) {
	t.Helper()

	select {
	case res := <-resChan:
		if res.Error() != exp.Error() {
			t.Errorf("Unexpected response error: %v != %v", res.Error(), exp.Error())
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

	output, err := NewRetry(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	ret, ok := output.(*Retry)
	if !ok {
		t.Fatal("Failed to cast")
	}

	mOut := &mockOutput{
		ts: make(chan types.Transaction),
	}
	ret.wrapped = mOut

	tChan := make(chan types.Transaction)
	if err = ret.Consume(tChan); err != nil {
		t.Fatal(err)
	}

	resChan1, resChan2 := make(chan types.Response), make(chan types.Response)
	sendForRetry("first", tChan, resChan1, t)
	expectFromRetry(response.NewNoack(), mOut.ts, t, "first")

	sendForRetry("second", tChan, resChan2, t)
	expectFromRetry(response.NewNoack(), mOut.ts, t, "first", "second")

	select {
	case tChan <- types.NewTransaction(nil, nil):
		t.Fatal("Accepted transaction during retry loop")
	default:
	}
	expectFromRetry(response.NewAck(), mOut.ts, t, "first", "second")
	ackForRetry(response.NewAck(), resChan1, t)
	ackForRetry(response.NewAck(), resChan2, t)

	sendForRetry("third", tChan, resChan1, t)
	expectFromRetry(response.NewAck(), mOut.ts, t, "third")
	ackForRetry(response.NewAck(), resChan1, t)

	sendForRetry("fourth", tChan, resChan2, t)
	expectFromRetry(response.NewNoack(), mOut.ts, t, "fourth")

	expectFromRetry(response.NewAck(), mOut.ts, t, "fourth")
	ackForRetry(response.NewAck(), resChan2, t)

	output.CloseAsync()
	if err = output.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}
