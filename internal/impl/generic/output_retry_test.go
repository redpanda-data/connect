package generic

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	bmock "github.com/benthosdev/benthos/v4/internal/bundle/mock"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
	ooutput "github.com/benthosdev/benthos/v4/internal/old/output"
)

func TestRetryConfigErrs(t *testing.T) {
	conf := ooutput.NewConfig()
	conf.Type = "retry"

	if _, err := bundle.AllOutputs.Init(conf, bmock.NewManager()); err == nil {
		t.Error("Expected error from bad retry output")
	}

	oConf := ooutput.NewConfig()
	conf.Retry.Output = &oConf
	conf.Retry.Backoff.InitialInterval = "not a time period"

	if _, err := bundle.AllOutputs.Init(conf, bmock.NewManager()); err == nil {
		t.Error("Expected error from bad initial period")
	}
}

func TestRetryBasic(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	conf := ooutput.NewConfig()
	conf.Type = "retry"

	childConf := ooutput.NewConfig()
	conf.Retry.Output = &childConf

	output, err := bundle.AllOutputs.Init(conf, bmock.NewManager())
	if err != nil {
		t.Fatal(err)
	}

	ret, ok := output.(*indefiniteRetry)
	if !ok {
		t.Fatalf("Failed to cast: %T", output)
	}

	mOut := &mock.OutputChanneled{}
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
	case tran = <-mOut.TChan:
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	if tran.Payload != testMsg {
		t.Error("Wrong payload returned")
	}
	require.NoError(t, tran.Ack(ctx, nil))

	select {
	case res := <-resChan:
		if err = res; err != nil {
			t.Error(err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	output.CloseAsync()
	require.NoError(t, output.WaitForClose(time.Second*30))
}

func TestRetrySadPath(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	conf := ooutput.NewConfig()
	conf.Type = "retry"

	childConf := ooutput.NewConfig()
	conf.Retry.Output = &childConf
	conf.Retry.Backoff.InitialInterval = "10us"
	conf.Retry.Backoff.MaxInterval = "10us"

	output, err := bundle.AllOutputs.Init(conf, bmock.NewManager())
	if err != nil {
		t.Fatal(err)
	}

	ret, ok := output.(*indefiniteRetry)
	if !ok {
		t.Fatal("Failed to cast")
	}

	mOut := &mock.OutputChanneled{}
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
		case tran = <-mOut.TChan:
		case <-resChan:
			t.Fatal("Received response not retry")
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}

		if tran.Payload != testMsg {
			t.Error("Wrong payload returned")
		}
		require.NoError(t, tran.Ack(ctx, component.ErrFailedSend))
	}

	select {
	case tran = <-mOut.TChan:
	case <-resChan:
		t.Fatal("Received response not retry")
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	if tran.Payload != testMsg {
		t.Error("Wrong payload returned")
	}
	require.NoError(t, tran.Ack(ctx, nil))

	select {
	case res := <-resChan:
		if err = res; err != nil {
			t.Error(err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	output.CloseAsync()
	require.NoError(t, output.WaitForClose(time.Second*30))
}

func expectFromRetry(
	resReturn error,
	tChan <-chan message.Transaction,
	t *testing.T,
	responsesSlice ...string) {
	t.Helper()

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	responses := map[string]struct{}{}
	for _, k := range responsesSlice {
		responses[k] = struct{}{}
	}

	resFns := []func(context.Context, error) error{}

	for len(responses) > 0 {
		select {
		case tran := <-tChan:
			act := string(tran.Payload.Get(0).Get())
			if _, exists := responses[act]; exists {
				delete(responses, act)
			} else {
				t.Errorf("Wrong result: %v", act)
			}
			resFns = append(resFns, tran.Ack)
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}
	}

	for _, resFn := range resFns {
		require.NoError(t, resFn(ctx, resReturn))
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
	conf := ooutput.NewConfig()
	conf.Type = "retry"

	childConf := ooutput.NewConfig()
	conf.Retry.Output = &childConf
	conf.Retry.Backoff.InitialInterval = "10us"
	conf.Retry.Backoff.MaxInterval = "10us"

	output, err := bundle.AllOutputs.Init(conf, bmock.NewManager())
	if err != nil {
		t.Fatal(err)
	}

	ret, ok := output.(*indefiniteRetry)
	if !ok {
		t.Fatal("Failed to cast")
	}

	mOut := &mock.OutputChanneled{}
	ret.wrapped = mOut

	tChan := make(chan message.Transaction)
	if err = ret.Consume(tChan); err != nil {
		t.Fatal(err)
	}

	resChan1, resChan2 := make(chan error), make(chan error)
	sendForRetry("first", tChan, resChan1, t)
	expectFromRetry(component.ErrFailedSend, mOut.TChan, t, "first")

	sendForRetry("second", tChan, resChan2, t)
	expectFromRetry(component.ErrFailedSend, mOut.TChan, t, "first", "second")

	select {
	case tChan <- message.NewTransaction(nil, nil):
		t.Fatal("Accepted transaction during retry loop")
	default:
	}
	expectFromRetry(nil, mOut.TChan, t, "first", "second")
	ackForRetry(nil, resChan1, t)
	ackForRetry(nil, resChan2, t)

	sendForRetry("third", tChan, resChan1, t)
	expectFromRetry(nil, mOut.TChan, t, "third")
	ackForRetry(nil, resChan1, t)

	sendForRetry("fourth", tChan, resChan2, t)
	expectFromRetry(component.ErrFailedSend, mOut.TChan, t, "fourth")

	expectFromRetry(nil, mOut.TChan, t, "fourth")
	ackForRetry(nil, resChan2, t)

	output.CloseAsync()
	require.NoError(t, output.WaitForClose(time.Second*30))
}
