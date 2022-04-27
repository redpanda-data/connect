package pure_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/bundle/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/old/processor"
)

func TestSleep(t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "sleep"
	conf.Sleep.Duration = "1ns"

	slp, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgIn := message.QuickBatch([][]byte{[]byte("hello world")})
	msgsOut, res := slp.ProcessMessage(msgIn)
	if res != nil {
		t.Fatal(res)
	}

	if exp, act := msgIn, msgsOut[0]; exp != act {
		t.Errorf("Wrong message returned: %v != %v", act, exp)
	}
}

func TestSleepExit(t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "sleep"
	conf.Sleep.Duration = "10s"

	slp, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	doneChan := make(chan struct{})
	go func() {
		_, _ = slp.ProcessMessage(message.QuickBatch([][]byte{[]byte("hello world")}))
		close(doneChan)
	}()

	slp.CloseAsync()
	slp.CloseAsync()
	select {
	case <-doneChan:
	case <-time.After(time.Second):
		t.Error("took too long")
	}
}

func TestSleep200Millisecond(t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "sleep"
	conf.Sleep.Duration = "200ms"

	slp, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	tBefore := time.Now()
	batches, err := slp.ProcessMessage(message.QuickBatch([][]byte{[]byte("hello world")}))
	tAfter := time.Now()
	require.NoError(t, err)
	require.Len(t, batches, 1)

	if dur := tAfter.Sub(tBefore); dur < (time.Millisecond * 200) {
		t.Errorf("Message didn't take long enough")
	}
}

func TestSleepInterpolated(t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "sleep"
	conf.Sleep.Duration = "${!json(\"foo\")}ms"

	slp, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	tBefore := time.Now()
	batches, err := slp.ProcessMessage(message.QuickBatch([][]byte{
		[]byte(`{"foo":200}`),
	}))
	tAfter := time.Now()
	require.NoError(t, err)
	require.Len(t, batches, 1)

	if dur := tAfter.Sub(tBefore); dur < (time.Millisecond * 200) {
		t.Errorf("Message didn't take long enough")
	}
}
