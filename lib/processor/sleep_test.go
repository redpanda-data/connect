package processor

import (
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/stretchr/testify/require"
)

func TestSleep(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeSleep
	conf.Sleep.Duration = "1ns"

	slp, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgIn := message.QuickBatch(nil)
	msgsOut, res := slp.ProcessMessage(msgIn)
	if res != nil {
		t.Fatal(res)
	}

	if exp, act := msgIn, msgsOut[0]; exp != act {
		t.Errorf("Wrong message returned: %v != %v", act, exp)
	}
}

func TestSleepExit(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeSleep
	conf.Sleep.Duration = "10s"

	slp, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	doneChan := make(chan struct{})
	go func() {
		_, _ = slp.ProcessMessage(message.QuickBatch(nil))
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
	conf := NewConfig()
	conf.Type = TypeSleep
	conf.Sleep.Duration = "200ms"

	slp, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	tBefore := time.Now()
	batches, err := slp.ProcessMessage(message.QuickBatch(nil))
	tAfter := time.Now()
	require.NoError(t, err)
	require.Len(t, batches, 1)

	if dur := tAfter.Sub(tBefore); dur < (time.Millisecond * 200) {
		t.Errorf("Message didn't take long enough")
	}
}

func TestSleepInterpolated(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeSleep
	conf.Sleep.Duration = "${!json(\"foo\")}ms"

	slp, err := New(conf, nil, log.Noop(), metrics.Noop())
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
