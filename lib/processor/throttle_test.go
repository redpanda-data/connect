package processor

import (
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/stretchr/testify/require"
)

func TestThrottle(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeThrottle
	conf.Throttle.Period = "1ns"

	throt, err := New(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgIn := message.QuickBatch(nil)
	msgsOut, res := throt.ProcessMessage(msgIn)
	if res != nil {
		t.Fatal(res)
	}

	if exp, act := msgIn, msgsOut[0]; exp != act {
		t.Errorf("Wrong message returned: %v != %v", act, exp)
	}
}

func TestThrottle200Millisecond(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeThrottle
	conf.Throttle.Period = "200ms"

	throt, err := New(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	tBefore := time.Now()
	batches, err := throt.ProcessMessage(message.QuickBatch(nil))
	require.NoError(t, err)
	require.Len(t, batches, 1)
	tBetween := time.Now()
	batches, err = throt.ProcessMessage(message.QuickBatch(nil))
	require.NoError(t, err)
	require.Len(t, batches, 1)
	tAfter := time.Now()

	if dur := tBetween.Sub(tBefore); dur > (time.Millisecond * 50) {
		t.Errorf("First message took too long")
	}
	if dur := tAfter.Sub(tBetween); dur < (time.Millisecond * 200) {
		t.Errorf("First message didn't take long enough")
	}
}

func TestThrottleBadPeriod(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeThrottle
	conf.Throttle.Period = "1gfdfgfdns"

	_, err := New(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	if err == nil {
		t.Error("Expected error from bad duration")
	}
}
