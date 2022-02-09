package output

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResourceOutput(t *testing.T) {
	var outLock sync.Mutex
	var outTS []message.Transaction

	mgr := mock.NewManager()
	mgr.Outputs["foo"] = func(c context.Context, t message.Transaction) error {
		outLock.Lock()
		defer outLock.Unlock()
		outTS = append(outTS, t)
		return nil
	}

	nConf := NewConfig()
	nConf.Type = "resource"
	nConf.Resource = "foo"

	p, err := New(nConf, mgr, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	assert.True(t, p.Connected())

	tChan := make(chan message.Transaction)
	assert.NoError(t, p.Consume(tChan))

	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("foo:%v", i)
		select {
		case tChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte(msg)}), nil):
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}

	require.Eventually(t, func() bool {
		outLock.Lock()
		ok := len(outTS) == 10
		outLock.Unlock()
		return ok
	}, time.Second*5, time.Millisecond*100)

	outLock.Lock()
	for i := 0; i < 10; i++ {
		exp := fmt.Sprintf("foo:%v", i)
		require.NotNil(t, outTS[i])
		require.NotNil(t, outTS[i].Payload)
		assert.Equal(t, 1, outTS[i].Payload.Len())
		assert.Equal(t, exp, string(outTS[i].Payload.Get(0).Get()))
	}
	outLock.Unlock()

	p.CloseAsync()
	assert.NoError(t, p.WaitForClose(time.Second))
}

func TestResourceBadName(t *testing.T) {
	mgr := mock.NewManager()

	conf := NewConfig()
	conf.Type = "resource"
	conf.Resource = "foo"

	_, err := NewResource(conf, mgr, log.Noop(), metrics.Noop())
	if err == nil {
		t.Error("expected error from bad resource")
	}
}

//------------------------------------------------------------------------------
