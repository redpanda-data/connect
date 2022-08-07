package pure_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestResourceOutput(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	var outLock sync.Mutex
	var outTS []message.Transaction

	mgr := mock.NewManager()
	mgr.Outputs["foo"] = func(c context.Context, t message.Transaction) error {
		outLock.Lock()
		defer outLock.Unlock()
		outTS = append(outTS, t)
		return nil
	}

	nConf := output.NewConfig()
	nConf.Type = "resource"
	nConf.Resource = "foo"

	p, err := mgr.NewOutput(nConf)
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
		assert.Equal(t, exp, string(outTS[i].Payload.Get(0).AsBytes()))
	}
	outLock.Unlock()

	p.TriggerCloseNow()
	assert.NoError(t, p.WaitForClose(tCtx))
}

func TestOutputResourceBadName(t *testing.T) {
	mgr := mock.NewManager()

	conf := output.NewConfig()
	conf.Type = "resource"
	conf.Resource = "foo"

	_, err := mgr.NewOutput(conf)
	if err == nil {
		t.Error("expected error from bad resource")
	}
}
