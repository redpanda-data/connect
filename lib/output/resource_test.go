package output

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeWriter struct {
	connected bool
	ts        []types.Transaction
	tsMut     sync.Mutex
	err       error
}

func (f *fakeWriter) Connected() bool {
	return f.connected
}

func (f *fakeWriter) WriteTransaction(ctx context.Context, ts types.Transaction) error {
	f.tsMut.Lock()
	f.ts = append(f.ts, ts)
	f.tsMut.Unlock()
	return f.err
}

func (f *fakeWriter) CloseAsync() {
}

func (f *fakeWriter) WaitForClose(time.Duration) error {
	return errors.New("not implemented")
}

//------------------------------------------------------------------------------

type fakeProcMgr struct {
	outs map[string]types.OutputWriter
}

func (f *fakeProcMgr) RegisterEndpoint(path, desc string, h http.HandlerFunc) {
}
func (f *fakeProcMgr) GetCache(name string) (types.Cache, error) {
	return nil, types.ErrCacheNotFound
}
func (f *fakeProcMgr) GetProcessor(name string) (types.Processor, error) {
	return nil, types.ErrProcessorNotFound
}
func (f *fakeProcMgr) GetOutput(name string) (types.OutputWriter, error) {
	if p, exists := f.outs[name]; exists {
		return p, nil
	}
	return nil, types.ErrOutputNotFound
}
func (f *fakeProcMgr) GetRateLimit(name string) (types.RateLimit, error) {
	return nil, types.ErrRateLimitNotFound
}
func (f *fakeProcMgr) GetPlugin(name string) (interface{}, error) {
	return nil, types.ErrPluginNotFound
}
func (f *fakeProcMgr) GetPipe(name string) (<-chan types.Transaction, error) {
	return nil, types.ErrPipeNotFound
}
func (f *fakeProcMgr) SetPipe(name string, prod <-chan types.Transaction)   {}
func (f *fakeProcMgr) UnsetPipe(name string, prod <-chan types.Transaction) {}

//------------------------------------------------------------------------------

func TestResourceOutput(t *testing.T) {
	out := &fakeWriter{}

	mgr := &fakeProcMgr{
		outs: map[string]types.OutputWriter{
			"foo": out,
		},
	}

	nConf := NewConfig()
	nConf.Type = "resource"
	nConf.Resource = "foo"

	p, err := New(nConf, mgr, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	assert.False(t, p.Connected())

	out.connected = true
	assert.True(t, p.Connected())

	tChan := make(chan types.Transaction)
	assert.NoError(t, p.Consume(tChan))

	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("foo:%v", i)
		select {
		case tChan <- types.NewTransaction(message.QuickBatch([][]byte{[]byte(msg)}), nil):
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}

	require.Eventually(t, func() bool {
		out.tsMut.Lock()
		ok := len(out.ts) == 10
		out.tsMut.Unlock()
		return ok
	}, time.Second*5, time.Millisecond*100)

	out.tsMut.Lock()
	for i := 0; i < 10; i++ {
		exp := fmt.Sprintf("foo:%v", i)
		require.NotNil(t, out.ts[i])
		require.NotNil(t, out.ts[i].Payload)
		assert.Equal(t, 1, out.ts[i].Payload.Len())
		assert.Equal(t, exp, string(out.ts[i].Payload.Get(0).Get()))
	}
	out.tsMut.Unlock()

	p.CloseAsync()
	assert.NoError(t, p.WaitForClose(time.Second))
}

func TestResourceBadName(t *testing.T) {
	mgr := &fakeProcMgr{
		outs: map[string]types.OutputWriter{},
	}

	conf := NewConfig()
	conf.Type = "resource"
	conf.Resource = "foo"

	_, err := NewResource(conf, mgr, log.Noop(), metrics.Noop())
	if err == nil {
		t.Error("expected error from bad resource")
	}
}

//------------------------------------------------------------------------------
