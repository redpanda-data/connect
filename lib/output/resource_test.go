package output

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
)

type fakeWriter struct {
	connected bool
	ts        types.Transaction
	tsMut     sync.Mutex
	err       error
}

func (f *fakeWriter) Connected() bool {
	return f.connected
}

func (f *fakeWriter) WriteTransaction(ctx context.Context, ts types.Transaction) error {
	f.tsMut.Lock()
	f.ts = ts
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
func (f *fakeProcMgr) GetCondition(name string) (types.Condition, error) {
	return nil, types.ErrConditionNotFound
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

func TestResourceProc(t *testing.T) {
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
	if err != nil {
		t.Fatal(err)
	}

	assert.False(t, p.Connected())

	out.connected = true
	assert.True(t, p.Connected())

	tChan := make(chan types.Transaction)
	assert.NoError(t, p.Consume(tChan))

	select {
	case tChan <- types.NewTransaction(message.New([][]byte{[]byte("foo")}), nil):
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	var resTs types.Transaction
	for i := 0; i < 10; i++ {
		out.tsMut.Lock()
		resTs = out.ts
		out.tsMut.Unlock()
		if resTs.Payload != nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	p.CloseAsync()
	assert.NoError(t, p.WaitForClose(time.Second))

	_ = assert.NotNil(t, resTs.Payload) &&
		assert.Equal(t, 1, resTs.Payload.Len()) &&
		assert.Equal(t, []byte("foo"), resTs.Payload.Get(0).Get())
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
