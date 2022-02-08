package input

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
)

type fakeInput struct {
	connected bool
	ts        chan types.Transaction
}

func (f *fakeInput) Connected() bool {
	return f.connected
}

func (f *fakeInput) TransactionChan() <-chan types.Transaction {
	return f.ts
}

func (f *fakeInput) CloseAsync() {
}

func (f *fakeInput) WaitForClose(time.Duration) error {
	return errors.New("not implemented")
}

//------------------------------------------------------------------------------

type fakeProcMgr struct {
	ins map[string]types.Input
}

func (f *fakeProcMgr) RegisterEndpoint(path, desc string, h http.HandlerFunc) {
}
func (f *fakeProcMgr) GetCache(name string) (types.Cache, error) {
	return nil, component.ErrCacheNotFound
}
func (f *fakeProcMgr) GetProcessor(name string) (types.Processor, error) {
	return nil, component.ErrProcessorNotFound
}
func (f *fakeProcMgr) GetInput(name string) (types.Input, error) {
	if p, exists := f.ins[name]; exists {
		return p, nil
	}
	return nil, component.ErrInputNotFound
}
func (f *fakeProcMgr) GetRateLimit(name string) (types.RateLimit, error) {
	return nil, component.ErrRateLimitNotFound
}
func (f *fakeProcMgr) GetPipe(name string) (<-chan types.Transaction, error) {
	return nil, component.ErrPipeNotFound
}
func (f *fakeProcMgr) SetPipe(name string, prod <-chan types.Transaction)   {}
func (f *fakeProcMgr) UnsetPipe(name string, prod <-chan types.Transaction) {}

//------------------------------------------------------------------------------

func TestResourceProc(t *testing.T) {
	in := &fakeInput{}

	mgr := &fakeProcMgr{
		ins: map[string]types.Input{
			"foo": in,
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

	in.connected = true
	assert.True(t, p.Connected())

	assert.Nil(t, p.TransactionChan())

	in.ts = make(chan types.Transaction)
	assert.NotNil(t, p.TransactionChan())

	p.CloseAsync()
	assert.NoError(t, p.WaitForClose(time.Second))
}

func TestResourceBadName(t *testing.T) {
	mgr := &fakeProcMgr{
		ins: map[string]types.Input{},
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
