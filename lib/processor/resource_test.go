package processor

import (
	"net/http"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

type fakeProcMgr struct {
	procs map[string]Type
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
	if p, exists := f.procs[name]; exists {
		return p, nil
	}
	return nil, types.ErrProcessorNotFound
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

func TestResourceProc(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeBloblang
	conf.Bloblang = `root = "foo: " + content()`

	resProc, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	mgr := &fakeProcMgr{
		procs: map[string]Type{
			"foo": resProc,
		},
	}

	nConf := NewConfig()
	nConf.Type = "resource"
	nConf.Resource = "foo"

	p, err := New(nConf, mgr, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := p.ProcessMessage(message.New([][]byte{[]byte("bar")}))
	if res != nil {
		t.Fatal(res.Error())
	}
	if len(msgs) != 1 {
		t.Error("Expected only 1 message")
	}
	if exp, act := "foo: bar", string(msgs[0].Get(0).Get()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
}

func TestResourceBadName(t *testing.T) {
	mgr := &fakeProcMgr{
		procs: map[string]Type{},
	}

	conf := NewConfig()
	conf.Type = "resource"
	conf.Resource = "foo"

	_, err := NewResource(conf, mgr, log.Noop(), metrics.Noop())
	if err == nil {
		t.Error("expected error from bad resource")
	}
}
