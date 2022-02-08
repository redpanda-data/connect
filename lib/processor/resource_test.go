package processor

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestResourceProc(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeBloblang
	conf.Bloblang = `root = "foo: " + content()`

	resProc, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	mgr := mock.NewManager()
	mgr.Processors["foo"] = func(b *message.Batch) ([]*message.Batch, error) {
		msgs, res := resProc.ProcessMessage(b)
		if res != nil {
			return nil, res.AckError()
		}
		return msgs, nil
	}

	nConf := NewConfig()
	nConf.Type = "resource"
	nConf.Resource = "foo"

	p, err := New(nConf, mgr, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := p.ProcessMessage(message.QuickBatch([][]byte{[]byte("bar")}))
	if res != nil {
		t.Fatal(res.AckError())
	}
	if len(msgs) != 1 {
		t.Error("Expected only 1 message")
	}
	if exp, act := "foo: bar", string(msgs[0].Get(0).Get()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
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
