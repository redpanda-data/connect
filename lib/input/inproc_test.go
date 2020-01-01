package input

import (
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func TestInprocDryRun(t *testing.T) {
	t.Parallel()

	mgr, err := manager.New(manager.NewConfig(), nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	mgr.SetPipe("foo", make(chan types.Transaction))

	conf := NewConfig()
	conf.Inproc = "foo"

	var ip Type
	if ip, err = NewInproc(conf, mgr, log.Noop(), metrics.Noop()); err != nil {
		t.Fatal(err)
	}

	<-time.After(time.Millisecond * 100)

	ip.CloseAsync()
	if err = ip.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestInprocDryRunNoConn(t *testing.T) {
	t.Parallel()

	mgr, err := manager.New(manager.NewConfig(), nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	conf := NewConfig()
	conf.Inproc = "foo"

	var ip Type
	if ip, err = NewInproc(conf, mgr, log.Noop(), metrics.Noop()); err != nil {
		t.Fatal(err)
	}

	<-time.After(time.Millisecond * 100)

	ip.CloseAsync()
	if err = ip.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

//------------------------------------------------------------------------------
