package manager

import (
	"reflect"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/lib/log"
	bmanager "github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/stream"
	"github.com/stretchr/testify/require"
)

func harmlessConf() stream.Config {
	c := stream.NewConfig()
	c.Input.Type = "http_server"
	c.Output.Type = "http_server"
	return c
}

func TestTypeBasicOperations(t *testing.T) {
	res, err := bmanager.NewV2(bmanager.NewResourceConfig(), mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	mgr := New(
		OptSetLogger(log.Noop()),
		OptSetStats(metrics.Noop()),
		OptSetManager(res),
	)

	if err := mgr.Update("foo", harmlessConf(), time.Second); err == nil {
		t.Error("Expected error on empty update")
	}
	if _, err := mgr.Read("foo"); err == nil {
		t.Error("Expected error on empty read")
	}

	if err := mgr.Create("foo", harmlessConf()); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Create("foo", harmlessConf()); err == nil {
		t.Error("Expected error on duplicate create")
	}

	if info, err := mgr.Read("foo"); err != nil {
		t.Error(err)
	} else if !info.IsRunning() {
		t.Error("Stream not active")
	} else if act, exp := info.Config(), harmlessConf(); !reflect.DeepEqual(act, exp) {
		t.Errorf("Unexpected config: %v != %v", act, exp)
	}

	newConf := harmlessConf()
	newConf.Buffer.Type = "memory"

	if err := mgr.Update("foo", newConf, time.Second); err != nil {
		t.Error(err)
	}

	if info, err := mgr.Read("foo"); err != nil {
		t.Error(err)
	} else if !info.IsRunning() {
		t.Error("Stream not active")
	} else if act, exp := info.Config(), newConf; !reflect.DeepEqual(act, exp) {
		t.Errorf("Unexpected config: %v != %v", act, exp)
	}

	if err := mgr.Delete("foo", time.Second); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Delete("foo", time.Second); err == nil {
		t.Error("Expected error on duplicate delete")
	}

	if err := mgr.Stop(time.Second * 5); err != nil {
		t.Error(err)
	}

	if exp, act := component.ErrTypeClosed, mgr.Create("foo", harmlessConf()); act != exp {
		t.Errorf("Unexpected error: %v != %v", act, exp)
	}
}

func TestTypeBasicClose(t *testing.T) {
	res, err := bmanager.NewV2(bmanager.NewResourceConfig(), mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	mgr := New(
		OptSetLogger(log.Noop()),
		OptSetStats(metrics.Noop()),
		OptSetManager(res),
	)

	conf := harmlessConf()
	conf.Output.Type = output.TypeNanomsg

	if err := mgr.Create("foo", conf); err != nil {
		t.Fatal(err)
	}

	if err := mgr.Stop(time.Second); err != nil {
		t.Error(err)
	}

	if exp, act := component.ErrTypeClosed, mgr.Create("foo", harmlessConf()); act != exp {
		t.Errorf("Unexpected error: %v != %v", act, exp)
	}
}
