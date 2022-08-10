package manager

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component"
	bmanager "github.com/benthosdev/benthos/v4/internal/manager"
	"github.com/benthosdev/benthos/v4/internal/stream"
)

func harmlessConf() stream.Config {
	c := stream.NewConfig()
	c.Input.Type = "generate"
	c.Input.Generate.Mapping = "root = deleted()"
	c.Output.Type = "drop"
	return c
}

func TestTypeBasicOperations(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	res, err := bmanager.New(bmanager.NewResourceConfig())
	require.NoError(t, err)

	mgr := New(res)

	if err := mgr.Update(ctx, "foo", harmlessConf()); err == nil {
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

	if err := mgr.Update(ctx, "foo", newConf); err != nil {
		t.Error(err)
	}

	if info, err := mgr.Read("foo"); err != nil {
		t.Error(err)
	} else if !info.IsRunning() {
		t.Error("Stream not active")
	} else if act, exp := info.Config(), newConf; !reflect.DeepEqual(act, exp) {
		t.Errorf("Unexpected config: %v != %v", act, exp)
	}

	if err := mgr.Delete(ctx, "foo"); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Delete(ctx, "foo"); err == nil {
		t.Error("Expected error on duplicate delete")
	}

	if err := mgr.Stop(ctx); err != nil {
		t.Error(err)
	}

	if exp, act := component.ErrTypeClosed, mgr.Create("foo", harmlessConf()); act != exp {
		t.Errorf("Unexpected error: %v != %v", act, exp)
	}
}

func TestTypeBasicClose(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	res, err := bmanager.New(bmanager.NewResourceConfig())
	require.NoError(t, err)

	mgr := New(res)

	conf := harmlessConf()
	if err := mgr.Create("foo", conf); err != nil {
		t.Fatal(err)
	}

	if err := mgr.Stop(ctx); err != nil {
		t.Error(err)
	}

	if exp, act := component.ErrTypeClosed, mgr.Create("foo", harmlessConf()); act != exp {
		t.Errorf("Unexpected error: %v != %v", act, exp)
	}
}
