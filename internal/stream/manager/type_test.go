package manager

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/testutil"
	bmanager "github.com/benthosdev/benthos/v4/internal/manager"
	"github.com/benthosdev/benthos/v4/internal/stream"
)

func harmlessConf(tb testing.TB) stream.Config {
	tb.Helper()

	c, err := testutil.StreamFromYAML(`
input:
  generate:
    mapping: 'root = deleted()'
output:
  drop: {}
`)
	require.NoError(tb, err)

	return c
}

func TestTypeBasicOperations(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	res, err := bmanager.New(bmanager.NewResourceConfig())
	require.NoError(t, err)

	mgr := New(res)

	if err := mgr.Update(ctx, "foo", harmlessConf(t)); err == nil {
		t.Error("Expected error on empty update")
	}
	if _, err := mgr.Read("foo"); err == nil {
		t.Error("Expected error on empty read")
	}

	if err := mgr.Create("foo", harmlessConf(t)); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Create("foo", harmlessConf(t)); err == nil {
		t.Error("Expected error on duplicate create")
	}

	if info, err := mgr.Read("foo"); err != nil {
		t.Error(err)
	} else if !info.IsRunning() {
		t.Error("Stream not active")
	} else if act, exp := info.Config(), harmlessConf(t); !reflect.DeepEqual(act, exp) {
		t.Errorf("Unexpected config: %v != %v", act, exp)
	}

	newConf := harmlessConf(t)
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

	if exp, act := component.ErrTypeClosed, mgr.Create("foo", harmlessConf(t)); act != exp {
		t.Errorf("Unexpected error: %v != %v", act, exp)
	}
}

func TestTypeBasicClose(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	res, err := bmanager.New(bmanager.NewResourceConfig())
	require.NoError(t, err)

	mgr := New(res)

	conf := harmlessConf(t)
	if err := mgr.Create("foo", conf); err != nil {
		t.Fatal(err)
	}

	if err := mgr.Stop(ctx); err != nil {
		t.Error(err)
	}

	if exp, act := component.ErrTypeClosed, mgr.Create("foo", harmlessConf(t)); act != exp {
		t.Errorf("Unexpected error: %v != %v", act, exp)
	}
}
