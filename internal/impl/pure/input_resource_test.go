package pure_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
)

func TestResourceInput(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	mgr := mock.NewManager()
	mgr.Inputs["foo"] = mock.NewInput(nil)

	nConf := input.NewConfig()
	nConf.Type = "resource"
	nConf.Resource = "foo"

	p, err := mgr.NewInput(nConf)
	require.NoError(t, err)

	assert.NotNil(t, p.TransactionChan())

	p.TriggerStopConsuming()
	assert.NoError(t, p.WaitForClose(ctx))
}

func TestResourceInputBadName(t *testing.T) {
	conf := input.NewConfig()
	conf.Type = "resource"
	conf.Resource = "foo"

	_, err := mock.NewManager().NewInput(conf)
	if err == nil {
		t.Error("expected error from bad resource")
	}
}
