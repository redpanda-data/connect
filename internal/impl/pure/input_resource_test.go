package pure_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
)

func TestResourceInput(t *testing.T) {
	mgr := mock.NewManager()
	mgr.Inputs["foo"] = mock.NewInput(nil)

	nConf := input.NewConfig()
	nConf.Type = "resource"
	nConf.Resource = "foo"

	p, err := mgr.NewInput(nConf)
	require.NoError(t, err)

	assert.NotNil(t, p.TransactionChan())

	p.CloseAsync()
	assert.NoError(t, p.WaitForClose(time.Second))
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
