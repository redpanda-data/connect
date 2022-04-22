package pure_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bmock "github.com/benthosdev/benthos/v4/internal/bundle/mock"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	oinput "github.com/benthosdev/benthos/v4/internal/old/input"
)

func TestResourceProc(t *testing.T) {
	mgr := bmock.NewManager()
	mgr.Inputs["foo"] = mock.NewInput(nil)

	nConf := oinput.NewConfig()
	nConf.Type = "resource"
	nConf.Resource = "foo"

	p, err := mgr.NewInput(nConf)
	require.NoError(t, err)

	assert.NotNil(t, p.TransactionChan())

	p.CloseAsync()
	assert.NoError(t, p.WaitForClose(time.Second))
}

func TestResourceBadName(t *testing.T) {
	conf := oinput.NewConfig()
	conf.Type = "resource"
	conf.Resource = "foo"

	_, err := bmock.NewManager().NewInput(conf)
	if err == nil {
		t.Error("expected error from bad resource")
	}
}
