package pure_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/manager"
	"github.com/benthosdev/benthos/v4/internal/message"

	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

func TestInproc(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	_, err = mgr.GetPipe("foo")
	assert.Equal(t, err, component.ErrPipeNotFound)

	conf := output.NewConfig()
	conf.Type = "inproc"
	conf.Inproc = "foo"

	ip, err := mgr.NewOutput(conf)
	require.NoError(t, err)

	tinchan := make(chan message.Transaction)
	require.NoError(t, ip.Consume(tinchan))

	select {
	case tinchan <- message.NewTransaction(nil, nil):
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	var toutchan <-chan message.Transaction
	if toutchan, err = mgr.GetPipe("foo"); err != nil {
		t.Error(err)
	}

	select {
	case <-toutchan:
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	ip.TriggerCloseNow()
	require.NoError(t, ip.WaitForClose(tCtx))

	select {
	case _, open := <-toutchan:
		assert.False(t, open)
	case <-time.After(time.Second):
		t.Error("Timed out")
	}
	_, err = mgr.GetPipe("foo")
	assert.Equal(t, err, component.ErrPipeNotFound)
}
