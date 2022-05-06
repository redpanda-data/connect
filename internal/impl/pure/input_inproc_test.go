package pure_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	bmock "github.com/benthosdev/benthos/v4/internal/bundle/mock"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/manager"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/old/input"
)

func TestInprocDryRun(t *testing.T) {
	t.Parallel()

	mgr, err := manager.New(manager.NewResourceConfig(), nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	mgr.SetPipe("foo", make(chan message.Transaction))

	conf := input.NewConfig()
	conf.Type = "inproc"
	conf.Inproc = "foo"

	ip, err := mgr.NewInput(conf)
	require.NoError(t, err)

	<-time.After(time.Millisecond * 100)

	ip.CloseAsync()
	if err = ip.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestInprocDryRunNoConn(t *testing.T) {
	t.Parallel()

	conf := input.NewConfig()
	conf.Type = "inproc"
	conf.Inproc = "foo"

	ip, err := bmock.NewManager().NewInput(conf)
	require.NoError(t, err)

	<-time.After(time.Millisecond * 100)

	ip.CloseAsync()
	if err = ip.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}
