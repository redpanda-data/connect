package pure_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/manager"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestInprocDryRun(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	t.Parallel()

	mgr, err := manager.New(manager.NewResourceConfig())
	if err != nil {
		t.Fatal(err)
	}

	mgr.SetPipe("foo", make(chan message.Transaction))

	ip := testInput(t, `
inproc: foo
`)

	<-time.After(time.Millisecond * 100)

	ip.TriggerStopConsuming()
	if err = ip.WaitForClose(ctx); err != nil {
		t.Error(err)
	}
}

func TestInprocDryRunNoConn(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	t.Parallel()

	ip := testInput(t, `
inproc: foo
`)

	<-time.After(time.Millisecond * 100)

	ip.TriggerStopConsuming()
	require.NoError(t, ip.WaitForClose(ctx))
}
