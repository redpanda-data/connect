package io_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/testutil"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"

	_ "github.com/benthosdev/benthos/v4/internal/impl/io"
)

func readMsg(t *testing.T, tranChan <-chan message.Transaction) message.Batch {
	t.Helper()

	tCtx, done := context.WithTimeout(context.Background(), time.Second*10)
	defer done()

	select {
	case tran := <-tranChan:
		require.NoError(t, tran.Ack(tCtx, nil))
		return tran.Payload
	case <-time.After(time.Second * 5):
	}
	t.Fatal("timed out")
	return nil
}

func testInput(t testing.TB, confPattern string, args ...any) input.Streamed {
	iConf, err := testutil.InputFromYAML(fmt.Sprintf(confPattern, args...))
	require.NoError(t, err)

	i, err := mock.NewManager().NewInput(iConf)
	require.NoError(t, err)

	return i
}

func TestSubprocessBasic(t *testing.T) {
	filePath := testProgram(t, `package main

import (
	"fmt"
)

func main() {
	fmt.Println("foo")
	fmt.Println("bar")
	fmt.Println("baz")
}
`)

	i := testInput(t, `
subprocess:
  name: go
  args: [ "run", "%v" ]
`, filePath)

	msg := readMsg(t, i.TransactionChan())
	assert.Equal(t, 1, msg.Len())
	assert.Equal(t, "foo", string(msg.Get(0).AsBytes()))

	msg = readMsg(t, i.TransactionChan())
	assert.Equal(t, 1, msg.Len())
	assert.Equal(t, "bar", string(msg.Get(0).AsBytes()))

	msg = readMsg(t, i.TransactionChan())
	assert.Equal(t, 1, msg.Len())
	assert.Equal(t, "baz", string(msg.Get(0).AsBytes()))

	select {
	case _, open := <-i.TransactionChan():
		assert.False(t, open)
	case <-time.After(time.Second):
		t.Error("timed out")
	}
}

func TestSubprocessRestarted(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	filePath := testProgram(t, `package main

import (
	"fmt"
)

func main() {
	fmt.Println("foo")
	fmt.Println("bar")
	fmt.Println("baz")
}
`)

	i := testInput(t, `
subprocess:
  name: go
  args: [ "run", "%v" ]
  restart_on_exit: true
`, filePath)

	msg := readMsg(t, i.TransactionChan())
	assert.Equal(t, 1, msg.Len())
	assert.Equal(t, "foo", string(msg.Get(0).AsBytes()))

	msg = readMsg(t, i.TransactionChan())
	assert.Equal(t, 1, msg.Len())
	assert.Equal(t, "bar", string(msg.Get(0).AsBytes()))

	msg = readMsg(t, i.TransactionChan())
	assert.Equal(t, 1, msg.Len())
	assert.Equal(t, "baz", string(msg.Get(0).AsBytes()))

	msg = readMsg(t, i.TransactionChan())
	assert.Equal(t, 1, msg.Len())
	assert.Equal(t, "foo", string(msg.Get(0).AsBytes()))

	msg = readMsg(t, i.TransactionChan())
	assert.Equal(t, 1, msg.Len())
	assert.Equal(t, "bar", string(msg.Get(0).AsBytes()))

	msg = readMsg(t, i.TransactionChan())
	assert.Equal(t, 1, msg.Len())
	assert.Equal(t, "baz", string(msg.Get(0).AsBytes()))

	i.TriggerStopConsuming()
	require.NoError(t, i.WaitForClose(ctx))
}

func TestSubprocessCloseInBetween(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	filePath := testProgram(t, `package main

import (
	"fmt"
)

func main() {
	i := 0
	for {
		fmt.Printf("foo:%v\n", i)
		i++
	}
}
`)

	i := testInput(t, `
subprocess:
  name: go
  args: [ "run", "%v" ]
`, filePath)

	msg := readMsg(t, i.TransactionChan())
	assert.Equal(t, 1, msg.Len())
	assert.Equal(t, "foo:0", string(msg.Get(0).AsBytes()))

	msg = readMsg(t, i.TransactionChan())
	assert.Equal(t, 1, msg.Len())
	assert.Equal(t, "foo:1", string(msg.Get(0).AsBytes()))

	i.TriggerStopConsuming()
	require.NoError(t, i.WaitForClose(ctx))
}
