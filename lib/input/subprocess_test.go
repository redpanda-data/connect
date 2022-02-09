package input

import (
	"os"
	"path"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testProgram(t *testing.T, program string) string {
	t.Helper()

	dir := t.TempDir()

	pathStr := path.Join(dir, "main.go")
	require.NoError(t, os.WriteFile(pathStr, []byte(program), 0o666))

	return pathStr
}

func readMsg(t *testing.T, tranChan <-chan message.Transaction) *message.Batch {
	t.Helper()
	select {
	case tran := <-tranChan:
		select {
		case tran.ResponseChan <- response.NewError(nil):
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}
		return tran.Payload
	case <-time.After(time.Second * 5):
	}
	t.Fatal("timed out")
	return nil
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

	conf := NewConfig()
	conf.Type = TypeSubprocess
	conf.Subprocess.Name = "go"
	conf.Subprocess.Args = []string{"run", filePath}

	i, err := New(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	msg := readMsg(t, i.TransactionChan())
	assert.Equal(t, 1, msg.Len())
	assert.Equal(t, "foo", string(msg.Get(0).Get()))

	msg = readMsg(t, i.TransactionChan())
	assert.Equal(t, 1, msg.Len())
	assert.Equal(t, "bar", string(msg.Get(0).Get()))

	msg = readMsg(t, i.TransactionChan())
	assert.Equal(t, 1, msg.Len())
	assert.Equal(t, "baz", string(msg.Get(0).Get()))

	select {
	case _, open := <-i.TransactionChan():
		assert.False(t, open)
	case <-time.After(time.Second):
		t.Error("timed out")
	}
}

func TestSubprocessRestarted(t *testing.T) {
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

	conf := NewConfig()
	conf.Type = TypeSubprocess
	conf.Subprocess.Name = "go"
	conf.Subprocess.RestartOnExit = true
	conf.Subprocess.Args = []string{"run", filePath}

	i, err := New(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	msg := readMsg(t, i.TransactionChan())
	assert.Equal(t, 1, msg.Len())
	assert.Equal(t, "foo", string(msg.Get(0).Get()))

	msg = readMsg(t, i.TransactionChan())
	assert.Equal(t, 1, msg.Len())
	assert.Equal(t, "bar", string(msg.Get(0).Get()))

	msg = readMsg(t, i.TransactionChan())
	assert.Equal(t, 1, msg.Len())
	assert.Equal(t, "baz", string(msg.Get(0).Get()))

	msg = readMsg(t, i.TransactionChan())
	assert.Equal(t, 1, msg.Len())
	assert.Equal(t, "foo", string(msg.Get(0).Get()))

	msg = readMsg(t, i.TransactionChan())
	assert.Equal(t, 1, msg.Len())
	assert.Equal(t, "bar", string(msg.Get(0).Get()))

	msg = readMsg(t, i.TransactionChan())
	assert.Equal(t, 1, msg.Len())
	assert.Equal(t, "baz", string(msg.Get(0).Get()))

	i.CloseAsync()
	require.NoError(t, i.WaitForClose(time.Second))
}

func TestSubprocessCloseInBetween(t *testing.T) {
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

	conf := NewConfig()
	conf.Type = TypeSubprocess
	conf.Subprocess.Name = "go"
	conf.Subprocess.Args = []string{"run", filePath}

	i, err := New(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	msg := readMsg(t, i.TransactionChan())
	assert.Equal(t, 1, msg.Len())
	assert.Equal(t, "foo:0", string(msg.Get(0).Get()))

	msg = readMsg(t, i.TransactionChan())
	assert.Equal(t, 1, msg.Len())
	assert.Equal(t, "foo:1", string(msg.Get(0).Get()))

	i.CloseAsync()
	require.NoError(t, i.WaitForClose(time.Second))
}
