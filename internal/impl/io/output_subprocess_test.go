package io_test

import (
	"context"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/integration"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"

	_ "github.com/benthosdev/benthos/v4/internal/impl/io"
)

func sendMsg(t *testing.T, msg string, tChan chan message.Transaction) {
	t.Helper()

	m := message.Batch{message.NewPart([]byte(msg))}

	resChan := make(chan error)

	select {
	case tChan <- message.NewTransaction(m, resChan):
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	select {
	case res := <-resChan:
		require.NoError(t, res)
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
}

func TestSubprocessOutputBasic(t *testing.T) {
	integration.CheckSkip(t)

	t.Parallel()

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	dir := t.TempDir()

	filePath := testProgram(t, fmt.Sprintf(`package main

import (
	"fmt"
	"bufio"
	"os"
	"strings"
	"bytes"
)

func main() {
	var buf bytes.Buffer

	target := "%v/output.txt"

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		fmt.Fprintln(&buf, strings.ToUpper(scanner.Text()))
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	if err := os.WriteFile(target, buf.Bytes(), 0o644); err != nil {
		panic(err)
	}
}
`, dir))

	conf := output.NewConfig()
	conf.Type = "subprocess"
	conf.Subprocess.Name = "go"
	conf.Subprocess.Args = []string{"run", filePath}

	o, err := mock.NewManager().NewOutput(conf)
	require.NoError(t, err)

	tranChan := make(chan message.Transaction)
	require.NoError(t, o.Consume(tranChan))

	sendMsg(t, "foo", tranChan)
	sendMsg(t, "bar", tranChan)
	sendMsg(t, "baz", tranChan)

	o.TriggerCloseNow()
	o.TriggerCloseNow() // No panic on double close

	require.NoError(t, o.WaitForClose(ctx))

	assert.Eventually(t, func() bool {
		resBytes, err := os.ReadFile(path.Join(dir, "output.txt"))
		if err != nil {
			return false
		}
		return string(resBytes) == "FOO\nBAR\nBAZ\n"
	}, time.Second, time.Millisecond*100)
}

func TestSubprocessOutputEarlyExit(t *testing.T) {
	t.Skip()

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	dir := t.TempDir()

	filePath := testProgram(t, fmt.Sprintf(`package main

import (
	"fmt"
	"bufio"
	"os"
	"strings"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	if scanner.Scan() {
		f, err := os.OpenFile("%v/"+scanner.Text()+".txt", os.O_RDWR|os.O_CREATE, 0o644)
		if err != nil {
			panic(err)
		}
		fmt.Fprintln(f, strings.ToUpper(scanner.Text()))
		f.Sync()
	} else if err := scanner.Err(); err != nil {
		panic(err)
	}
}
`, dir))

	conf := output.NewConfig()
	conf.Type = "subprocess"
	conf.Subprocess.Name = "go"
	conf.Subprocess.Args = []string{"run", filePath}

	o, err := mock.NewManager().NewOutput(conf)
	require.NoError(t, err)

	tranChan := make(chan message.Transaction)
	require.NoError(t, o.Consume(tranChan))

	sendMsg(t, "foo", tranChan)

	assert.Eventually(t, func() bool {
		sendMsg(t, "bar", tranChan)

		resBytes, err := os.ReadFile(path.Join(dir, "bar.txt"))
		if err != nil {
			return false
		}
		return string(resBytes) == "BAR\n"
	}, time.Second, time.Millisecond*100)

	assert.Eventually(t, func() bool {
		sendMsg(t, "baz", tranChan)

		resBytes, err := os.ReadFile(path.Join(dir, "baz.txt"))
		if err != nil {
			return false
		}
		return string(resBytes) == "BAZ\n"
	}, time.Second, time.Millisecond*100)

	o.TriggerCloseNow()
	require.NoError(t, o.WaitForClose(ctx))

	resBytes, err := os.ReadFile(path.Join(dir, "foo.txt"))
	require.NoError(t, err)
	assert.Equal(t, "FOO\n", string(resBytes))

	resBytes, err = os.ReadFile(path.Join(dir, "bar.txt"))
	require.NoError(t, err)
	assert.Equal(t, "BAR\n", string(resBytes))

	resBytes, err = os.ReadFile(path.Join(dir, "baz.txt"))
	require.NoError(t, err)
	assert.Equal(t, "BAZ\n", string(resBytes))
}
