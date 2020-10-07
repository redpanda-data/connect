package processor

import (
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubprocessWithSed(t *testing.T) {
	t.Skip("disabled for now")

	conf := NewConfig()
	conf.Type = TypeSubprocess
	conf.Subprocess.Name = "sed"
	conf.Subprocess.Args = []string{"s/foo/bar/g", "-u"}

	proc, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Skipf("Not sure if this is due to missing executable: %v", err)
	}

	exp := [][]byte{
		[]byte(`hello bar world`),
		[]byte(`hello baz world`),
		[]byte(`bar`),
	}
	msgIn := message.New([][]byte{
		[]byte(`hello foo world`),
		[]byte(`hello baz world`),
		[]byte(`foo`),
	})
	msgs, res := proc.ProcessMessage(msgIn)
	if len(msgs) != 1 {
		t.Fatal("Wrong count of messages")
	}
	if res != nil {
		t.Fatalf("Non-nil result: %v", res.Error())
	}

	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong results: %s != %s", act, exp)
	}

	proc.CloseAsync()
	if err := proc.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestSubprocessWithCat(t *testing.T) {
	t.Skip("disabled for now")

	conf := NewConfig()
	conf.Type = TypeSubprocess
	conf.Subprocess.Name = "cat"

	proc, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Skipf("Not sure if this is due to missing executable: %v", err)
	}

	exp := [][]byte{
		[]byte(`hello bar world`),
		[]byte(`hello baz world`),
		[]byte(`bar`),
	}
	msgIn := message.New([][]byte{
		[]byte(`hello bar world`),
		[]byte(`hello baz world`),
		[]byte(`bar`),
	})
	msgs, res := proc.ProcessMessage(msgIn)
	if len(msgs) != 1 {
		t.Fatal("Wrong count of messages")
	}
	if res != nil {
		t.Fatalf("Non-nil result: %v", res.Error())
	}

	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong results: %s != %s", act, exp)
	}

	proc.CloseAsync()
	if err := proc.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestSubprocessLineBreaks(t *testing.T) {
	t.Skip("disabled for now")

	conf := NewConfig()
	conf.Type = TypeSubprocess
	conf.Subprocess.Name = "sed"
	conf.Subprocess.Args = []string{`s/\(^$\)\|\(foo\)/bar/`, "-u"}

	proc, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Skipf("Not sure if this is due to missing executable: %v", err)
	}

	exp := [][]byte{
		[]byte("hello bar\nbar world"),
		[]byte("hello bar bar world"),
		[]byte("hello bar\nbar world\n"),
		[]byte("bar"),
		[]byte("hello bar\nbar\nbar world\n"),
	}
	msgIn := message.New([][]byte{
		[]byte("hello foo\nfoo world"),
		[]byte("hello foo bar world"),
		[]byte("hello foo\nfoo world\n"),
		[]byte(""),
		[]byte("hello foo\n\nfoo world\n"),
	})
	msgs, res := proc.ProcessMessage(msgIn)
	if len(msgs) != 1 {
		t.Fatalf("Wrong count of messages %d", len(msgs))
	}
	if res != nil {
		t.Fatalf("Non-nil result: %v", res.Error())
	}

	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong results: %s != %s", act, exp)
	}

	proc.CloseAsync()
	if err := proc.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestSubprocessWithErrors(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeSubprocess
	conf.Subprocess.Name = "sh"
	conf.Subprocess.Args = []string{"-c", "cat 1>&2"}

	proc, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Skipf("Not sure if this is due to missing executable: %v", err)
	}

	msgIn := message.New([][]byte{
		[]byte(`hello bar world`),
	})

	msgs, _ := proc.ProcessMessage(msgIn)

	if !HasFailed(msgs[0].Get(0)) {
		t.Errorf("Expected subprocessor to fail")
	}

	proc.CloseAsync()
	if err := proc.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func testProgram(t *testing.T, program string) string {
	t.Helper()

	dir, err := ioutil.TempDir("", "benthos_subprocessor_test")
	require.NoError(t, err)

	t.Cleanup(func() {
		os.RemoveAll(dir)
	})

	pathStr := path.Join(dir, "main.go")
	require.NoError(t, ioutil.WriteFile(pathStr, []byte(program), 0666))

	return pathStr
}

func TestSubprocessHappy(t *testing.T) {
	filePath := testProgram(t, `package main

import (
	"fmt"
	"bufio"
	"os"
	"log"
	"strings"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		fmt.Println(strings.ToUpper(scanner.Text()))
	}

	if err := scanner.Err(); err != nil {
		log.Println(err)
	}
}
`)

	conf := NewConfig()
	conf.Type = TypeSubprocess
	conf.Subprocess.Name = "go"
	conf.Subprocess.Args = []string{"run", filePath}

	proc, err := New(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	exp := [][]byte{
		[]byte(`FOO`),
		[]byte(`BAR`),
		[]byte(`BAZ`),
	}

	msgIn := message.New([][]byte{
		[]byte(`foo`),
		[]byte(`bar`),
		[]byte(`baz`),
	})

	msgs, res := proc.ProcessMessage(msgIn)
	require.Len(t, msgs, 1)
	require.Nil(t, res)

	assert.Empty(t, msgs[0].Get(0).Metadata().Get(FailFlagKey))
	assert.Equal(t, exp, message.GetAllBytes(msgs[0]))

	proc.CloseAsync()
	assert.NoError(t, proc.WaitForClose(time.Second))
}
