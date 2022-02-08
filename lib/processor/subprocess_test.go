package processor

import (
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
	msgIn := message.QuickBatch([][]byte{
		[]byte(`hello foo world`),
		[]byte(`hello baz world`),
		[]byte(`foo`),
	})
	msgs, res := proc.ProcessMessage(msgIn)
	if len(msgs) != 1 {
		t.Fatal("Wrong count of messages")
	}
	if res != nil {
		t.Fatalf("Non-nil result: %v", res.AckError())
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
	msgIn := message.QuickBatch([][]byte{
		[]byte(`hello bar world`),
		[]byte(`hello baz world`),
		[]byte(`bar`),
	})
	msgs, res := proc.ProcessMessage(msgIn)
	if len(msgs) != 1 {
		t.Fatal("Wrong count of messages")
	}
	if res != nil {
		t.Fatalf("Non-nil result: %v", res.AckError())
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
	msgIn := message.QuickBatch([][]byte{
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
		t.Fatalf("Non-nil result: %v", res.AckError())
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

	msgIn := message.QuickBatch([][]byte{
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

	dir := t.TempDir()

	pathStr := path.Join(dir, "main.go")
	require.NoError(t, os.WriteFile(pathStr, []byte(program), 0o666))

	return pathStr
}

func TestSubprocessHappy(t *testing.T) {
	filePath := testProgram(t, `package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
)

func lengthPrefixedUInt32BESplitFunc(data []byte, atEOF bool) (advance int, token []byte, err error) {
	const prefixBytes int = 4
	if atEOF {
		return 0, nil, nil
	}
	if len(data) < prefixBytes {
		// request more data
		return 0, nil, nil
	}
	l := binary.BigEndian.Uint32(data)
	bytesToRead := int(l)

	if len(data)-prefixBytes >= bytesToRead {
		return prefixBytes + bytesToRead, data[prefixBytes : prefixBytes+bytesToRead], nil
	} else {
		// request more data
		return 0, nil, nil
	}
}

var stdinCodec *string = flag.String("stdinCodec", "lines", "format to use for input")
var stdoutCodec *string = flag.String("stdoutCodec", "lines", "format for use for output")

func main() {
	flag.Parse()

	scanner := bufio.NewScanner(os.Stdin)
	if *stdinCodec == "length_prefixed_uint32_be" {
		scanner.Split(lengthPrefixedUInt32BESplitFunc)
	}

	lenBuf := make([]byte, 4)
	for scanner.Scan() {
		res := strings.ToUpper(scanner.Text())
		switch *stdoutCodec {
			case "length_prefixed_uint32_be":
				buf := []byte(res)
				binary.BigEndian.PutUint32(lenBuf, uint32(len(buf)))
				_, _ = os.Stdout.Write(lenBuf)
				_, _ = os.Stdout.Write(buf)
				break
			case "netstring":
				fmt.Printf("%d:%s,",len(res),res)
				break
			default:
				fmt.Println(res)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Println(err)
	}
}
`)
	f := func(formatSend string, formatRecv string, extra bool) {
		conf := NewConfig()
		conf.Type = TypeSubprocess
		conf.Subprocess.Name = "go"
		conf.Subprocess.Args = []string{"run", filePath, "-stdinCodec", formatSend, "-stdoutCodec", formatRecv}
		conf.Subprocess.CodecSend = formatSend
		conf.Subprocess.CodecRecv = formatRecv

		proc, err := New(conf, nil, log.Noop(), metrics.Noop())
		require.NoError(t, err)

		exp := [][]byte{
			[]byte(`FOO`),
			[]byte(`FOÖ`),
			[]byte(`BAR`),
			[]byte(`BAZ`),
		}
		if extra {
			exp = append(exp, []byte(``), []byte("|{O\n\r\nO}|"))
		}

		msgIn := message.QuickBatch([][]byte{
			[]byte(`foo`),
			[]byte(`foö`),
			[]byte(`bar`),
			[]byte(`baz`),
		})
		if extra {
			msgIn.Append(message.NewPart([]byte(``)))
			msgIn.Append(message.NewPart([]byte("|{o\n\r\no}|")))
		}

		msgs, res := proc.ProcessMessage(msgIn)
		require.Len(t, msgs, 1)
		require.Nil(t, res)

		for i := 0; i < msgIn.Len(); i++ {
			assert.Empty(t, msgs[0].Get(i).MetaGet(FailFlagKey))
		}
		assert.Equal(t, exp, message.GetAllBytes(msgs[0]))

		proc.CloseAsync()
		assert.NoError(t, proc.WaitForClose(time.Second))
	}
	f("lines", "lines", false)
	f("length_prefixed_uint32_be", "lines", false)
	f("lines", "length_prefixed_uint32_be", false)
	f("length_prefixed_uint32_be", "netstring", true)
	f("length_prefixed_uint32_be", "length_prefixed_uint32_be", true)
}
