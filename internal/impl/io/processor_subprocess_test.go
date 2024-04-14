package io_test

import (
	"context"
	"fmt"
	"os"
	"path"
	"reflect"
	"testing"
	"time"

	"github.com/Jeffail/gabs/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/testutil"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestSubprocessWithSed(t *testing.T) {
	t.Skip("disabled for now")

	conf, err := testutil.ProcessorFromYAML(`
subprocess:
  name: sed
  args: [ "s/foo/bar/g", "-u" ]
`)
	require.NoError(t, err)

	proc, err := mock.NewManager().NewProcessor(conf)
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
	msgs, res := proc.ProcessBatch(context.Background(), msgIn)
	if len(msgs) != 1 {
		t.Fatal("Wrong count of messages")
	}
	if res != nil {
		t.Fatalf("Non-nil result: %v", res)
	}

	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong results: %s != %s", act, exp)
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()
	require.NoError(t, proc.Close(ctx))
}

func TestSubprocessWithCat(t *testing.T) {
	t.Skip("disabled for now")

	conf, err := testutil.ProcessorFromYAML(`
subprocess:
  name: cat
`)
	require.NoError(t, err)

	proc, err := mock.NewManager().NewProcessor(conf)
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
	msgs, res := proc.ProcessBatch(context.Background(), msgIn)
	if len(msgs) != 1 {
		t.Fatal("Wrong count of messages")
	}
	if res != nil {
		t.Fatalf("Non-nil result: %v", res)
	}

	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong results: %s != %s", act, exp)
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()
	require.NoError(t, proc.Close(ctx))
}

func TestSubprocessLineBreaks(t *testing.T) {
	t.Skip("disabled for now")

	conf, err := testutil.ProcessorFromYAML(`
subprocess:
  name: sed
  args: [ "s/\\(^$\\)\\|\\(foo\\)/bar/", "-u" ]
`)
	require.NoError(t, err)

	proc, err := mock.NewManager().NewProcessor(conf)
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
	msgs, res := proc.ProcessBatch(context.Background(), msgIn)
	if len(msgs) != 1 {
		t.Fatalf("Wrong count of messages %d", len(msgs))
	}
	if res != nil {
		t.Fatalf("Non-nil result: %v", res)
	}

	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong results: %s != %s", act, exp)
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()
	require.NoError(t, proc.Close(ctx))
}

func TestSubprocessWithErrors(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
subprocess:
  name: sh
  args: [ "-c", "cat 1>&2" ]
`)
	require.NoError(t, err)

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Skipf("Not sure if this is due to missing executable: %v", err)
	}

	msgIn := message.QuickBatch([][]byte{
		[]byte(`hello bar world`),
	})

	msgs, _ := proc.ProcessBatch(context.Background(), msgIn)

	assert.Error(t, msgs[0].Get(0).ErrorGet())

	ctx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()
	require.NoError(t, proc.Close(ctx))
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
	f := func(formatSend, formatRecv string, extra bool) {
		conf, err := testutil.ProcessorFromYAML(fmt.Sprintf(`
subprocess:
  name: go
  args: %v
  codec_send: %v
  codec_recv: %v
`, gabs.Wrap([]string{"run", filePath, "-stdinCodec", formatSend, "-stdoutCodec", formatRecv}).String(), formatSend, formatRecv))
		require.NoError(t, err)

		proc, err := mock.NewManager().NewProcessor(conf)
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
			msgIn = append(msgIn, message.NewPart([]byte(``)), message.NewPart([]byte("|{o\n\r\no}|")))
		}

		msgs, res := proc.ProcessBatch(context.Background(), msgIn)
		require.Len(t, msgs, 1)
		require.NoError(t, res)

		for i := 0; i < msgIn.Len(); i++ {
			assert.NoError(t, msgs[0].Get(i).ErrorGet())
		}
		assert.Equal(t, exp, message.GetAllBytes(msgs[0]))

		ctx, done := context.WithTimeout(context.Background(), time.Second*5)
		defer done()
		require.NoError(t, proc.Close(ctx))
	}
	f("lines", "lines", false)
	f("length_prefixed_uint32_be", "lines", false)
	f("lines", "length_prefixed_uint32_be", false)
	f("length_prefixed_uint32_be", "netstring", true)
	f("length_prefixed_uint32_be", "length_prefixed_uint32_be", true)
}
