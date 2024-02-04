package pure_test

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"github.com/klauspost/compress/flate"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zlib"
	"github.com/klauspost/pgzip"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/testutil"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestCompressBadAlgo(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
compress:
  algorithm: does not exist
`)
	require.NoError(t, err)

	_, err = mock.NewManager().NewProcessor(conf)
	if err == nil {
		t.Error("Expected error from bad algo")
	}
}

func TestCompressGZIP(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
compress:
  algorithm: gzip
`)
	require.NoError(t, err)

	input := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	exp := [][]byte{}

	for i := range input {
		var buf bytes.Buffer

		zw := gzip.NewWriter(&buf)
		_, _ = zw.Write(input[i])
		zw.Close()

		exp = append(exp, buf.Bytes())
	}

	if reflect.DeepEqual(input, exp) {
		t.Fatal("Input and exp output are the same")
	}

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessBatch(context.Background(), message.QuickBatch(input))
	if len(msgs) != 1 {
		t.Error("Compress failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}

func TestCompressPGZIP(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
compress:
  algorithm: pgzip
`)
	require.NoError(t, err)

	input := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	exp := [][]byte{}

	for i := range input {
		var buf bytes.Buffer

		zw := pgzip.NewWriter(&buf)
		_, _ = zw.Write(input[i])
		zw.Close()

		exp = append(exp, buf.Bytes())
	}

	if reflect.DeepEqual(input, exp) {
		t.Fatal("Input and exp output are the same")
	}

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessBatch(context.Background(), message.QuickBatch(input))
	if len(msgs) != 1 {
		t.Error("Compress failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}

func TestCompressZLIB(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
compress:
  algorithm: zlib
`)
	require.NoError(t, err)

	input := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	exp := [][]byte{}

	for i := range input {
		var buf bytes.Buffer

		zw := zlib.NewWriter(&buf)
		_, _ = zw.Write(input[i])
		zw.Close()

		exp = append(exp, buf.Bytes())
	}

	if reflect.DeepEqual(input, exp) {
		t.Fatal("Input and exp output are the same")
	}

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessBatch(context.Background(), message.QuickBatch(input))
	if len(msgs) != 1 {
		t.Error("Compress failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}

func TestCompressFlate(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
compress:
  algorithm: flate
`)
	require.NoError(t, err)

	input := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	exp := [][]byte{}

	for i := range input {
		var buf bytes.Buffer

		zw, err := flate.NewWriter(&buf, -1)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = zw.Write(input[i])
		zw.Close()

		exp = append(exp, buf.Bytes())
	}

	if reflect.DeepEqual(input, exp) {
		t.Fatal("Input and exp output are the same")
	}

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessBatch(context.Background(), message.QuickBatch(input))
	if len(msgs) != 1 {
		t.Error("Compress failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}

func TestCompressSnappy(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
compress:
  algorithm: snappy
`)
	require.NoError(t, err)

	input := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	exp := [][]byte{}

	for i := range input {
		output := snappy.Encode(nil, input[i])
		exp = append(exp, output)
	}

	if reflect.DeepEqual(input, exp) {
		t.Fatal("Input and exp output are the same")
	}

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessBatch(context.Background(), message.QuickBatch(input))
	if len(msgs) != 1 {
		t.Error("Compress failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}

func TestCompressLZ4(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
compress:
  algorithm: lz4
`)
	require.NoError(t, err)

	input := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	exp := [][]byte{}

	for i := range input {
		var buf bytes.Buffer

		w := lz4.NewWriter(&buf)
		if _, err := w.Write(input[i]); err != nil {
			w.Close()
			t.Fatalf("Failed to compress input: %s", err)
		}
		w.Close()

		exp = append(exp, buf.Bytes())
	}

	if reflect.DeepEqual(input, exp) {
		t.Fatal("Input and exp output are the same")
	}

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessBatch(context.Background(), message.QuickBatch(input))
	if len(msgs) != 1 {
		t.Error("Compress failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}
