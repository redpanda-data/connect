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
	"github.com/pierrec/lz4/v4"

	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestCompressBadAlgo(t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "compress"
	conf.Compress.Algorithm = "does not exist"

	_, err := mock.NewManager().NewProcessor(conf)
	if err == nil {
		t.Error("Expected error from bad algo")
	}
}

func TestCompressGZIP(t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "compress"
	conf.Compress.Algorithm = "gzip"

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

func TestCompressZLIB(t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "compress"
	conf.Compress.Algorithm = "zlib"

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
	conf := processor.NewConfig()
	conf.Type = "compress"
	conf.Compress.Algorithm = "flate"

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

		zw, err := flate.NewWriter(&buf, conf.Compress.Level)
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
	conf := processor.NewConfig()
	conf.Type = "compress"
	conf.Compress.Algorithm = "snappy"

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
	conf := processor.NewConfig()
	conf.Type = "compress"
	conf.Compress.Algorithm = "lz4"

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
