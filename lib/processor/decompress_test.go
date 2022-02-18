package processor

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
)

func TestDecompressBadAlgo(t *testing.T) {
	conf := NewConfig()
	conf.Type = "decompress"
	conf.Decompress.Algorithm = "does not exist"

	testLog := log.Noop()

	_, err := New(conf, mock.NewManager(), testLog, metrics.Noop())
	if err == nil {
		t.Error("Expected error from bad algo")
	}
}

func TestDecompressGZIP(t *testing.T) {
	conf := NewConfig()
	conf.Type = "decompress"
	conf.Decompress.Algorithm = "gzip"

	testLog := log.Noop()

	input := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	exp := [][]byte{}

	for i := range input {
		exp = append(exp, input[i])

		var buf bytes.Buffer

		zw := gzip.NewWriter(&buf)
		zw.Write(input[i])
		zw.Close()

		input[i] = buf.Bytes()
	}

	if reflect.DeepEqual(input, exp) {
		t.Fatal("Input and exp output are the same")
	}

	proc, err := New(conf, mock.NewManager(), testLog, metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessMessage(message.QuickBatch(input))
	if len(msgs) != 1 {
		t.Error("Decompress failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}

func TestDecompressSnappy(t *testing.T) {
	conf := NewConfig()
	conf.Type = "decompress"
	conf.Decompress.Algorithm = "snappy"

	input := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	exp := [][]byte{}

	for i := range input {
		exp = append(exp, input[i])
		input[i] = snappy.Encode(nil, input[i])
	}

	if reflect.DeepEqual(input, exp) {
		t.Fatal("Input and exp output are the same")
	}

	proc, err := New(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessMessage(message.QuickBatch(input))
	if len(msgs) != 1 {
		t.Error("Decompress failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}

func TestDecompressZLIB(t *testing.T) {
	conf := NewConfig()
	conf.Type = "decompress"
	conf.Decompress.Algorithm = "zlib"

	testLog := log.Noop()

	input := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	exp := [][]byte{}

	for i := range input {
		exp = append(exp, input[i])

		var buf bytes.Buffer

		zw := zlib.NewWriter(&buf)
		_, _ = zw.Write(input[i])
		zw.Close()

		input[i] = buf.Bytes()
	}

	if reflect.DeepEqual(input, exp) {
		t.Fatal("Input and exp output are the same")
	}

	proc, err := New(conf, mock.NewManager(), testLog, metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessMessage(message.QuickBatch(input))
	if len(msgs) != 1 {
		t.Error("Decompress failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}

func TestDecompressFlate(t *testing.T) {
	conf := NewConfig()
	conf.Type = "decompress"
	conf.Decompress.Algorithm = "flate"

	testLog := log.Noop()

	input := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	exp := [][]byte{}

	for i := range input {
		exp = append(exp, input[i])

		var buf bytes.Buffer

		zw, err := flate.NewWriter(&buf, 0)
		if err != nil {
			t.Fatal(err)
		}
		zw.Write(input[i])
		zw.Close()

		input[i] = buf.Bytes()
	}

	if reflect.DeepEqual(input, exp) {
		t.Fatal("Input and exp output are the same")
	}

	proc, err := New(conf, mock.NewManager(), testLog, metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessMessage(message.QuickBatch(input))
	if len(msgs) != 1 {
		t.Error("Decompress failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}

func TestDecompressLZ4(t *testing.T) {
	conf := NewConfig()
	conf.Type = "decompress"
	conf.Decompress.Algorithm = "lz4"

	input := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	exp := [][]byte{}

	for i := range input {
		exp = append(exp, input[i])

		buf := bytes.Buffer{}
		w := lz4.NewWriter(&buf)
		if _, err := w.Write(input[i]); err != nil {
			w.Close()
			t.Fatalf("Failed to compress input: %s", err)
		}
		w.Close()

		input[i] = buf.Bytes()
	}

	if reflect.DeepEqual(input, exp) {
		t.Fatal("Input and exp output are the same")
	}

	proc, err := New(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessMessage(message.QuickBatch(input))
	if len(msgs) != 1 {
		t.Error("Decompress failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}
