package processor

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
)

func TestDecompressBadAlgo(t *testing.T) {
	conf := NewConfig()
	conf.Decompress.Algorithm = "does not exist"

	testLog := log.Noop()

	_, err := NewDecompress(conf, nil, testLog, metrics.Noop())
	if err == nil {
		t.Error("Expected error from bad algo")
	}
}

func TestDecompressGZIP(t *testing.T) {
	conf := NewConfig()
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

	proc, err := NewDecompress(conf, nil, testLog, metrics.Noop())
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

	proc, err := NewDecompress(conf, nil, log.Noop(), metrics.Noop())
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

	proc, err := NewDecompress(conf, nil, testLog, metrics.Noop())
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

	proc, err := NewDecompress(conf, nil, testLog, metrics.Noop())
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

	proc, err := NewDecompress(conf, nil, log.Noop(), metrics.Noop())
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

func TestDecompressIndexBounds(t *testing.T) {
	conf := NewConfig()

	testLog := log.Noop()

	input := [][]byte{
		[]byte("0"),
		[]byte("1"),
		[]byte("2"),
		[]byte("3"),
		[]byte("4"),
	}

	for i := range input {
		var buf bytes.Buffer

		zw := gzip.NewWriter(&buf)
		zw.Write(input[i])
		zw.Close()

		input[i] = buf.Bytes()
	}

	type result struct {
		index int
		value string
	}

	tests := map[int]result{
		-5: {
			index: 0,
			value: "0",
		},
		-4: {
			index: 1,
			value: "1",
		},
		-3: {
			index: 2,
			value: "2",
		},
		-2: {
			index: 3,
			value: "3",
		},
		-1: {
			index: 4,
			value: "4",
		},
		0: {
			index: 0,
			value: "0",
		},
		1: {
			index: 1,
			value: "1",
		},
		2: {
			index: 2,
			value: "2",
		},
		3: {
			index: 3,
			value: "3",
		},
		4: {
			index: 4,
			value: "4",
		},
	}

	for i, result := range tests {
		conf.Decompress.Parts = []int{i}
		proc, err := NewDecompress(conf, nil, testLog, metrics.Noop())
		if err != nil {
			t.Fatal(err)
		}

		msgs, res := proc.ProcessMessage(message.QuickBatch(input))
		if len(msgs) != 1 {
			t.Errorf("Decompress failed on index: %v", i)
		} else if res != nil {
			t.Errorf("Expected nil response: %v", res)
		}
		if exp, act := result.value, string(message.GetAllBytes(msgs[0])[result.index]); exp != act {
			t.Errorf("Unexpected output for index %v: %v != %v", i, act, exp)
		}
		if exp, act := result.value, string(message.GetAllBytes(msgs[0])[(result.index+1)%5]); exp == act {
			t.Errorf("Processor was applied to wrong index %v: %v != %v", (result.index+1)%5, act, exp)
		}
	}
}
