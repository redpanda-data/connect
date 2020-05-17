package processor

import (
	"bytes"
	"encoding/ascii85"
	"encoding/base64"
	"encoding/hex"
	"os"
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/tilinna/z85"
)

func TestDecodeBadAlgo(t *testing.T) {
	conf := NewConfig()
	conf.Decode.Scheme = "does not exist"

	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})

	_, err := NewDecode(conf, nil, testLog, metrics.DudType{})
	if err == nil {
		t.Error("Expected error from bad algo")
	}
}

func TestDecodeBase64(t *testing.T) {
	conf := NewConfig()
	conf.Decode.Scheme = "base64"

	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})

	exp := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	input := [][]byte{}

	for i := range exp {
		var buf bytes.Buffer

		zw := base64.NewEncoder(base64.StdEncoding, &buf)
		zw.Write(exp[i])
		zw.Close()

		input = append(input, buf.Bytes())
	}

	if reflect.DeepEqual(input, exp) {
		t.Fatal("Input and exp output are the same")
	}

	proc, err := NewDecode(conf, nil, testLog, metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessMessage(message.New(input))
	if len(msgs) != 1 {
		t.Error("Decode failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}

func TestDecodeHex(t *testing.T) {
	conf := NewConfig()
	conf.Decode.Scheme = "hex"

	exp := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	input := [][]byte{}

	for i := range exp {
		var buf bytes.Buffer

		zw := hex.NewEncoder(&buf)
		zw.Write(exp[i])

		input = append(input, buf.Bytes())
	}

	if reflect.DeepEqual(input, exp) {
		t.Fatal("Input and exp output are the same")
	}

	proc, err := NewDecode(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessMessage(message.New(input))
	if len(msgs) != 1 {
		t.Error("Decode failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}

func TestDecodeAscii85(t *testing.T) {
	conf := NewConfig()
	conf.Decode.Scheme = "ascii85"

	exp := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	input := [][]byte{}

	for i := range exp {
		var buf bytes.Buffer

		zw := ascii85.NewEncoder(&buf)
		zw.Write(exp[i])
		zw.Close()

		input = append(input, buf.Bytes())
	}

	if reflect.DeepEqual(input, exp) {
		t.Fatal("Input and exp output are the same")
	}

	proc, err := NewDecode(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessMessage(message.New(input))
	if len(msgs) != 1 {
		t.Error("Decode failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}

func TestDecodeZ85(t *testing.T) {
	conf := NewConfig()
	conf.Decode.Scheme = "z85"

	exp := [][]byte{
		[]byte("hello world first part!!"),
		[]byte("hello world second p"),
		[]byte("third part abcde"),
		[]byte("fourth part!"),
		[]byte("five"),
	}

	input := [][]byte{}

	for i := range exp {
		enc := make([]byte, z85.EncodedLen(len(exp[i])))
		_, err := z85.Encode(enc, exp[i])
		if err != nil {
			t.Fatalf("Failed to prep example %d: %s", i, err)
		}
		input = append(input, enc)
	}

	if reflect.DeepEqual(input, exp) {
		t.Fatal("Input and exp output are the same")
	}

	proc, err := NewDecode(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessMessage(message.New(input))
	if len(msgs) != 1 {
		t.Error("Decode failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}

func TestDecodeIndexBounds(t *testing.T) {
	conf := NewConfig()

	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})

	input := [][]byte{}

	decoded := [][]byte{
		[]byte("0"),
		[]byte("1"),
		[]byte("2"),
		[]byte("3"),
		[]byte("4"),
	}

	for i := range decoded {
		var buf bytes.Buffer

		zw := base64.NewEncoder(base64.StdEncoding, &buf)
		zw.Write(decoded[i])
		zw.Close()

		input = append(input, buf.Bytes())
	}

	tests := map[int]int{
		-5: 0,
		-4: 1,
		-3: 2,
		-2: 3,
		-1: 4,
		0:  0,
		1:  1,
		2:  2,
		3:  3,
		4:  4,
	}

	for i, expIndex := range tests {
		conf.Decode.Parts = []int{i}
		proc, err := NewDecode(conf, nil, testLog, metrics.DudType{})
		if err != nil {
			t.Fatal(err)
		}

		msgs, res := proc.ProcessMessage(message.New(input))
		if len(msgs) != 1 {
			t.Errorf("Decode failed on index: %v", i)
		} else if res != nil {
			t.Errorf("Expected nil response: %v", res)
		}
		if exp, act := string(decoded[expIndex]), string(message.GetAllBytes(msgs[0])[expIndex]); exp != act {
			t.Errorf("Unexpected output for index %v: %v != %v", i, act, exp)
		}
		if exp, act := string(decoded[expIndex]), string(message.GetAllBytes(msgs[0])[(expIndex+1)%5]); exp == act {
			t.Errorf("Processor was applied to wrong index %v: %v != %v", (expIndex+1)%5, act, exp)
		}
	}
}

func TestDecodeEmpty(t *testing.T) {
	conf := NewConfig()
	conf.Decode.Parts = []int{0, 1}

	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	proc, err := NewDecode(conf, nil, testLog, metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	msgs, _ := proc.ProcessMessage(message.New([][]byte{}))
	if len(msgs) > 0 {
		t.Error("Expected failure with zero part message")
	}
}
