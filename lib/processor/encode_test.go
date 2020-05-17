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
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/tilinna/z85"
)

func TestEncodeBadAlgo(t *testing.T) {
	conf := NewConfig()
	conf.Encode.Scheme = "does not exist"

	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})

	_, err := NewEncode(conf, nil, testLog, metrics.DudType{})
	if err == nil {
		t.Error("Expected error from bad algo")
	}
}

func TestEncodeBase64(t *testing.T) {
	conf := NewConfig()
	conf.Encode.Scheme = "base64"

	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})

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

		zw := base64.NewEncoder(base64.StdEncoding, &buf)
		zw.Write(input[i])
		zw.Close()

		exp = append(exp, buf.Bytes())
	}

	if reflect.DeepEqual(input, exp) {
		t.Fatal("Input and exp output are the same")
	}

	proc, err := NewEncode(conf, nil, testLog, metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessMessage(message.New(input))
	if len(msgs) != 1 {
		t.Error("Encode failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}

func TestEncodeHex(t *testing.T) {
	conf := NewConfig()
	conf.Encode.Scheme = "hex"

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

		zw := hex.NewEncoder(&buf)
		zw.Write(input[i])

		exp = append(exp, buf.Bytes())
	}

	if reflect.DeepEqual(input, exp) {
		t.Fatal("Input and exp output are the same")
	}

	proc, err := NewEncode(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessMessage(message.New(input))
	if len(msgs) != 1 {
		t.Error("Encode failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}

func TestEncodeAscii85(t *testing.T) {
	conf := NewConfig()
	conf.Encode.Scheme = "ascii85"

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

		zw := ascii85.NewEncoder(&buf)
		zw.Write(input[i])

		exp = append(exp, buf.Bytes())
	}

	if reflect.DeepEqual(input, exp) {
		t.Fatal("Input and exp output are the same")
	}

	proc, err := NewEncode(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessMessage(message.New(input))
	if len(msgs) != 1 {
		t.Error("Encode failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}

func TestEncodeZ85(t *testing.T) {
	conf := NewConfig()
	conf.Encode.Scheme = "z85"

	input := [][]byte{
		[]byte("hello world first part!!"),
		[]byte("hello world second p"),
		[]byte("third part abcde"),
		[]byte("fourth part!"),
		[]byte("five"),
	}

	exp := [][]byte{}

	for i := range input {
		enc := make([]byte, z85.EncodedLen(len(input[i])))
		_, err := z85.Encode(enc, input[i])
		if err != nil {
			t.Fatal("Failed to encode z85 input")
		}
		exp = append(exp, enc)
	}

	if reflect.DeepEqual(input, exp) {
		t.Fatal("Input and exp output are the same")
	}

	//proc, err := NewEncode(conf, nil, log.Noop(), metrics.Noop())
	testLog := log.New(os.Stdout, log.Config{LogLevel: "DEBUG"})
	proc, err := NewEncode(conf, nil, testLog, metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessMessage(message.New(input))
	if len(msgs) != 1 {
		t.Error("Encode failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}

	// make sure an attempt to encode a byte array that is
	// not divisible by four fails
	input = [][]byte{
		[]byte("12345"),
		[]byte("1234"),
		[]byte("123"),
		[]byte("12"),
		[]byte("1"),
	}
	msgs, res = proc.ProcessMessage(message.New(input))
	if len(msgs) != 1 {
		t.Errorf("Expected to process a message")
	}
	msgs[0].Iter(func(i int, p types.Part) error {
		if len(input[i])%4 == 0 && HasFailed(p) {
			t.Errorf("Unexpected fail flag on part %d", i)
		} else if len(input[i])%4 != 0 && !HasFailed(p) {
			t.Errorf("Expected fail flag on part %d", i)
		}
		return nil
	})
}

func TestEncodeIndexBounds(t *testing.T) {
	conf := NewConfig()

	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})

	input := [][]byte{
		[]byte("0"),
		[]byte("1"),
		[]byte("2"),
		[]byte("3"),
		[]byte("4"),
	}

	encoded := [][]byte{}

	for i := range input {
		var buf bytes.Buffer

		zw := base64.NewEncoder(base64.StdEncoding, &buf)
		zw.Write(input[i])
		zw.Close()

		encoded = append(encoded, buf.Bytes())
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
		conf.Encode.Parts = []int{i}
		proc, err := NewEncode(conf, nil, testLog, metrics.DudType{})
		if err != nil {
			t.Fatal(err)
		}

		msgs, res := proc.ProcessMessage(message.New(input))
		if len(msgs) != 1 {
			t.Errorf("Encode failed on index: %v", i)
		} else if res != nil {
			t.Errorf("Expected nil response: %v", res)
		}
		if exp, act := string(encoded[expIndex]), string(message.GetAllBytes(msgs[0])[expIndex]); exp != act {
			t.Errorf("Unexpected output for index %v: %v != %v", i, act, exp)
		}
		if exp, act := string(encoded[expIndex]), string(message.GetAllBytes(msgs[0])[(expIndex+1)%5]); exp == act {
			t.Errorf("Processor was applied to wrong index %v: %v != %v", (expIndex+1)%5, act, exp)
		}
	}
}

func TestEncodeEmpty(t *testing.T) {
	conf := NewConfig()
	conf.Encode.Parts = []int{0, 1}

	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	proc, err := NewEncode(conf, nil, testLog, metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	msgs, _ := proc.ProcessMessage(message.New([][]byte{}))
	if len(msgs) > 0 {
		t.Error("Expected failure with zero part message")
	}
}
