// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package processor

import (
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"os"
	"reflect"
	"strconv"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/OneOfOne/xxhash"
)

func TestHashBadAlgo(t *testing.T) {
	conf := NewConfig()
	conf.Hash.Algorithm = "does not exist"

	_, err := NewHash(conf, nil, log.Noop(), metrics.Noop())
	if err == nil {
		t.Error("Expected error from bad algo")
	}
}

func TestHashSha1(t *testing.T) {
	conf := NewConfig()
	conf.Hash.Algorithm = "sha1"

	input := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	exp := [][]byte{}

	for i := range input {
		h := sha1.New()
		h.Write(input[i])
		exp = append(exp, h.Sum(nil))
	}

	if reflect.DeepEqual(input, exp) {
		t.Fatal("Input and exp output are the same")
	}

	proc, err := NewHash(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessMessage(message.New(input))
	if len(msgs) != 1 {
		t.Error("Hash failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}

func TestHashSha256(t *testing.T) {
	conf := NewConfig()
	conf.Hash.Algorithm = "sha256"

	input := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	exp := [][]byte{}

	for i := range input {
		h := sha256.New()
		h.Write(input[i])
		exp = append(exp, h.Sum(nil))
	}

	if reflect.DeepEqual(input, exp) {
		t.Fatal("Input and exp output are the same")
	}

	proc, err := NewHash(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessMessage(message.New(input))
	if len(msgs) != 1 {
		t.Error("Hash failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}

func TestHashSha512(t *testing.T) {
	conf := NewConfig()
	conf.Hash.Algorithm = "sha512"

	input := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	exp := [][]byte{}

	for i := range input {
		h := sha512.New()
		h.Write(input[i])
		exp = append(exp, h.Sum(nil))
	}

	if reflect.DeepEqual(input, exp) {
		t.Fatal("Input and exp output are the same")
	}

	proc, err := NewHash(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessMessage(message.New(input))
	if len(msgs) != 1 {
		t.Error("Hash failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}

func TestHashXXHash64(t *testing.T) {
	conf := NewConfig()
	conf.Hash.Algorithm = "xxhash64"

	input := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	exp := [][]byte{}

	for i := range input {
		h := xxhash.New64()
		h.Write(input[i])
		exp = append(exp, []byte(strconv.FormatUint(h.Sum64(), 10)))
	}

	if reflect.DeepEqual(input, exp) {
		t.Fatal("Input and exp output are the same")
	}

	proc, err := NewHash(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessMessage(message.New(input))
	if len(msgs) != 1 {
		t.Error("Hash failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}

func TestHashIndexBounds(t *testing.T) {
	conf := NewConfig()

	input := [][]byte{
		[]byte("0"),
		[]byte("1"),
		[]byte("2"),
		[]byte("3"),
		[]byte("4"),
	}

	hashed := [][]byte{}

	for i := range input {
		h := sha256.New()
		h.Write(input[i])
		hashed = append(hashed, h.Sum(nil))
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
		conf.Hash.Parts = []int{i}
		proc, err := NewHash(conf, nil, log.Noop(), metrics.Noop())
		if err != nil {
			t.Fatal(err)
		}

		msgs, res := proc.ProcessMessage(message.New(input))
		if len(msgs) != 1 {
			t.Errorf("Hash failed on index: %v", i)
		} else if res != nil {
			t.Errorf("Expected nil response: %v", res)
		}
		if exp, act := string(hashed[expIndex]), string(message.GetAllBytes(msgs[0])[expIndex]); exp != act {
			t.Errorf("Unexpected output for index %v: %v != %v", i, act, exp)
		}
		if exp, act := string(hashed[expIndex]), string(message.GetAllBytes(msgs[0])[(expIndex+1)%5]); exp == act {
			t.Errorf("Processor was applied to wrong index %v: %v != %v", (expIndex+1)%5, act, exp)
		}
	}
}

func TestHashEmpty(t *testing.T) {
	conf := NewConfig()
	conf.Hash.Parts = []int{0, 1}

	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	proc, err := NewHash(conf, nil, testLog, metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	msgs, _ := proc.ProcessMessage(message.New([][]byte{}))
	if len(msgs) > 0 {
		t.Error("Expected failure with zero part message")
	}
}

func BenchmarkHashSha256(b *testing.B) {
	conf := NewConfig()
	conf.Hash.Algorithm = "sha256"

	input := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	exp := [][]byte{}

	for i := range input {
		h := sha256.New()
		h.Write(input[i])
		exp = append(exp, h.Sum(nil))
	}

	if reflect.DeepEqual(input, exp) {
		b.Fatal("Input and exp output are the same")
	}

	proc, err := NewHash(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		msgs, res := proc.ProcessMessage(message.New(input))
		if len(msgs) != 1 {
			b.Error("Hash failed")
		} else if res != nil {
			b.Errorf("Expected nil response: %v", res)
		}
		if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
			b.Errorf("Unexpected output: %s != %s", act, exp)
		}
	}
}
