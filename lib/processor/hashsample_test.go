// Copyright (c) 2018 Lorenzo Alberton
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
	"os"
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

func TestHashSample(t *testing.T) {
	doc1 := []byte(`some text`)       // hashed to 44.82100
	doc2 := []byte(`some other text`) // hashed to 94.99035
	doc3 := []byte(`abc`)             // hashed to 26.84963

	tt := []struct {
		name     string
		input    []byte
		min float64
		max float64
		expected []byte
	}{
		{"100% sample", doc1, 0.0, 101.0, doc1},
		{"0% sample", doc1, 0.0, 0.0, nil},

		{"lower 50% sample", doc1, 0.0, 50.0, doc1},
		{"upper 50% sample", doc1, 50.0, 101.0, nil},

		{"lower 33% sample", doc1, 0.0, 33.0, nil},
		{"mid 33% sample", doc1, 33.0, 66.0, doc1},
		{"upper 33% sample", doc1, 66.0, 101.0, nil},

		// -----

		{"100% sample", doc2, 0.0, 101.0, doc2},
		{"0% sample", doc2, 0.0, 0.0, nil},

		{"lower 50% sample", doc2, 0.0, 50.0, nil},
		{"upper 50% sample", doc2, 50.0, 101.0, doc2},

		{"lower 33% sample", doc2, 0.0, 33.0, nil},
		{"mid 33% sample", doc2, 33.0, 66.0, nil},
		{"upper 33% sample", doc2, 66.0, 101.0, doc2},

		// -----

		{"100% sample", doc3, 0.0, 101.0, doc3},
		{"0% sample", doc3, 0.0, 0.0, nil},

		{"lower 50% sample", doc3, 0.0, 50.0, doc3},
		{"upper 50% sample", doc3, 50.0, 101.0, nil},

		{"lower 33% sample", doc3, 0.0, 33.0, doc3},
		{"mid 33% sample", doc3, 33.0, 66.0, nil},
		{"upper 33% sample", doc3, 66.0, 101.0, nil},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			conf := NewConfig()
			conf.HashSample.RetainMin = tc.min
			conf.HashSample.RetainMax = tc.max
			conf.HashSample.Parts     = []int{0}

			testLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})
			proc, err := NewHashSample(conf, testLog, metrics.DudType{})
			if err != nil {
				t.Error(err)
				return
			}

			msgIn := types.Message{Parts: [][]byte{tc.input}}
			msgOut, _, propagate := proc.ProcessMessage(&msgIn)
			if propagate {
				if &msgIn != msgOut {
					t.Error("Message told to propagate but not given")
				}
			}

			if nil != tc.expected && !propagate {
				t.Error("Message told not to propagate even if it was expected to propagate")
			}
			if nil == tc.expected && propagate {
				t.Error("Message told to propagate even if it was not expected to propagate")
			}
			if nil != tc.expected && propagate {
				if !reflect.DeepEqual(msgOut.Parts[0], tc.expected) {
					t.Errorf("Unexpected sampling: EXPECTED: %v, ACTUAL: %v", tc.expected, msgOut.Parts[0])
				}
			}
		})
	}
}

