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
	"os"
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
)

func TestFilterPartsTextCheck(t *testing.T) {
	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	testMet := metrics.DudType{}

	tests := []struct {
		name string
		arg  [][]byte
		want [][]byte
	}{
		{
			name: "single part 1",
			arg: [][]byte{
				[]byte("foo"),
			},
			want: nil,
		},
		{
			name: "single part 2",
			arg: [][]byte{
				[]byte("bar"),
			},
			want: [][]byte{
				[]byte("bar"),
			},
		},
		{
			name: "multi part 1",
			arg: [][]byte{
				[]byte("foo"),
				[]byte("foo"),
				[]byte("foo"),
				[]byte("foo"),
			},
			want: nil,
		},
		{
			name: "multi part 2",
			arg: [][]byte{
				[]byte("bar"),
				[]byte("foo"),
				[]byte("baz"),
				[]byte("foo"),
			},
			want: [][]byte{
				[]byte("bar"),
				[]byte("baz"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := NewConfig()
			conf.Type = "filter_parts"
			conf.FilterParts.Type = "text"
			conf.FilterParts.Text.Operator = "prefix"
			conf.FilterParts.Text.Part = 0
			conf.FilterParts.Text.Arg = "b"

			c, err := New(conf, nil, testLog, testMet)
			if err != nil {
				t.Error(err)
				return
			}
			got, res := c.ProcessMessage(message.New(tt.arg))
			if tt.want == nil {
				if !reflect.DeepEqual(res, response.NewAck()) {
					t.Error("Filter.ProcessMessage() expected drop")
				}
			} else if !reflect.DeepEqual(message.GetAllBytes(got[0]), tt.want) {
				t.Errorf("Filter.ProcessMessage() = %s, want %s", message.GetAllBytes(got[0]), tt.want)
			}
		})
	}
}
