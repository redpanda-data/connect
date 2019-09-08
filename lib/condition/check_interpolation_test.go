// Copyright (c) 2019 Ashley Jeffs
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

package condition

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestCheckInterpolationString(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		arg     string
		payload [][]byte
		want    bool
	}{
		{
			name:  "static value",
			value: "foo",
			arg:   "foo",
			payload: [][]byte{
				[]byte("foobar"),
			},
			want: true,
		},
		{
			name:  "batch size 1",
			value: "${!batch_size}",
			arg:   "1",
			payload: [][]byte{
				[]byte("foobar"),
			},
			want: true,
		},
		{
			name:  "batch size not 1",
			value: "${!batch_size}",
			arg:   "1",
			payload: [][]byte{
				[]byte("foobar"),
				[]byte("foobar"),
			},
			want: false,
		},
		{
			name:  "batch size 2",
			value: "${!batch_size}",
			arg:   "2",
			payload: [][]byte{
				[]byte("foobar"),
				[]byte("foobar"),
			},
			want: true,
		},
		{
			name:  "two interps",
			value: "${!batch_size}-${!json_field:id}",
			arg:   "1-foo",
			payload: [][]byte{
				[]byte(`{"id":"foo"}`),
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tConf := NewConfig()
			tConf.Type = "text"
			tConf.Text.Operator = "equals"
			tConf.Text.Arg = tt.arg

			conf := NewConfig()
			conf.Type = "check_interpolation"
			conf.CheckInterpolation.Value = tt.value
			conf.CheckInterpolation.Condition = &tConf

			c, err := NewCheckInterpolation(conf, nil, log.Noop(), metrics.Noop())
			if err != nil {
				t.Error(err)
				return
			}
			if got := c.Check(message.New(tt.payload)); got != tt.want {
				t.Errorf("CheckInterpolation.Check() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCheckInterpolationBadChild(t *testing.T) {
	conf := NewConfig()
	conf.Type = "check_interpolation"

	_, err := NewCheckInterpolation(conf, nil, log.Noop(), metrics.Noop())
	if err == nil {
		t.Error("expected error from bad child")
	}
}
