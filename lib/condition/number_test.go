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

func TestNumberCheck(t *testing.T) {
	type fields struct {
		operator string
		part     int
		arg      float64
	}
	tests := []struct {
		name   string
		fields fields
		arg    [][]byte
		want   bool
	}{
		{
			name: "equals 1",
			fields: fields{
				operator: "equals",
				part:     0,
				arg:      67,
			},
			arg: [][]byte{
				[]byte("67"),
			},
			want: true,
		},
		{
			name: "equals 2",
			fields: fields{
				operator: "equals",
				part:     0,
				arg:      67,
			},
			arg: [][]byte{
				[]byte("68"),
			},
			want: false,
		},
		{
			name: "equals 3",
			fields: fields{
				operator: "equals",
				part:     0,
				arg:      67,
			},
			arg: [][]byte{
				[]byte("nah"),
			},
			want: false,
		},
		{
			name: "greater than 1",
			fields: fields{
				operator: "greater_than",
				part:     0,
				arg:      50,
			},
			arg: [][]byte{
				[]byte("51"),
			},
			want: true,
		},
		{
			name: "greater than 2",
			fields: fields{
				operator: "greater_than",
				part:     1,
				arg:      50,
			},
			arg: [][]byte{
				[]byte("51"),
				[]byte("49"),
			},
			want: false,
		},
		{
			name: "greater than 3",
			fields: fields{
				operator: "greater_than",
				part:     0,
				arg:      50,
			},
			arg: [][]byte{
				[]byte("not a number"),
			},
			want: false,
		},
		{
			name: "less than 1",
			fields: fields{
				operator: "less_than",
				part:     0,
				arg:      50,
			},
			arg: [][]byte{
				[]byte("49"),
			},
			want: true,
		},
		{
			name: "less than 2",
			fields: fields{
				operator: "less_than",
				part:     0,
				arg:      50,
			},
			arg: [][]byte{
				[]byte("51"),
			},
			want: false,
		},
		{
			name: "less than 3",
			fields: fields{
				operator: "less_than",
				part:     0,
				arg:      50,
			},
			arg: [][]byte{
				[]byte("not a number"),
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := NewConfig()
			conf.Type = "number"
			conf.Number.Operator = tt.fields.operator
			conf.Number.Part = tt.fields.part
			conf.Number.Arg = tt.fields.arg

			c, err := NewNumber(conf, nil, log.Noop(), metrics.Noop())
			if err != nil {
				t.Error(err)
				return
			}
			if got := c.Check(message.New(tt.arg)); got != tt.want {
				t.Errorf("Number.Check() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNumberBadOperator(t *testing.T) {
	conf := NewConfig()
	conf.Type = "number"
	conf.Number.Operator = "NOT_EXIST"

	_, err := NewNumber(conf, nil, log.Noop(), metrics.Noop())
	if err == nil {
		t.Error("expected error from bad operator")
	}
}
