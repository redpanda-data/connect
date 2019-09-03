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

package condition

import (
	"os"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestJMESPathCheck(t *testing.T) {
	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	testMet := metrics.DudType{}

	type fields struct {
		query string
		part  int
	}
	tests := []struct {
		name   string
		fields fields
		arg    [][]byte
		want   bool
	}{
		{
			name: "bool result pos",
			fields: fields{
				query: "foo == 'bar'",
				part:  0,
			},
			arg: [][]byte{
				[]byte(`{"foo":"bar"}`),
			},
			want: true,
		},
		{
			name: "bool result neg",
			fields: fields{
				query: "foo == 'bar'",
				part:  0,
			},
			arg: [][]byte{
				[]byte(`{"foo":"baz"}`),
			},
			want: false,
		},
		{
			name: "str result neg",
			fields: fields{
				query: "foo",
				part:  0,
			},
			arg: [][]byte{
				[]byte(`{"foo":"baz"}`),
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := NewConfig()
			conf.Type = "jmespath"
			conf.JMESPath.Query = tt.fields.query
			conf.JMESPath.Part = tt.fields.part

			c, err := NewJMESPath(conf, nil, testLog, testMet)
			if err != nil {
				t.Error(err)
				return
			}
			if got := c.Check(message.New(tt.arg)); got != tt.want {
				t.Errorf("JMESPath.Check() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJMESPathBadOperator(t *testing.T) {
	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	testMet := metrics.DudType{}

	conf := NewConfig()
	conf.Type = "jmespath"
	conf.JMESPath.Query = "this@#$@#$%@#%$@# is a bad query"

	_, err := NewJMESPath(conf, nil, testLog, testMet)
	if err == nil {
		t.Error("expected error from bad query")
	}
}
