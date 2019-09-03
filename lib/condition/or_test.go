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

func TestOrCheck(t *testing.T) {
	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	testMet := metrics.DudType{}

	testMsg := message.New([][]byte{
		[]byte("foo"),
	})

	passConf := NewConfig()
	passConf.Text.Operator = "contains"
	passConf.Text.Arg = "foo"

	failConf := NewConfig()
	failConf.Text.Operator = "contains"
	failConf.Text.Arg = "bar"

	tests := []struct {
		name string
		arg  []Config
		want bool
	}{
		{
			name: "one pass",
			arg: []Config{
				passConf,
			},
			want: true,
		},
		{
			name: "two pass",
			arg: []Config{
				passConf,
				passConf,
			},
			want: true,
		},
		{
			name: "one fail",
			arg: []Config{
				failConf,
			},
			want: false,
		},
		{
			name: "two fail",
			arg: []Config{
				failConf,
				failConf,
			},
			want: false,
		},
		{
			name: "first fail",
			arg: []Config{
				failConf,
				passConf,
			},
			want: true,
		},
		{
			name: "second fail",
			arg: []Config{
				passConf,
				failConf,
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := NewConfig()
			conf.Type = "or"
			conf.Or = tt.arg

			c, err := New(conf, nil, testLog, testMet)
			if err != nil {
				t.Error(err)
				return
			}
			if got := c.Check(testMsg); got != tt.want {
				t.Errorf("Or.Check() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOrBadOperator(t *testing.T) {
	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	testMet := metrics.DudType{}

	cConf := NewConfig()
	cConf.Type = "text"
	cConf.Text.Operator = "NOT_EXIST"

	conf := NewConfig()
	conf.Type = "or"
	conf.Or = []Config{
		cConf,
	}

	_, err := NewOr(conf, nil, testLog, testMet)
	if err == nil {
		t.Error("expected error from bad operator")
	}
}
