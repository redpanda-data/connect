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
	"net/http"
	"os"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

type fakeMgr struct {
	conds map[string]Type
}

func (f *fakeMgr) RegisterEndpoint(path, desc string, h http.HandlerFunc) {
}
func (f *fakeMgr) GetCache(name string) (types.Cache, error) {
	return nil, types.ErrCacheNotFound
}
func (f *fakeMgr) GetCondition(name string) (types.Condition, error) {
	if c, exists := f.conds[name]; exists {
		return c, nil
	}
	return nil, types.ErrConditionNotFound
}
func (f *fakeMgr) GetRateLimit(name string) (types.RateLimit, error) {
	return nil, types.ErrRateLimitNotFound
}
func (f *fakeMgr) GetPlugin(name string) (interface{}, error) {
	return nil, types.ErrPluginNotFound
}
func (f *fakeMgr) GetPipe(name string) (<-chan types.Transaction, error) {
	return nil, types.ErrPipeNotFound
}
func (f *fakeMgr) SetPipe(name string, prod <-chan types.Transaction)   {}
func (f *fakeMgr) UnsetPipe(name string, prod <-chan types.Transaction) {}

func TestResourceCheck(t *testing.T) {
	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	testMet := metrics.DudType{}

	type fields struct {
		operator string
		part     int
		arg      string
	}
	tests := []struct {
		name   string
		fields fields
		arg    [][]byte
		want   bool
	}{
		{
			name: "equals_cs foo pos",
			fields: fields{
				operator: "equals_cs",
				part:     0,
				arg:      "foo",
			},
			arg: [][]byte{
				[]byte("foo"),
			},
			want: true,
		},
		{
			name: "equals_cs foo neg",
			fields: fields{
				operator: "equals_cs",
				part:     0,
				arg:      "foo",
			},
			arg: [][]byte{
				[]byte("not foo"),
			},
			want: false,
		},
		{
			name: "equals foo pos",
			fields: fields{
				operator: "equals",
				part:     0,
				arg:      "fOo",
			},
			arg: [][]byte{
				[]byte("foo"),
			},
			want: true,
		},
		{
			name: "equals foo pos 2",
			fields: fields{
				operator: "equals",
				part:     0,
				arg:      "foo",
			},
			arg: [][]byte{
				[]byte("fOo"),
			},
			want: true,
		},
		{
			name: "equals foo neg",
			fields: fields{
				operator: "equals",
				part:     0,
				arg:      "fOo",
			},
			arg: [][]byte{
				[]byte("f0o"),
			},
			want: false,
		},
		{
			name: "contains_cs foo pos",
			fields: fields{
				operator: "contains_cs",
				part:     0,
				arg:      "foo",
			},
			arg: [][]byte{
				[]byte("hello foo world"),
			},
			want: true,
		},
		{
			name: "contains_cs foo neg",
			fields: fields{
				operator: "contains_cs",
				part:     0,
				arg:      "foo",
			},
			arg: [][]byte{
				[]byte("hello fOo world"),
			},
			want: false,
		},
		{
			name: "contains foo pos",
			fields: fields{
				operator: "contains",
				part:     0,
				arg:      "fOo",
			},
			arg: [][]byte{
				[]byte("hello foo world"),
			},
			want: true,
		},
		{
			name: "contains foo pos 2",
			fields: fields{
				operator: "contains",
				part:     0,
				arg:      "foo",
			},
			arg: [][]byte{
				[]byte("hello fOo world"),
			},
			want: true,
		},
		{
			name: "contains foo neg",
			fields: fields{
				operator: "contains",
				part:     0,
				arg:      "fOo",
			},
			arg: [][]byte{
				[]byte("hello f0o world"),
			},
			want: false,
		},
		{
			name: "equals_cs foo pos from neg index",
			fields: fields{
				operator: "equals_cs",
				part:     -1,
				arg:      "foo",
			},
			arg: [][]byte{
				[]byte("bar"),
				[]byte("foo"),
			},
			want: true,
		},
		{
			name: "equals_cs foo neg from neg index",
			fields: fields{
				operator: "equals_cs",
				part:     -2,
				arg:      "foo",
			},
			arg: [][]byte{
				[]byte("bar"),
				[]byte("foo"),
			},
			want: false,
		},
		{
			name: "equals_cs neg empty msg",
			fields: fields{
				operator: "equals_cs",
				part:     0,
				arg:      "foo",
			},
			arg:  [][]byte{},
			want: false,
		},
		{
			name: "equals_cs neg oob",
			fields: fields{
				operator: "equals_cs",
				part:     1,
				arg:      "foo",
			},
			arg: [][]byte{
				[]byte("foo"),
			},
			want: false,
		},
		{
			name: "equals_cs neg oob neg index",
			fields: fields{
				operator: "equals_cs",
				part:     -2,
				arg:      "foo",
			},
			arg: [][]byte{
				[]byte("foo"),
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := NewConfig()
			conf.Type = "text"
			conf.Text.Operator = tt.fields.operator
			conf.Text.Part = tt.fields.part
			conf.Text.Arg = tt.fields.arg

			resCond, err := New(conf, nil, testLog, testMet)
			if err != nil {
				t.Fatal(err)
			}

			mgr := &fakeMgr{
				conds: map[string]Type{
					"foo": resCond,
				},
			}

			nConf := NewConfig()
			nConf.Type = "resource"
			nConf.Resource = "foo"

			c, err := New(nConf, mgr, testLog, testMet)
			if err != nil {
				t.Error(err)
				return
			}
			if got := c.Check(message.New(tt.arg)); got != tt.want {
				t.Errorf("Text.Check() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestResourceBadName(t *testing.T) {
	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	testMet := metrics.DudType{}

	mgr := &fakeMgr{
		conds: map[string]Type{},
	}

	conf := NewConfig()
	conf.Type = "resource"
	conf.Resource = "foo"

	_, err := NewResource(conf, mgr, testLog, testMet)
	if err == nil {
		t.Error("expected error from bad resource")
	}
}
