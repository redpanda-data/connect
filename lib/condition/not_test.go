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
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	yaml "gopkg.in/yaml.v3"
)

func TestNotConfigMarshalJSON(t *testing.T) {
	conf := NewConfig()
	conf.Type = "not"

	exp := []byte(`"not":{}`)

	act, err := json.Marshal(conf)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Contains(act, exp) {
		t.Errorf("Wrong marshal result: %s does not contain %s", act, exp)
	}

	cConf := NewConfig()
	conf.Not.Config = &cConf

	var cMarshalled []byte
	if cMarshalled, err = json.Marshal(cConf); err != nil {
		t.Fatal(err)
	}

	exp = []byte(fmt.Sprintf(`"not":%s`, cMarshalled))

	act, err = json.Marshal(conf)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Contains(act, exp) {
		t.Errorf("Wrong marshal result: %s does not contain %s", act, exp)
	}
}

func TestNotConfigMarshalYAML(t *testing.T) {
	conf := NewConfig()
	conf.Type = "not"

	exp := []byte(`not: {}`)

	act, err := yaml.Marshal(conf)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Contains(act, exp) {
		t.Errorf("Wrong marshal result: %s does not contain %s", act, exp)
	}
}

func TestNotConfigDefaultsJSON(t *testing.T) {
	conf := []Config{}

	if err := json.Unmarshal([]byte(`[
		{
			"type": "not",
			"not": {
				"type": "text",
				"text": {
					"part": 1
				}
			}
		}
	]`), &conf); err != nil {
		t.Error(err)
	}

	if exp, act := 1, len(conf); exp != act {
		t.Errorf("Wrong number of config parts: %v != %v", act, exp)
		return
	}
	if exp, act := "text", conf[0].Not.Type; exp != act {
		t.Errorf("Wrong type: %v != %v", act, exp)
	}
	if exp, act := "equals_cs", conf[0].Not.Text.Operator; exp != act {
		t.Errorf("Wrong default operator: %v != %v", act, exp)
	}
	if exp, act := 1, conf[0].Not.Text.Part; exp != act {
		t.Errorf("Wrong default part: %v != %v", act, exp)
	}
}

func TestNotConfigDefaultsYAML(t *testing.T) {
	conf := []Config{}

	if err := yaml.Unmarshal([]byte(`[
		{
			"type": "not",
			"not": {
				"type": "text",
				"text": {
					"part": 1
				}
			}
		}
	]`), &conf); err != nil {
		t.Error(err)
	}

	if exp, act := 1, len(conf); exp != act {
		t.Errorf("Wrong number of config parts: %v != %v", act, exp)
		return
	}
	if exp, act := "text", conf[0].Not.Type; exp != act {
		t.Errorf("Wrong type: %v != %v", act, exp)
	}
	if exp, act := "equals_cs", conf[0].Not.Text.Operator; exp != act {
		t.Errorf("Wrong default operator: %v != %v", act, exp)
	}
	if exp, act := 1, conf[0].Not.Text.Part; exp != act {
		t.Errorf("Wrong default part: %v != %v", act, exp)
	}
}

func TestNotCheck(t *testing.T) {
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

			nConf := NewConfig()
			nConf.Type = "not"
			nConf.Not.Config = &conf

			c, err := New(nConf, nil, testLog, testMet)
			if err != nil {
				t.Error(err)
				return
			}
			if got := c.Check(message.New(tt.arg)); got == tt.want {
				t.Errorf("Text.Check() = %v, want %v", got, !tt.want)
			}
		})
	}
}

func TestNotBadOperator(t *testing.T) {
	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	testMet := metrics.DudType{}

	cConf := NewConfig()
	cConf.Type = "text"
	cConf.Text.Operator = "NOT_EXIST"

	conf := NewConfig()
	conf.Type = "not"
	conf.Not.Config = &cConf

	_, err := NewNot(conf, nil, testLog, testMet)
	if err == nil {
		t.Error("expected error from bad operator")
	}
}
