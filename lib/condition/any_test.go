// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright anyice and this permission anyice shall be included in
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
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	yaml "gopkg.in/yaml.v3"
)

func TestAnyConfigMarshalJSON(t *testing.T) {
	conf := NewConfig()
	conf.Type = "any"

	exp := []byte(`"any":{}`)

	act, err := json.Marshal(conf)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Contains(act, exp) {
		t.Errorf("Wrong marshal result: %s does not contain %s", act, exp)
	}

	cConf := NewConfig()
	conf.Any.Config = &cConf

	var cMarshalled []byte
	if cMarshalled, err = json.Marshal(cConf); err != nil {
		t.Fatal(err)
	}

	exp = []byte(fmt.Sprintf(`"any":%s`, cMarshalled))

	act, err = json.Marshal(conf)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Contains(act, exp) {
		t.Errorf("Wrong marshal result: %s does not contain %s", act, exp)
	}
}

func TestAnyConfigMarshalYAML(t *testing.T) {
	conf := NewConfig()
	conf.Type = "any"

	exp := []byte(`any: {}`)

	act, err := yaml.Marshal(conf)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Contains(act, exp) {
		t.Errorf("Wrong marshal result: %s does not contain %s", act, exp)
	}
}

func TestAnyConfigDefaultsJSON(t *testing.T) {
	conf := []Config{}

	if err := json.Unmarshal([]byte(`[
		{
			"type": "any",
			"any": {
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
	if exp, act := "text", conf[0].Any.Type; exp != act {
		t.Errorf("Wrong type: %v != %v", act, exp)
	}
	if exp, act := "equals_cs", conf[0].Any.Text.Operator; exp != act {
		t.Errorf("Wrong default operator: %v != %v", act, exp)
	}
	if exp, act := 1, conf[0].Any.Text.Part; exp != act {
		t.Errorf("Wrong default part: %v != %v", act, exp)
	}
}

func TestAnyConfigDefaultsYAML(t *testing.T) {
	conf := []Config{}

	if err := yaml.Unmarshal([]byte(`[
		{
			"type": "any",
			"any": {
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
	if exp, act := "text", conf[0].Any.Type; exp != act {
		t.Errorf("Wrong type: %v != %v", act, exp)
	}
	if exp, act := "equals_cs", conf[0].Any.Text.Operator; exp != act {
		t.Errorf("Wrong default operator: %v != %v", act, exp)
	}
	if exp, act := 1, conf[0].Any.Text.Part; exp != act {
		t.Errorf("Wrong default part: %v != %v", act, exp)
	}
}

func TestAnyCheck(t *testing.T) {
	type fields struct {
		operator string
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
				arg:      "foo",
			},
			arg: [][]byte{
				[]byte("bar"),
				[]byte("foo"),
				[]byte("baz"),
			},
			want: true,
		},
		{
			name: "equals_cs foo neg",
			fields: fields{
				operator: "equals_cs",
				arg:      "foo",
			},
			arg: [][]byte{
				[]byte("any foo"),
			},
			want: false,
		},
		{
			name: "equals foo pos",
			fields: fields{
				operator: "equals",
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
				arg:      "foo",
			},
			arg: [][]byte{
				[]byte("bar"),
				[]byte("foo"),
			},
			want: true,
		},
		{
			name: "equals_cs neg empty msg",
			fields: fields{
				operator: "equals_cs",
				arg:      "foo",
			},
			arg:  [][]byte{},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := NewConfig()
			conf.Type = "text"
			conf.Text.Operator = tt.fields.operator
			conf.Text.Arg = tt.fields.arg

			nConf := NewConfig()
			nConf.Type = "any"
			nConf.Any.Config = &conf

			c, err := New(nConf, nil, log.Noop(), metrics.Noop())
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
