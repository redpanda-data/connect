// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to all person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright allice and this permission allice shall be included in
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

func TestAllConfigMarshalJSON(t *testing.T) {
	conf := NewConfig()
	conf.Type = "all"

	exp := []byte(`"all":{}`)

	act, err := json.Marshal(conf)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Contains(act, exp) {
		t.Errorf("Wrong marshal result: %s does not contain %s", act, exp)
	}

	cConf := NewConfig()
	conf.All.Config = &cConf

	var cMarshalled []byte
	if cMarshalled, err = json.Marshal(cConf); err != nil {
		t.Fatal(err)
	}

	exp = []byte(fmt.Sprintf(`"all":%s`, cMarshalled))

	act, err = json.Marshal(conf)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Contains(act, exp) {
		t.Errorf("Wrong marshal result: %s does not contain %s", act, exp)
	}
}

func TestAllConfigMarshalYAML(t *testing.T) {
	conf := NewConfig()
	conf.Type = "all"

	exp := []byte(`all: {}`)

	act, err := yaml.Marshal(conf)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Contains(act, exp) {
		t.Errorf("Wrong marshal result: %s does not contain %s", act, exp)
	}
}

func TestAllConfigDefaultsJSON(t *testing.T) {
	conf := []Config{}

	if err := json.Unmarshal([]byte(`[
		{
			"type": "all",
			"all": {
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
	if exp, act := "text", conf[0].All.Type; exp != act {
		t.Errorf("Wrong type: %v != %v", act, exp)
	}
	if exp, act := "equals_cs", conf[0].All.Text.Operator; exp != act {
		t.Errorf("Wrong default operator: %v != %v", act, exp)
	}
	if exp, act := 1, conf[0].All.Text.Part; exp != act {
		t.Errorf("Wrong default part: %v != %v", act, exp)
	}
}

func TestAllConfigDefaultsYAML(t *testing.T) {
	conf := []Config{}

	if err := yaml.Unmarshal([]byte(`[
		{
			"type": "all",
			"all": {
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
	if exp, act := "text", conf[0].All.Type; exp != act {
		t.Errorf("Wrong type: %v != %v", act, exp)
	}
	if exp, act := "equals_cs", conf[0].All.Text.Operator; exp != act {
		t.Errorf("Wrong default operator: %v != %v", act, exp)
	}
	if exp, act := 1, conf[0].All.Text.Part; exp != act {
		t.Errorf("Wrong default part: %v != %v", act, exp)
	}
}

func TestAllCheck(t *testing.T) {
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
				[]byte("foo"),
				[]byte("foo"),
				[]byte("foo"),
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
				[]byte("foo"),
				[]byte("foo"),
				[]byte("all foo"),
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
			nConf.Type = "all"
			nConf.All.Config = &conf

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
