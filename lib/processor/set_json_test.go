package processor

import (
	"os"
	"testing"

	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	yaml "gopkg.in/yaml.v2"
)

func TestSetJSONValidation(t *testing.T) {
	conf := NewConfig()
	conf.SetJSON.Parts = []int{0}
	conf.SetJSON.Path = "foo.bar"
	conf.SetJSON.Value = []byte(`this isnt valid json`)

	testLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})

	jSet, err := NewSetJSON(conf, nil, testLog, metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}

	msgIn := types.NewMessage([][]byte{[]byte("this is bad json")})
	msgs, res := jSet.ProcessMessage(msgIn)
	if len(msgs) != 1 {
		t.Fatal("No passthrough for bad input data")
	}
	if res != nil {
		t.Fatal("Non-nil result")
	}
	if exp, act := "this is bad json", string(msgs[0].GetAll()[0]); exp != act {
		t.Errorf("Wrong output from bad json: %v != %v", act, exp)
	}

	conf.SetJSON.Parts = []int{5}

	jSet, err = NewSetJSON(conf, nil, testLog, metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}

	msgIn = types.NewMessage([][]byte{[]byte("{}")})
	msgs, res = jSet.ProcessMessage(msgIn)
	if len(msgs) != 1 {
		t.Fatal("No passthrough for bad index")
	}
	if res != nil {
		t.Fatal("Non-nil result")
	}
	if exp, act := "{}", string(msgs[0].GetAll()[0]); exp != act {
		t.Errorf("Wrong output from bad index: %v != %v", act, exp)
	}
}

func TestSetJSONPartBounds(t *testing.T) {
	tLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	conf := NewConfig()
	conf.SetJSON.Path = "foo.bar"
	conf.SetJSON.Value = []byte(`{"baz":1}`)

	exp := `{"foo":{"bar":{"baz":1}}}`

	tests := map[int]int{
		-3: 0,
		-2: 1,
		-1: 2,
		0:  0,
		1:  1,
		2:  2,
	}

	for i, j := range tests {
		input := [][]byte{
			[]byte(`{"foo":{"bar":2}}`),
			[]byte(`{"foo":{"bar":2}}`),
			[]byte(`{"foo":{"bar":2}}`),
		}

		conf.SetJSON.Parts = []int{i}
		proc, err := NewSetJSON(conf, nil, tLog, tStats)
		if err != nil {
			t.Fatal(err)
		}

		msgs, res := proc.ProcessMessage(types.NewMessage(input))
		if len(msgs) != 1 {
			t.Errorf("Select Parts failed on index: %v", i)
		} else if res != nil {
			t.Errorf("Expected nil response: %v", res)
		}
		if act := string(msgs[0].GetAll()[j]); exp != act {
			t.Errorf("Unexpected output for index %v: %v != %v", i, act, exp)
		}
	}
}

func TestSetJSON(t *testing.T) {
	tLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	type jTest struct {
		name   string
		path   string
		value  string
		input  string
		output string
	}

	tests := []jTest{
		{
			name:   "set 1",
			path:   "foo.bar",
			value:  `{"baz":1}`,
			input:  `{"foo":{"bar":5}}`,
			output: `{"foo":{"bar":{"baz":1}}}`,
		},
		{
			name:   "set 2",
			path:   "foo",
			value:  `5`,
			input:  `{"foo":{"bar":5}}`,
			output: `{"foo":5}`,
		},
		{
			name:   "set 3",
			path:   "foo",
			value:  `"5"`,
			input:  `{"foo":{"bar":5}}`,
			output: `{"foo":"5"}`,
		},
		{
			name: "set 4",
			path: "foo.bar",
			value: `{
				"baz": 1
			}`,
			input:  `{"foo":{"bar":5}}`,
			output: `{"foo":{"bar":{"baz":1}}}`,
		},
		{
			name:   "set 5",
			path:   "foo.bar",
			value:  `{"baz":${!echo:"foo"}}`,
			input:  `{"foo":{"bar":5}}`,
			output: `{"foo":{"bar":{"baz":"foo"}}}`,
		},
		{
			name:   "set 6",
			path:   "foo.bar",
			value:  `${!echo:10}`,
			input:  `{"foo":{"bar":5}}`,
			output: `{"foo":{"bar":10}}`,
		},
		{
			name:   "set root 1",
			path:   "",
			value:  `{"baz":1}`,
			input:  `hello world`,
			output: `{"baz":1}`,
		},
		{
			name:   "set root 2",
			path:   ".",
			value:  `{"baz":1}`,
			input:  `{"foo":2}`,
			output: `{"baz":1}`,
		},
	}

	for _, test := range tests {
		conf := NewConfig()
		conf.SetJSON.Parts = []int{0}
		conf.SetJSON.Path = test.path
		conf.SetJSON.Value = []byte(test.value)

		jSet, err := NewSetJSON(conf, nil, tLog, tStats)
		if err != nil {
			t.Fatalf("Error for test '%v': %v", test.name, err)
		}

		inMsg := types.NewMessage(
			[][]byte{
				[]byte(test.input),
			},
		)
		msgs, _ := jSet.ProcessMessage(inMsg)
		if len(msgs) != 1 {
			t.Fatalf("Test '%v' did not succeed", test.name)
		}

		if exp, act := test.output, string(msgs[0].GetAll()[0]); exp != act {
			t.Errorf("Wrong result '%v': %v != %v", test.name, act, exp)
		}
	}
}

func TestSetJSONConfigYAML(t *testing.T) {
	tLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	input := `{"foo":{"bar":5}}`

	tests := map[string]string{
		`value: 10`:            `{"foo":{"bar":10}}`,
		`value: "hello world"`: `{"foo":{"bar":"hello world"}}`,
		`value: hello world`:   `{"foo":{"bar":"hello world"}}`,
		`
value:
  baz: 10`: `{"foo":{"bar":{"baz":10}}}`,
		`
value:
  baz:
  - first
  - 2
  - third`: `{"foo":{"bar":{"baz":["first",2,"third"]}}}`,
		`
value:
  baz:
    deeper: look at me
  here: 11`: `{"foo":{"bar":{"baz":{"deeper":"look at me"},"here":11}}}`,
	}

	for config, exp := range tests {
		conf := NewConfig()
		conf.SetJSON.Parts = []int{}
		conf.SetJSON.Path = "foo.bar"

		if err := yaml.Unmarshal([]byte(config), &conf.SetJSON); err != nil {
			t.Fatal(err)
		}

		jSet, err := NewSetJSON(conf, nil, tLog, tStats)
		if err != nil {
			t.Fatalf("Error creating proc '%v': %v", config, err)
		}

		inMsg := types.NewMessage(
			[][]byte{
				[]byte(input),
			},
		)
		msgs, _ := jSet.ProcessMessage(inMsg)
		if len(msgs) != 1 {
			t.Fatalf("Test did not succeed with config: %v", config)
		}

		if act := string(msgs[0].GetAll()[0]); exp != act {
			t.Errorf("Wrong result '%v': %v != %v", config, act, exp)
		}
	}
}

func TestSetJSONConfigYAMLMarshal(t *testing.T) {
	tLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	tests := []string{
		`parts:
- 0
path: foo.bar
value:
  baz:
    deeper: look at me
  here: 11
`,
		`parts:
- 0
path: foo.bar
value:
  baz:
    deeper:
    - first
    - second
    - third
  here: 11
`,
		`parts:
- 5
path: foo.bar.baz
value: 5
`,
		`parts:
- 0
path: foo.bar
value: hello world
`,
		`parts:
- 0
path: foo.bar
value:
  foo:
    bar:
      baz:
        value: true
`,
	}

	for _, config := range tests {
		conf := NewConfig()
		if err := yaml.Unmarshal([]byte(config), &conf.SetJSON); err != nil {
			t.Error(err)
			continue
		}

		if act, err := yaml.Marshal(conf.SetJSON); err != nil {
			t.Error(err)
		} else if string(act) != config {
			t.Errorf("Marshalled config does not match: %v != %v", string(act), config)
		}

		if _, err := NewSetJSON(conf, nil, tLog, tStats); err != nil {
			t.Errorf("Error creating proc '%v': %v", config, err)
		}
	}
}
