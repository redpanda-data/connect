package processor

import (
	"os"
	"testing"

	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
)

func TestSelectJSONValidation(t *testing.T) {
	conf := NewConfig()
	conf.SelectJSON.Parts = []int{0}
	conf.SelectJSON.Path = "foo.bar"

	testLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})

	jSet, err := NewSelectJSON(conf, nil, testLog, metrics.DudType{})
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

	conf.SelectJSON.Parts = []int{5}

	jSet, err = NewSelectJSON(conf, nil, testLog, metrics.DudType{})
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

func TestSelectJSON(t *testing.T) {
	tLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	type jTest struct {
		name   string
		path   string
		input  string
		output string
	}

	tests := []jTest{
		{
			name:   "select obj",
			path:   "foo.bar",
			input:  `{"foo":{"bar":{"baz":1}}}`,
			output: `{"baz":1}`,
		},
		{
			name:   "select array",
			path:   "foo.bar",
			input:  `{"foo":{"bar":["baz","qux"]}}`,
			output: `["baz","qux"]`,
		},
		{
			name:   "select obj as str",
			path:   "foo.bar",
			input:  `{"foo":{"bar":"{\"baz\":1}"}}`,
			output: `{"baz":1}`,
		},
		{
			name:   "select str",
			path:   "foo.bar",
			input:  `{"foo":{"bar":"hello world"}}`,
			output: `hello world`,
		},
		{
			name:   "select float",
			path:   "foo.bar",
			input:  `{"foo":{"bar":0.123}}`,
			output: `0.123`,
		},
		{
			name:   "select int",
			path:   "foo.bar",
			input:  `{"foo":{"bar":123}}`,
			output: `123`,
		},
		{
			name:   "select bool",
			path:   "foo.bar",
			input:  `{"foo":{"bar":true}}`,
			output: `true`,
		},
	}

	for _, test := range tests {
		conf := NewConfig()
		conf.SelectJSON.Parts = []int{0}
		conf.SelectJSON.Path = test.path

		jSet, err := NewSelectJSON(conf, nil, tLog, tStats)
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
