package processor

import (
	"os"
	"strconv"
	"testing"

	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
)

func TestJMESPathAllParts(t *testing.T) {
	conf := NewConfig()
	conf.JMESPath.Parts = []int{}
	conf.JMESPath.Query = "foo.bar"

	testLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})

	jSet, err := NewJMESPath(conf, nil, testLog, metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}

	msgIn := types.NewMessage([][]byte{
		[]byte(`{"foo":{"bar":0}}`),
		[]byte(`{"foo":{"bar":1}}`),
		[]byte(`{"foo":{"bar":2}}`),
	})
	msgs, res := jSet.ProcessMessage(msgIn)
	if len(msgs) != 1 {
		t.Fatal("Wrong count of messages")
	}
	if res != nil {
		t.Fatal("Non-nil result")
	}
	for i, part := range msgs[0].GetAll() {
		if exp, act := strconv.Itoa(i), string(part); exp != act {
			t.Errorf("Wrong output from json: %v != %v", act, exp)
		}
	}
}

func TestJMESPathValidation(t *testing.T) {
	conf := NewConfig()
	conf.JMESPath.Parts = []int{0}
	conf.JMESPath.Query = "foo.bar"

	testLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})

	jSet, err := NewJMESPath(conf, nil, testLog, metrics.DudType{})
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

	conf.JMESPath.Parts = []int{5}

	jSet, err = NewJMESPath(conf, nil, testLog, metrics.DudType{})
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

func TestJMESPath(t *testing.T) {
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
			output: `"{\"baz\":1}"`,
		},
		{
			name:   "select str",
			path:   "foo.bar",
			input:  `{"foo":{"bar":"hello world"}}`,
			output: `"hello world"`,
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
		conf.JMESPath.Parts = []int{0}
		conf.JMESPath.Query = test.path

		jSet, err := NewJMESPath(conf, nil, tLog, tStats)
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
