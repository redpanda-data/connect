package processor

import (
	"strconv"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/gabs/v2"
)

func TestJMESPathAllParts(t *testing.T) {
	conf := NewConfig()
	conf.JMESPath.Parts = []int{}
	conf.JMESPath.Query = "foo.bar"

	testLog := log.Noop()

	jSet, err := NewJMESPath(conf, nil, testLog, metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgIn := message.QuickBatch([][]byte{
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
	for i, part := range message.GetAllBytes(msgs[0]) {
		if exp, act := strconv.Itoa(i), string(part); exp != act {
			t.Errorf("Wrong output from json: %v != %v", act, exp)
		}
	}
}

func TestJMESPathValidation(t *testing.T) {
	conf := NewConfig()
	conf.JMESPath.Parts = []int{0}
	conf.JMESPath.Query = "foo.bar"

	testLog := log.Noop()

	jSet, err := NewJMESPath(conf, nil, testLog, metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgIn := message.QuickBatch([][]byte{[]byte("this is bad json")})
	msgs, res := jSet.ProcessMessage(msgIn)
	if len(msgs) != 1 {
		t.Fatal("No passthrough for bad input data")
	}
	if res != nil {
		t.Fatal("Non-nil result")
	}
	if exp, act := "this is bad json", string(message.GetAllBytes(msgs[0])[0]); exp != act {
		t.Errorf("Wrong output from bad json: %v != %v", act, exp)
	}

	conf.JMESPath.Parts = []int{5}

	jSet, err = NewJMESPath(conf, nil, testLog, metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgIn = message.QuickBatch([][]byte{[]byte("{}")})
	msgs, res = jSet.ProcessMessage(msgIn)
	if len(msgs) != 1 {
		t.Fatal("No passthrough for bad index")
	}
	if res != nil {
		t.Fatal("Non-nil result")
	}
	if exp, act := "{}", string(message.GetAllBytes(msgs[0])[0]); exp != act {
		t.Errorf("Wrong output from bad index: %v != %v", act, exp)
	}
}

func TestJMESPathMutation(t *testing.T) {
	conf := NewConfig()
	conf.JMESPath.Query = "{foo: merge(foo, {bar:'baz'})}"

	jSet, err := NewJMESPath(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	ogObj := gabs.New()
	ogObj.Set("is this", "foo", "original", "content")
	ogExp := ogObj.String()

	msgIn := message.QuickBatch(make([][]byte, 1))
	msgIn.Get(0).SetJSON(ogObj.Data())
	msgs, res := jSet.ProcessMessage(msgIn)
	if len(msgs) != 1 {
		t.Fatal("No passthrough for bad input data")
	}
	if res != nil {
		t.Fatal("Non-nil result")
	}
	if exp, act := `{"foo":{"bar":"baz","original":{"content":"is this"}}}`, string(message.GetAllBytes(msgs[0])[0]); exp != act {
		t.Errorf("Wrong output: %v != %v", act, exp)
	}

	if exp, act := ogExp, ogObj.String(); exp != act {
		t.Errorf("Original contents were mutated: %v != %v", act, exp)
	}
}

func TestJMESPath(t *testing.T) {
	tLog := log.Noop()
	tStats := metrics.Noop()

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
		{
			name:   "addition int",
			path:   "sum([foo.bar, `6`])",
			input:  `{"foo":{"bar":123}}`,
			output: `129`,
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

		inMsg := message.QuickBatch(
			[][]byte{
				[]byte(test.input),
			},
		)
		msgs, _ := jSet.ProcessMessage(inMsg)
		if len(msgs) != 1 {
			t.Fatalf("Test '%v' did not succeed", test.name)
		}

		if exp, act := test.output, string(message.GetAllBytes(msgs[0])[0]); exp != act {
			t.Errorf("Wrong result '%v': %v != %v", test.name, act, exp)
		}
	}
}
