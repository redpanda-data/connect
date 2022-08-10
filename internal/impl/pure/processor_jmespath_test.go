package pure_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/Jeffail/gabs/v2"

	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestJMESPathAllParts(t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "jmespath"
	conf.JMESPath.Query = "foo.bar"

	jSet, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgIn := message.QuickBatch([][]byte{
		[]byte(`{"foo":{"bar":0}}`),
		[]byte(`{"foo":{"bar":1}}`),
		[]byte(`{"foo":{"bar":2}}`),
	})
	msgs, res := jSet.ProcessBatch(context.Background(), msgIn)
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
	conf := processor.NewConfig()
	conf.Type = "jmespath"
	conf.JMESPath.Query = "foo.bar"

	jSet, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgIn := message.QuickBatch([][]byte{[]byte("this is bad json")})
	msgs, res := jSet.ProcessBatch(context.Background(), msgIn)
	if len(msgs) != 1 {
		t.Fatal("No passthrough for bad input data")
	}
	if res != nil {
		t.Fatal("Non-nil result")
	}
	if exp, act := "this is bad json", string(message.GetAllBytes(msgs[0])[0]); exp != act {
		t.Errorf("Wrong output from bad json: %v != %v", act, exp)
	}
}

func TestJMESPathMutation(t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "jmespath"
	conf.JMESPath.Query = "{foo: merge(foo, {bar:'baz'})}"

	jSet, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	ogObj := gabs.New()
	_, _ = ogObj.Set("is this", "foo", "original", "content")
	ogExp := ogObj.String()

	msgIn := message.QuickBatch(make([][]byte, 1))
	msgIn.Get(0).SetStructured(ogObj.Data())
	msgs, res := jSet.ProcessBatch(context.Background(), msgIn)
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
		conf := processor.NewConfig()
		conf.Type = "jmespath"
		conf.JMESPath.Query = test.path

		jSet, err := mock.NewManager().NewProcessor(conf)
		if err != nil {
			t.Fatalf("Error for test '%v': %v", test.name, err)
		}

		inMsg := message.QuickBatch(
			[][]byte{
				[]byte(test.input),
			},
		)
		msgs, _ := jSet.ProcessBatch(context.Background(), inMsg)
		if len(msgs) != 1 {
			t.Fatalf("Test '%v' did not succeed", test.name)
		}

		if exp, act := test.output, string(message.GetAllBytes(msgs[0])[0]); exp != act {
			t.Errorf("Wrong result '%v': %v != %v", test.name, act, exp)
		}
	}
}
