package processor

import (
	"os"
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
)

func TestMergeJSON(t *testing.T) {
	tLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	type jTest struct {
		name   string
		first  string
		second string
		output string
	}

	tests := []jTest{
		{
			name:   "object 1",
			first:  `{"baz":{"foo":1}}`,
			second: `{"baz":{"bar":5}}`,
			output: `{"baz":{"bar":5,"foo":1}}`,
		},
		{
			name:   "val to array 1",
			first:  `{"baz":{"foo":3}}`,
			second: `{"baz":{"foo":5}}`,
			output: `{"baz":{"foo":[3,5]}}`,
		},
		{
			name:   "array 1",
			first:  `{"baz":{"foo":[1,2,3]}}`,
			second: `{"baz":{"foo":5}}`,
			output: `{"baz":{"foo":[1,2,3,5]}}`,
		},
	}

	for _, test := range tests {
		conf := NewConfig()
		conf.MergeJSON.Parts = []int{}
		conf.MergeJSON.RetainParts = false

		jMrg, err := NewMergeJSON(conf, nil, tLog, tStats)
		if err != nil {
			t.Fatalf("Error for test '%v': %v", test.name, err)
		}

		inMsg := types.NewMessage(
			[][]byte{
				[]byte(test.first),
				[]byte(test.second),
			},
		)
		msgs, _ := jMrg.ProcessMessage(inMsg)
		if len(msgs) != 1 {
			t.Fatalf("Test '%v' did not succeed", test.name)
		}

		if exp, act := test.output, string(msgs[0].GetAll()[0]); exp != act {
			t.Errorf("Wrong result '%v': %v != %v", test.name, act, exp)
		}
	}
}

func TestMergeJSONRetention(t *testing.T) {
	tLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	conf := NewConfig()
	conf.MergeJSON.Parts = []int{}
	conf.MergeJSON.RetainParts = true

	jMrg, err := NewMergeJSON(conf, nil, tLog, tStats)
	if err != nil {
		t.Fatal(err)
	}

	input := types.NewMessage(
		[][]byte{
			[]byte(`{"foo":1}`),
			[]byte(`{"foo":2}`),
		},
	)
	expOutput := input.ShallowCopy()
	expOutput.Append([]byte(`{"foo":[1,2]}`))
	exp := expOutput.GetAll()

	msgs, _ := jMrg.ProcessMessage(input)
	if len(msgs) != 1 {
		t.Errorf("Wrong output count")
	}
	act := msgs[0].GetAll()
	if !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}

	input = types.NewMessage(
		[][]byte{
			[]byte(`{"foo":1}`),
		},
	)
	expOutput = input.ShallowCopy()
	expOutput.Append([]byte(`{"foo":1}`))
	exp = expOutput.GetAll()

	msgs, _ = jMrg.ProcessMessage(input)
	if len(msgs) != 1 {
		t.Errorf("Wrong output count")
	}
	act = msgs[0].GetAll()
	if !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}

	conf = NewConfig()
	conf.MergeJSON.Parts = []int{0, -1}
	conf.MergeJSON.RetainParts = true

	jMrg, err = NewMergeJSON(conf, nil, tLog, tStats)
	if err != nil {
		t.Fatal(err)
	}

	input = types.NewMessage(
		[][]byte{
			[]byte(`{"foo":1}`),
			[]byte(`not related`),
			[]byte(`{"foo":2}`),
		},
	)
	expOutput = input.ShallowCopy()
	expOutput.Append([]byte(`{"foo":[1,2]}`))
	exp = expOutput.GetAll()

	msgs, _ = jMrg.ProcessMessage(input)
	if len(msgs) != 1 {
		t.Errorf("Wrong output count")
	}
	act = msgs[0].GetAll()
	if !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}

	input = types.NewMessage(
		[][]byte{
			[]byte(`{"foo":1}`),
		},
	)
	expOutput = input.ShallowCopy()
	expOutput.Append([]byte(`{"foo":1}`))
	exp = expOutput.GetAll()

	msgs, _ = jMrg.ProcessMessage(input)
	if len(msgs) != 1 {
		t.Errorf("Wrong output count")
	}
	act = msgs[0].GetAll()
	if !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
}
