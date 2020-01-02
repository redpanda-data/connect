package processor

import (
	"os"
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestMergeJSON(t *testing.T) {
	tLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
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

		inMsg := message.New(
			[][]byte{
				[]byte(test.first),
				[]byte(test.second),
			},
		)
		msgs, _ := jMrg.ProcessMessage(inMsg)
		if len(msgs) != 1 {
			t.Fatalf("Test '%v' did not succeed", test.name)
		}

		if exp, act := test.output, string(message.GetAllBytes(msgs[0])[0]); exp != act {
			t.Errorf("Wrong result '%v': %v != %v", test.name, act, exp)
		}
	}
}

func TestMergeJSONRetention(t *testing.T) {
	tLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	conf := NewConfig()
	conf.MergeJSON.Parts = []int{}
	conf.MergeJSON.RetainParts = true

	jMrg, err := NewMergeJSON(conf, nil, tLog, tStats)
	if err != nil {
		t.Fatal(err)
	}

	input := message.New(
		[][]byte{
			[]byte(`{"foo":1}`),
			[]byte(`{"foo":2}`),
		},
	)
	expOutput := input.Copy()
	expOutput.Append(message.NewPart([]byte(`{"foo":[1,2]}`)))
	exp := message.GetAllBytes(expOutput)

	msgs, _ := jMrg.ProcessMessage(input)
	if len(msgs) != 1 {
		t.Errorf("Wrong output count")
	}
	act := message.GetAllBytes(msgs[0])
	if !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}

	input = message.New(
		[][]byte{
			[]byte(`{"foo":1}`),
		},
	)
	expOutput = input.Copy()
	expOutput.Append(message.NewPart([]byte(`{"foo":1}`)))
	exp = message.GetAllBytes(expOutput)

	msgs, _ = jMrg.ProcessMessage(input)
	if len(msgs) != 1 {
		t.Errorf("Wrong output count")
	}
	act = message.GetAllBytes(msgs[0])
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

	input = message.New(
		[][]byte{
			[]byte(`{"foo":1}`),
			[]byte(`not related`),
			[]byte(`{"foo":2}`),
		},
	)
	expOutput = input.Copy()
	expOutput.Append(message.NewPart([]byte(`{"foo":[1,2]}`)))
	exp = message.GetAllBytes(expOutput)

	msgs, _ = jMrg.ProcessMessage(input)
	if len(msgs) != 1 {
		t.Errorf("Wrong output count")
	}
	act = message.GetAllBytes(msgs[0])
	if !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}

	input = message.New(
		[][]byte{
			[]byte(`{"foo":1}`),
		},
	)
	expOutput = input.Copy()
	expOutput.Append(message.NewPart([]byte(`{"foo":1}`)))
	exp = message.GetAllBytes(expOutput)

	msgs, _ = jMrg.ProcessMessage(input)
	if len(msgs) != 1 {
		t.Errorf("Wrong output count")
	}
	act = message.GetAllBytes(msgs[0])
	if !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
}

func TestMergeJSONNoRetention(t *testing.T) {
	tLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	conf := NewConfig()
	conf.MergeJSON.Parts = []int{0, -1}
	conf.MergeJSON.RetainParts = false

	jMrg, err := NewMergeJSON(conf, nil, tLog, tStats)
	if err != nil {
		t.Fatal(err)
	}

	input := message.New(
		[][]byte{
			[]byte(`{"foo":1}`),
			[]byte(`not related`),
			[]byte(`{"foo":2}`),
		},
	)
	input.Get(0).Metadata().Set("foo", "1")
	input.Get(1).Metadata().Set("foo", "2")
	input.Get(2).Metadata().Set("foo", "3")

	expParts := [][]byte{
		[]byte(`not related`),
		[]byte(`{"foo":[1,2]}`),
	}

	msgs, _ := jMrg.ProcessMessage(input)
	if len(msgs) != 1 {
		t.Errorf("Wrong output count")
	}
	if act, exp := message.GetAllBytes(msgs[0]), expParts; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
	if exp, act := "2", msgs[0].Get(0).Metadata().Get("foo"); exp != act {
		t.Errorf("Wrong metadata: %v != %v", act, exp)
	}
	if exp, act := "1", msgs[0].Get(1).Metadata().Get("foo"); exp != act {
		t.Errorf("Wrong metadata: %v != %v", act, exp)
	}
}
