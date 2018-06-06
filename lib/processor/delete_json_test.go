package processor

import (
	"os"
	"testing"

	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
)

func TestDeleteJSONValidation(t *testing.T) {
	conf := NewConfig()
	conf.DeleteJSON.Parts = []int{0}
	conf.DeleteJSON.Path = ""

	testLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})

	_, err := NewDeleteJSON(conf, nil, testLog, metrics.DudType{})
	if err == nil {
		t.Error("Expected error from empty path")
	}

	conf.DeleteJSON.Path = "."

	_, err = NewDeleteJSON(conf, nil, testLog, metrics.DudType{})
	if err == nil {
		t.Error("Expected error from root path")
	}
}

func TestDeleteJSONPartBounds(t *testing.T) {
	tLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	conf := NewConfig()
	conf.DeleteJSON.Path = "foo.bar"

	exp := `{"foo":{}}`

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

		conf.DeleteJSON.Parts = []int{i}
		proc, err := NewDeleteJSON(conf, nil, tLog, tStats)
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

func TestDeleteJSON(t *testing.T) {
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
			name:   "del field 1",
			path:   "foo.bar",
			input:  `{"foo":{"bar":5}}`,
			output: `{"foo":{}}`,
		},
		{
			name:   "del obj field 1",
			path:   "foo.bar",
			input:  `{"foo":{"bar":{"baz":5}}}`,
			output: `{"foo":{}}`,
		},
		{
			name:   "del array field 1",
			path:   "foo.bar",
			input:  `{"foo":{"bar":[5]}}`,
			output: `{"foo":{}}`,
		},
	}

	for _, test := range tests {
		conf := NewConfig()
		conf.DeleteJSON.Parts = []int{0}
		conf.DeleteJSON.Path = test.path

		jSet, err := NewDeleteJSON(conf, nil, tLog, tStats)
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
