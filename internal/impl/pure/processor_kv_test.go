package pure

import (
	"context"
	"reflect"
	"testing"

	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestKVProcessor(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		fieldSplit string
		valueSplit string
		output     map[string]string
	}{
		{
			name:       "simple log with key-value pairs",
			input:      "level=info msg=test-message",
			fieldSplit: " ",
			valueSplit: "=",
			output: map[string]string{
				"level": "info",
				"msg":   "test-message",
			},
		},
		{
			name:       "log with values containing spaces",
			input:      "level=info msg=this is a message logSource=source",
			fieldSplit: " ",
			valueSplit: "=",
			output: map[string]string{
				"level":     "info",
				"logSource": "source",
				"msg":       "this is a message",
			},
		},
		{
			name:       "complex log with quoted values",
			input:      "time=\"2018-07-04T09:36:25Z\" level=info msg=\"this is a message\" logSource=dummy source",
			fieldSplit: " ",
			valueSplit: "=",
			output: map[string]string{
				"level":     "info",
				"logSource": "dummy source",
				"msg":       "\"this is a message\"",
				"time":      "\"2018-07-04T09:36:25Z\"",
			},
		},
		{
			name:       "input with multiple field splits",
			input:      "key1=value1||key2=value2|key3=value3",
			fieldSplit: "|",
			valueSplit: "=",
			output: map[string]string{
				"key1": "value1",
				"key2": "value2",
				"key3": "value3",
			},
		},
		{
			name:       "input with empty values",
			input:      "key1=|key2=value2|key3=",
			fieldSplit: "|",
			valueSplit: "=",
			output: map[string]string{
				"key1": "",
				"key2": "value2",
				"key3": "",
			},
		},
	}

	for _, test := range tests {
		conf := processor.NewConfig()
		conf.Type = "kv"
		conf.KV.FieldSplit = test.fieldSplit
		conf.KV.ValueSplit = test.valueSplit

		proc, err := mock.NewManager().NewProcessor(conf)
		if err != nil {
			t.Fatal(err)
		}
		t.Run(test.name, func(tt *testing.T) {
			msgsOut, res := proc.ProcessBatch(context.Background(), message.QuickBatch([][]byte{[]byte(test.input)}))
			if res != nil {
				tt.Fatal(res)
			}
			if len(msgsOut) != 1 {
				tt.Fatalf("Wrong count of result messages: %v != 1", len(msgsOut))
			}
			structured, _ := msgsOut[0].Get(0).AsStructured()
			if exp, act := test.output, structured; !reflect.DeepEqual(exp, act) {
				tt.Errorf("Wrong result: %v != %v", exp, act)
			}
		})
	}
}
