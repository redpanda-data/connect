package processor

import (
	"github.com/vmihailenco/msgpack/v5"
	r "reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

func TestMsgPackBasic(t *testing.T) {

	type testCase struct {
		name               string
		operator           string
		input              string
		expectedOutput     interface{}
		parseMsgPackOutput bool
	}

	tests := []testCase{
		{
			name:     "json to msgpack basic",
			operator: "from_json",
			input:    `{"key":"foo","trueKey":true,"falseKey":false,"nullKey":null,"intKey":123,"floatKey":45.6,"array":["bar"],"nested":{"key":"baz"}}`,
			expectedOutput: map[string]interface{}{
				"key":      "foo",
				"trueKey":  true,
				"falseKey": false,
				"nullKey":  nil,
				"intKey":   123,
				"floatKey": 45.6,
				"array":    []string{"bar"},
				"nested": map[string]interface{}{
					"key": "baz",
				},
			},
			parseMsgPackOutput: true,
		},
		/*{
			name:     "msgpack to json basic",
			operator: "to_json",
			input: []string{
				"\x88\xa6intKey\xa3123\xa8floatKey\xa445.6\xa5array\x91\xa3bar\xa6nested\x81\xa3key\xa3baz\xa3key\xa3foo\xa7trueKeyèfalseKey§nullKey\xc0",
			},
			output: []string{
				`{"key":"foo","trueKey":true,"falseKey":false,"nullKey":null,"intKey":123,"floatKey":45.6,"array":["bar"],"nested":{"key":"baz"}}`,
			},
		},*/
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			conf := NewConfig()
			conf.Type = TypeMsgPack
			conf.MsgPack.Operator = test.operator

			proc, err := New(conf, nil, log.Noop(), metrics.Noop())
			if err != nil {
				tt.Fatal(err)
			}

			input := message.New(nil)
			input.Append(message.NewPart([]byte(test.input)))

			msgs, res := proc.ProcessMessage(input)
			if res != nil {
				tt.Fatal(res.Error())
			}

			if len(msgs) != 1 {
				tt.Fatalf("Expected one message, received: %v", len(msgs))
			}
			var act interface{}
			actBytes := message.GetAllBytes(msgs[0])[0]
			if test.parseMsgPackOutput {
				err := msgpack.Unmarshal(actBytes, &act)
				if err != nil {
					tt.Errorf("Unable to parse MessagePack out of result: %v", err)
				}
			} else {
				act = actBytes
			}
			if !r.DeepEqual(act, test.expectedOutput) {
				//sliceOfBytes := r.SliceOf(r.TypeOf(byte('A')))
				//tt.Errorf("Unexpected output:\n  expected %+q\n  actual   %+q", exp[0], act[0])
				tt.Errorf("Unexpected output:\n  expected %s\n  actual   %s", test.expectedOutput, act)
			}
			msgs[0].Iter(func(i int, part types.Part) error {
				if fail := part.Metadata().Get(FailFlagKey); len(fail) > 0 {
					tt.Error(fail)
				}
				return nil
			})
		})
	}
}
