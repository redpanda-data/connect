package processor

import (
	b64 "encoding/base64"
	"testing"

	cmp "github.com/google/go-cmp/cmp"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

func TestMsgPackToJson(t *testing.T) {

	type testCase struct {
		name           string
		base64Input    string
		expectedOutput interface{}
	}

	tests := []testCase{
		{
			name:        "basic",
			base64Input: "iKNrZXmjZm9vp3RydWVLZXnDqGZhbHNlS2V5wqdudWxsS2V5wKZpbnRLZXnQe6hmbG9hdEtlectARszMzMzMzaVhcnJheZGjYmFypm5lc3RlZIGja2V5o2Jheg==",
			expectedOutput: map[string]interface{}{
				"key":      "foo",
				"trueKey":  true,
				"falseKey": false,
				"nullKey":  nil,
				"intKey":   int8(123),
				"floatKey": 45.6,
				"array": []interface{}{
					"bar",
				},
				"nested": map[string]interface{}{
					"key": "baz",
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			conf := NewConfig()
			conf.Type = TypeMsgPack
			conf.MsgPack.Operator = "to_json"

			proc, err := New(conf, nil, log.Noop(), metrics.Noop())
			if err != nil {
				tt.Fatal(err)
			}

			input := message.New(nil)
			inputBytes, err := b64.StdEncoding.DecodeString(test.base64Input)
			if err != nil {
				tt.Fatal(err)
			}
			input.Append(message.NewPart(inputBytes))

			msgs, res := proc.ProcessMessage(input)
			if res != nil {
				tt.Fatal(res.Error())
			}

			if len(msgs) != 1 {
				tt.Fatalf("Expected one message, received: %v", len(msgs))
			}
			act, err := msgs[0].Get(0).JSON()
			if err != nil {
				tt.Fatal(err)
			}
			validateResult(tt, act, test.expectedOutput, msgs)
		})
	}
}

func TestMsgPackFromJson(t *testing.T) {

	type testCase struct {
		name           string
		input          string
		expectedOutput interface{}
	}

	tests := []testCase{
		{
			name:  "basic",
			input: `{"key":"foo","trueKey":true,"falseKey":false,"nullKey":null,"intKey":123,"floatKey":45.6,"array":["bar"],"nested":{"key":"baz"}}`,
			expectedOutput: map[string]interface{}{
				"key":      "foo",
				"trueKey":  true,
				"falseKey": false,
				"nullKey":  nil,
				"intKey":   int8(123),
				"floatKey": 45.6,
				"array": []interface{}{
					"bar",
				},
				"nested": map[string]interface{}{
					"key": "baz",
				},
			},
		},
		{
			name:  "various ints",
			input: `{"int8": 13, "uint8": 254, "int16": -257, "uint16" : 65534, "int32" : -70123, "uint32" : 2147483648, "int64" : -9223372036854775808, "uint64": 18446744073709551615}`,
			expectedOutput: map[string]interface{}{
				"int8":   int8(13),
				"uint8":  uint8(254),
				"int16":  int16(-257),
				"uint16": uint16(65534),
				"int32":  int32(-70123),
				"uint32": uint32(2147483648),
				"int64":  int64(-9223372036854775808),
				"uint64": uint64(18446744073709551615),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			conf := NewConfig()
			conf.Type = TypeMsgPack
			conf.MsgPack.Operator = "from_json"

			proc, err := New(conf, nil, log.Noop(), metrics.Noop())
			if err != nil {
				tt.Fatal(err)
			}
			input := message.New([][]byte{[]byte(test.input)})

			msgs, res := proc.ProcessMessage(input)
			if res != nil {
				tt.Fatal(res.Error())
			}

			if len(msgs) != 1 {
				tt.Fatalf("Expected one message, received: %v", len(msgs))
			}
			var act interface{}
			if err := msgpack.Unmarshal(message.GetAllBytes(msgs[0])[0], &act); err != nil {
				tt.Fatalf("Unable to parse MessagePack out of result: %v", err)
			}
			validateResult(tt, act, test.expectedOutput, msgs)
		})
	}
}

func validateResult(tt *testing.T, actual, expected interface{}, msgs []types.Message) {
	if diff := cmp.Diff(actual, expected); diff != "" {
		tt.Errorf("Unexpected output (-want +got):\n%s", diff)
	}
	msgs[0].Iter(func(i int, part types.Part) error {
		if fail := part.Metadata().Get(FailFlagKey); len(fail) > 0 {
			tt.Error(fail)
		}
		return nil
	})
}
