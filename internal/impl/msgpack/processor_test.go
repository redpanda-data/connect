package msgpack

import (
	"context"
	b64 "encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/benthosdev/benthos/v4/public/service"
)

func TestMsgPackToJson(t *testing.T) {
	type testCase struct {
		name           string
		base64Input    string
		expectedOutput any
	}

	tests := []testCase{
		{
			name:        "basic",
			base64Input: "iKNrZXmjZm9vp3RydWVLZXnDqGZhbHNlS2V5wqdudWxsS2V5wKZpbnRLZXnQe6hmbG9hdEtlectARszMzMzMzaVhcnJheZGjYmFypm5lc3RlZIGja2V5o2Jheg==",
			expectedOutput: map[string]any{
				"key":      "foo",
				"trueKey":  true,
				"falseKey": false,
				"nullKey":  nil,
				"intKey":   int8(123),
				"floatKey": 45.6,
				"array": []any{
					"bar",
				},
				"nested": map[string]any{
					"key": "baz",
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proc, err := newProcessor("to_json")
			require.NoError(t, err)

			inputBytes, err := b64.StdEncoding.DecodeString(test.base64Input)
			require.NoError(t, err)

			input := service.NewMessage(inputBytes)

			msgs, err := proc.Process(context.Background(), input)
			require.NoError(t, err)
			require.Len(t, msgs, 1)

			act, err := msgs[0].AsStructured()
			require.NoError(t, err)

			assert.Equal(t, test.expectedOutput, act)
		})
	}
}

func TestMsgPackFromJson(t *testing.T) {
	type testCase struct {
		name           string
		input          string
		expectedOutput any
	}

	tests := []testCase{
		{
			name:  "basic",
			input: `{"key":"foo","trueKey":true,"falseKey":false,"nullKey":null,"intKey":123,"floatKey":45.6,"array":["bar"],"nested":{"key":"baz"}}`,
			expectedOutput: map[string]any{
				"key":      "foo",
				"trueKey":  true,
				"falseKey": false,
				"nullKey":  nil,
				"intKey":   int8(123),
				"floatKey": 45.6,
				"array": []any{
					"bar",
				},
				"nested": map[string]any{
					"key": "baz",
				},
			},
		},
		{
			name:  "various ints",
			input: `{"int8": 13, "uint8": 254, "int16": -257, "uint16" : 65534, "int32" : -70123, "uint32" : 2147483648, "int64" : -9223372036854775808, "uint64": 18446744073709551615}`,
			expectedOutput: map[string]any{
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
		t.Run(test.name, func(t *testing.T) {
			proc, err := newProcessor("from_json")
			require.NoError(t, err)

			input := service.NewMessage([]byte(test.input))

			msgs, err := proc.Process(context.Background(), input)
			require.NoError(t, err)
			require.Len(t, msgs, 1)

			rawBytes, err := msgs[0].AsBytes()
			require.NoError(t, err)

			var act any
			require.NoError(t, msgpack.Unmarshal(rawBytes, &act))
			assert.Equal(t, test.expectedOutput, act)
		})
	}
}
