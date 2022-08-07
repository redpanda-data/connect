package pure_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestProtobuf(t *testing.T) {
	type testCase struct {
		name       string
		operator   string
		message    string
		importPath string
		input      [][]byte
		output     [][]byte
	}

	tests := []testCase{
		{
			name:       "json to protobuf",
			operator:   "from_json",
			message:    "testing.Person",
			importPath: "../../../config/test/protobuf/schema",
			input: [][]byte{
				[]byte(`{"firstName":"john","lastName":"oates","age":10}`),
				[]byte(`{"firstName":"daryl","lastName":"hall"}`),
				[]byte(`{"firstName":"caleb","lastName":"quaye","email":"caleb@myspace.com"}`),
			},
			output: [][]byte{
				{0x0a, 0x04, 0x6a, 0x6f, 0x68, 0x6e, 0x12, 0x05, 0x6f, 0x61, 0x74, 0x65, 0x73, 0x20, 0x0a},
				{0x0a, 0x05, 0x64, 0x61, 0x72, 0x79, 0x6c, 0x12, 0x04, 0x68, 0x61, 0x6c, 0x6c},
				{
					0x0a, 0x05, 0x63, 0x61, 0x6c, 0x65, 0x62, 0x12, 0x05, 0x71, 0x75, 0x61, 0x79, 0x65, 0x32, 0x11,
					0x63, 0x61, 0x6c, 0x65, 0x62, 0x40, 0x6d, 0x79, 0x73, 0x70, 0x61, 0x63, 0x65, 0x2e, 0x63, 0x6f,
					0x6d,
				},
			},
		},
		{
			name:       "protobuf to json",
			operator:   "to_json",
			message:    "testing.Person",
			importPath: "../../../config/test/protobuf/schema",
			input: [][]byte{
				{0x0a, 0x04, 0x6a, 0x6f, 0x68, 0x6e, 0x12, 0x05, 0x6f, 0x61, 0x74, 0x65, 0x73, 0x20, 0x0a},
				{0x0a, 0x05, 0x64, 0x61, 0x72, 0x79, 0x6c, 0x12, 0x04, 0x68, 0x61, 0x6c, 0x6c},
				{
					0x0a, 0x05, 0x63, 0x61, 0x6c, 0x65, 0x62, 0x12, 0x05, 0x71, 0x75, 0x61, 0x79, 0x65, 0x32, 0x11,
					0x63, 0x61, 0x6c, 0x65, 0x62, 0x40, 0x6d, 0x79, 0x73, 0x70, 0x61, 0x63, 0x65, 0x2e, 0x63, 0x6f,
					0x6d,
				},
			},
			output: [][]byte{
				[]byte(`{"firstName":"john","lastName":"oates","age":10}`),
				[]byte(`{"firstName":"daryl","lastName":"hall"}`),
				[]byte(`{"firstName":"caleb","lastName":"quaye","email":"caleb@myspace.com"}`),
			},
		},
		{
			name:       "any: json to protobuf",
			operator:   "from_json",
			message:    "testing.Envelope",
			importPath: "../../../config/test/protobuf/schema",
			input: [][]byte{
				[]byte(`{"id":747,"content":{"@type":"type.googleapis.com/testing.Person","first_name":"bob"}}`),
				[]byte(`{"id":747,"content":{"@type":"type.googleapis.com/testing.House","address":"123"}}`),
			},
			output: [][]byte{
				{
					0x8, 0xeb, 0x5, 0x12, 0x2b, 0xa, 0x22, 0x74, 0x79, 0x70, 0x65, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c,
					0x65, 0x61, 0x70, 0x69, 0x73, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x74, 0x65, 0x73, 0x74, 0x69, 0x6e,
					0x67, 0x2e, 0x50, 0x65, 0x72, 0x73, 0x6f, 0x6e, 0x12, 0x5, 0xa, 0x3, 0x62, 0x6f, 0x62,
				},
				{
					0x8, 0xeb, 0x5, 0x12, 0x2a, 0xa, 0x21, 0x74, 0x79, 0x70, 0x65, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c,
					0x65, 0x61, 0x70, 0x69, 0x73, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x74, 0x65, 0x73, 0x74, 0x69, 0x6e,
					0x67, 0x2e, 0x48, 0x6f, 0x75, 0x73, 0x65, 0x12, 0x5, 0x12, 0x3, 0x31, 0x32, 0x33,
				},
			},
		},
		{
			name:       "any: protobuf to json",
			operator:   "to_json",
			message:    "testing.Envelope",
			importPath: "../../../config/test/protobuf/schema",
			input: [][]byte{
				{
					0x8, 0xeb, 0x5, 0x12, 0x2b, 0xa, 0x22, 0x74, 0x79, 0x70, 0x65, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c,
					0x65, 0x61, 0x70, 0x69, 0x73, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x74, 0x65, 0x73, 0x74, 0x69, 0x6e,
					0x67, 0x2e, 0x50, 0x65, 0x72, 0x73, 0x6f, 0x6e, 0x12, 0x5, 0xa, 0x3, 0x62, 0x6f, 0x62,
				},
				{
					0x8, 0xeb, 0x5, 0x12, 0x2a, 0xa, 0x21, 0x74, 0x79, 0x70, 0x65, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c,
					0x65, 0x61, 0x70, 0x69, 0x73, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x74, 0x65, 0x73, 0x74, 0x69, 0x6e,
					0x67, 0x2e, 0x48, 0x6f, 0x75, 0x73, 0x65, 0x12, 0x5, 0x12, 0x3, 0x31, 0x32, 0x33,
				},
			},
			output: [][]byte{
				[]byte(`{"id":747,"content":{"@type":"type.googleapis.com/testing.Person","firstName":"bob"}}`),
				[]byte(`{"id":747,"content":{"@type":"type.googleapis.com/testing.House","address":"123"}}`),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			conf := processor.NewConfig()
			conf.Type = "protobuf"
			conf.Protobuf.Operator = test.operator
			conf.Protobuf.Message = test.message
			conf.Protobuf.ImportPaths = []string{test.importPath}

			proc, err := mock.NewManager().NewProcessor(conf)
			require.NoError(t, err)

			input := message.QuickBatch(nil)
			for _, p := range test.input {
				input = append(input, message.NewPart(p))
			}

			msgs, res := proc.ProcessBatch(context.Background(), input)
			require.Nil(t, res)
			require.Len(t, msgs, 1)

			assert.Equal(t, message.GetAllBytes(msgs[0]), test.output)
			_ = msgs[0].Iter(func(i int, part *message.Part) error {
				require.NoError(t, part.ErrorGet())
				return nil
			})
		})
	}
}

func TestProtobufErrors(t *testing.T) {
	type testCase struct {
		name       string
		operator   string
		message    string
		importPath string
		input      [][]byte
		output     []string
	}

	tests := []testCase{
		{
			name:       "json to protobuf",
			operator:   "from_json",
			message:    "testing.Person",
			importPath: "../../../config/test/protobuf/schema",
			input: [][]byte{
				[]byte(`{"firstName":"john","lastName":"oates","ageFoo":10}`),
				[]byte(`not valid json`),
				[]byte(`{"firstName":5,"lastName":"quaye","email":"caleb@myspace.com"}`),
			},
			output: []string{
				`failed to unmarshal JSON message: message type testing.Person has no known field named ageFoo`,
				`failed to unmarshal JSON message: invalid character 'o' in literal null (expecting 'u')`,
				`failed to unmarshal JSON message: bad input: expecting string ; instead got 5`,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			conf := processor.NewConfig()
			conf.Type = "protobuf"
			conf.Protobuf.Operator = test.operator
			conf.Protobuf.Message = test.message
			conf.Protobuf.ImportPaths = []string{test.importPath}

			proc, err := mock.NewManager().NewProcessor(conf)
			require.NoError(t, err)

			input := message.QuickBatch(nil)
			for _, p := range test.input {
				input = append(input, message.NewPart(p))
			}

			msgs, res := proc.ProcessBatch(context.Background(), input)
			require.Nil(t, res)
			require.Len(t, msgs, 1)

			errs := make([]string, msgs[0].Len())
			_ = msgs[0].Iter(func(i int, part *message.Part) error {
				if err := part.ErrorGet(); err != nil {
					errs[i] = err.Error()
				}
				return nil
			})

			assert.Equal(t, test.output, errs)
		})
	}
}
