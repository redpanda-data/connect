package protobuf

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
)

func TestProtobufFromJSON(t *testing.T) {
	type testCase struct {
		name           string
		message        string
		importPath     string
		input          string
		outputContains []string
		discardUnknown bool
	}

	tests := []testCase{
		{
			name:           "json to protobuf age",
			message:        "testing.Person",
			importPath:     "../../../config/test/protobuf/schema",
			input:          `{"firstName":"john","lastName":"oates","age":10}`,
			outputContains: []string{"john"},
		},
		{
			name:           "json to protobuf min",
			message:        "testing.Person",
			importPath:     "../../../config/test/protobuf/schema",
			input:          `{"firstName":"daryl","lastName":"hall"}`,
			outputContains: []string{"daryl"},
		},
		{
			name:           "json to protobuf email",
			message:        "testing.Person",
			importPath:     "../../../config/test/protobuf/schema",
			input:          `{"firstName":"caleb","lastName":"quaye","email":"caleb@myspace.com"}`,
			outputContains: []string{"caleb"},
		},
		{
			name:           "json to protobuf with discard_unknown",
			message:        "testing.Person",
			importPath:     "../../../config/test/protobuf/schema",
			input:          `{"firstName":"caleb","lastName":"quaye","missingfield":"anyvalue"}`,
			outputContains: []string{"caleb"},
			discardUnknown: true,
		},
		{
			name:           "any: json to protobuf 1",
			message:        "testing.Envelope",
			importPath:     "../../../config/test/protobuf/schema",
			input:          `{"id":747,"content":{"@type":"type.googleapis.com/testing.Person","first_name":"bob"}}`,
			outputContains: []string{"type.googleapis.com/testing.Person"},
		},
		{
			name:           "any: json to protobuf 2",
			message:        "testing.Envelope",
			importPath:     "../../../config/test/protobuf/schema",
			input:          `{"id":747,"content":{"@type":"type.googleapis.com/testing.House","address":"123"}}`,
			outputContains: []string{"type.googleapis.com/testing.House"},
		},
		{
			name:           "any: json to protobuf with nested message",
			message:        "testing.House.Mailbox",
			importPath:     "../../../config/test/protobuf/schema",
			input:          `{"color":"red","identifier":"123"}`,
			outputContains: []string{"red"},
		},
	}

	for i, test := range tests {
		t.Run(test.name+"/"+strconv.Itoa(i), func(t *testing.T) {
			conf, err := protobufProcessorSpec().ParseYAML(fmt.Sprintf(`
operator: from_json
message: %v
import_paths: [ %v ]
discard_unknown: %t
`, test.message, test.importPath, test.discardUnknown), nil)
			require.NoError(t, err)

			proc, err := newProtobuf(conf, service.MockResources())
			require.NoError(t, err)

			msgs, res := proc.Process(context.Background(), service.NewMessage([]byte(test.input)))
			require.NoError(t, res)
			require.Len(t, msgs, 1)

			mBytes, err := msgs[0].AsBytes()
			require.NoError(t, err)

			assert.NotEqual(t, test.input, string(mBytes))
			for _, exp := range test.outputContains {
				assert.Contains(t, string(mBytes), exp)
			}
			require.NoError(t, msgs[0].GetError())
		})
	}
}

func TestProtobufToJSON(t *testing.T) {
	type testCase struct {
		name          string
		message       string
		importPath    string
		input         []byte
		output        string
		useProtoNames bool
	}

	tests := []testCase{
		{
			name:       "protobuf to json 1",
			message:    "testing.Person",
			importPath: "../../../config/test/protobuf/schema",
			input:      []byte{0x0a, 0x04, 0x6a, 0x6f, 0x68, 0x6e, 0x12, 0x05, 0x6f, 0x61, 0x74, 0x65, 0x73, 0x20, 0x0a},
			output:     `{"firstName":"john","lastName":"oates","age":10}`,
		},
		{
			name:       "protobuf to json 2",
			message:    "testing.Person",
			importPath: "../../../config/test/protobuf/schema",
			input:      []byte{0x0a, 0x05, 0x64, 0x61, 0x72, 0x79, 0x6c, 0x12, 0x04, 0x68, 0x61, 0x6c, 0x6c},
			output:     `{"firstName":"daryl","lastName":"hall"}`,
		},
		{
			name:       "protobuf to json 3",
			message:    "testing.Person",
			importPath: "../../../config/test/protobuf/schema",
			input: []byte{
				0x0a, 0x05, 0x63, 0x61, 0x6c, 0x65, 0x62, 0x12, 0x05, 0x71, 0x75, 0x61, 0x79, 0x65, 0x32, 0x11,
				0x63, 0x61, 0x6c, 0x65, 0x62, 0x40, 0x6d, 0x79, 0x73, 0x70, 0x61, 0x63, 0x65, 0x2e, 0x63, 0x6f,
				0x6d,
			},
			output: `{"firstName":"caleb","lastName":"quaye","email":"caleb@myspace.com"}`,
		},
		{
			name:          "protobuf to json with use_proto_names",
			message:       "testing.Person",
			importPath:    "../../../config/test/protobuf/schema",
			useProtoNames: true,
			input: []byte{
				0x0a, 0x05, 0x63, 0x61, 0x6c, 0x65, 0x62, 0x12, 0x05, 0x71, 0x75, 0x61, 0x79, 0x65, 0x32, 0x11,
				0x63, 0x61, 0x6c, 0x65, 0x62, 0x40, 0x6d, 0x79, 0x73, 0x70, 0x61, 0x63, 0x65, 0x2e, 0x63, 0x6f,
				0x6d,
			},
			output: `{"first_name":"caleb","last_name":"quaye","email":"caleb@myspace.com"}`,
		},
		{
			name:       "any: protobuf to json 1",
			message:    "testing.Envelope",
			importPath: "../../../config/test/protobuf/schema",
			input: []byte{
				0x8, 0xeb, 0x5, 0x12, 0x2b, 0xa, 0x22, 0x74, 0x79, 0x70, 0x65, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c,
				0x65, 0x61, 0x70, 0x69, 0x73, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x74, 0x65, 0x73, 0x74, 0x69, 0x6e,
				0x67, 0x2e, 0x50, 0x65, 0x72, 0x73, 0x6f, 0x6e, 0x12, 0x5, 0xa, 0x3, 0x62, 0x6f, 0x62,
			},
			output: `{"id":747,"content":{"@type":"type.googleapis.com/testing.Person","firstName":"bob"}}`,
		},
		{
			name:       "any: protobuf to json 2",
			message:    "testing.Envelope",
			importPath: "../../../config/test/protobuf/schema",
			input: []byte{
				0x8, 0xeb, 0x5, 0x12, 0x2a, 0xa, 0x21, 0x74, 0x79, 0x70, 0x65, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c,
				0x65, 0x61, 0x70, 0x69, 0x73, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x74, 0x65, 0x73, 0x74, 0x69, 0x6e,
				0x67, 0x2e, 0x48, 0x6f, 0x75, 0x73, 0x65, 0x12, 0x5, 0x12, 0x3, 0x31, 0x32, 0x33,
			},
			output: `{"id":747,"content":{"@type":"type.googleapis.com/testing.House","address":"123"}}`,
		},
	}

	for i, test := range tests {
		t.Run(test.name+"/"+strconv.Itoa(i), func(t *testing.T) {
			conf, err := protobufProcessorSpec().ParseYAML(fmt.Sprintf(`
operator: to_json
message: %v
import_paths: [ %v ]
use_proto_names: %t
`, test.message, test.importPath, test.useProtoNames), nil)
			require.NoError(t, err)

			proc, err := newProtobuf(conf, service.MockResources())
			require.NoError(t, err)

			msgs, res := proc.Process(context.Background(), service.NewMessage(test.input))
			require.NoError(t, res)
			require.Len(t, msgs, 1)

			mBytes, err := msgs[0].AsBytes()
			require.NoError(t, err)

			assert.JSONEq(t, test.output, string(mBytes))
			require.NoError(t, msgs[0].GetError())
		})
	}
}

func TestProtobufErrors(t *testing.T) {
	type testCase struct {
		name       string
		operator   string
		message    string
		importPath string
		input      string
		output     string
	}

	tests := []testCase{
		{
			name:       "json to protobuf unknown field",
			operator:   "from_json",
			message:    "testing.Person",
			importPath: "../../../config/test/protobuf/schema",
			input:      `{"firstName":"john","lastName":"oates","ageFoo":10}`,
			output:     "unknown field \"ageFoo\"",
		},
		{
			name:       "json to protobuf invalid value",
			operator:   "from_json",
			message:    "testing.Person",
			importPath: "../../../config/test/protobuf/schema",
			input:      `not valid json`,
			output:     "syntax error (line 1:1): invalid value not",
		},
		{
			name:       "json to protobuf invalid string",
			operator:   "from_json",
			message:    "testing.Person",
			importPath: "../../../config/test/protobuf/schema",
			input:      `{"firstName":5,"lastName":"quaye","email":"caleb@myspace.com"}`,
			output:     "invalid value for string type: 5",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			conf, err := protobufProcessorSpec().ParseYAML(fmt.Sprintf(`
operator: %v
message: %v
import_paths: [ %v ]
`, test.operator, test.message, test.importPath), nil)
			require.NoError(t, err)

			proc, err := newProtobuf(conf, service.MockResources())
			require.NoError(t, err)

			_, err = proc.Process(context.Background(), service.NewMessage([]byte(test.input)))
			require.Error(t, err)
			require.Contains(t, err.Error(), test.output)
		})
	}
}
