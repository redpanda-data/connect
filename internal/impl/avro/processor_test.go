package avro

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestAvroBasic(t *testing.T) {
	type testCase struct {
		name     string
		operator string
		encoding string
		input    string
		output   string
	}

	tests := []testCase{
		{
			name:     "textual to json 1",
			operator: "to_json",
			encoding: "textual",
			input:    `{"Name":"foo","Address":{"my.namespace.com.address":{"City":"foo","State":"bar"}}}`,
			output:   `{"Address":{"my.namespace.com.address":{"City":"foo","State":"bar"}},"Name":"foo"}`,
		},
		{
			name:     "binary to json 1",
			operator: "to_json",
			encoding: "binary",
			input:    "\x06foo\x02\x06foo\x06bar",
			output:   `{"Address":{"my.namespace.com.address":{"City":"foo","State":"bar"}},"Name":"foo"}`,
		},
		{
			name:     "json to binary 1",
			operator: "from_json",
			encoding: "binary",
			input:    `{"Name":"foo","Address":{"my.namespace.com.address":{"City":"foo","State":"bar"}}}`,
			output:   "\x06foo\x02\x06foo\x06bar",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			conf, err := avroConfigSpec().ParseYAML(fmt.Sprintf(`
operator: %v
encoding: %v
schema: |
    {
      "namespace": "foo.namespace.com",
      "type": "record",
      "name": "identity",
      "fields": [
        { "name": "Name", "type": "string"},
        { "name": "Address", "type": [ "null", {
          "namespace": "my.namespace.com",
          "type": "record",
          "name": "address",
          "fields": [
            { "name": "City", "type": "string" },
            { "name": "State", "type": "string" }
          ]
        } ], "default": null }
      ]
    }
`, test.operator, test.encoding), nil)
			require.NoError(t, err)

			proc, err := newAvroFromConfig(conf, service.MockResources())
			require.NoError(t, err)

			msgs, err := proc.Process(context.Background(), service.NewMessage([]byte(test.input)))
			require.NoError(t, err)
			require.Len(t, msgs, 1)

			mBytes, err := msgs[0].AsBytes()
			require.NoError(t, err)

			assert.Equal(t, test.output, string(mBytes))
		})
	}
}

func TestAvroSchemaPath(t *testing.T) {
	schema := `{
	"namespace": "foo.namespace.com",
	"type":	"record",
	"name": "identity",
	"fields": [
		{ "name": "Name", "type": "string"},
		{ "name": "Address", "type": ["null",{
			"namespace": "my.namespace.com",
			"type":	"record",
			"name": "address",
			"fields": [
				{ "name": "City", "type": "string" },
				{ "name": "State", "type": "string" }
			]
		}],"default":null}
	]
}`

	tmpSchemaFile, err := os.CreateTemp("", "benthos_avro_test")
	require.NoError(t, err)

	defer os.Remove(tmpSchemaFile.Name())

	// write schema definition to tmpfile
	_, err = tmpSchemaFile.WriteString(schema)
	require.NoError(t, err)

	type testCase struct {
		name     string
		operator string
		encoding string
		input    string
		output   string
	}

	tests := []testCase{
		{
			name:     "textual to json 1",
			operator: "to_json",
			encoding: "textual",
			input:    `{"Name":"foo","Address":{"my.namespace.com.address":{"City":"foo","State":"bar"}}}`,
			output:   `{"Address":{"my.namespace.com.address":{"City":"foo","State":"bar"}},"Name":"foo"}`,
		},
		{
			name:     "binary to json 1",
			operator: "to_json",
			encoding: "binary",
			input:    "\x06foo\x02\x06foo\x06bar",
			output:   `{"Address":{"my.namespace.com.address":{"City":"foo","State":"bar"}},"Name":"foo"}`,
		},
		{
			name:     "json to binary 1",
			operator: "from_json",
			encoding: "binary",
			input:    `{"Name":"foo","Address":{"my.namespace.com.address":{"City":"foo","State":"bar"}}}`,
			output:   "\x06foo\x02\x06foo\x06bar",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			conf, err := avroConfigSpec().ParseYAML(fmt.Sprintf(`
operator: %v
encoding: %v
schema_path: %v
`, test.operator, test.encoding, fmt.Sprintf("file://%s", tmpSchemaFile.Name())), nil)
			require.NoError(t, err)

			proc, err := newAvroFromConfig(conf, service.MockResources())
			require.NoError(t, err)

			msgs, err := proc.Process(context.Background(), service.NewMessage([]byte(test.input)))
			require.NoError(t, err)
			require.Len(t, msgs, 1)

			mBytes, err := msgs[0].AsBytes()
			require.NoError(t, err)

			assert.Equal(t, test.output, string(mBytes))
		})
	}
}

func TestAvroSchemaPathNotExist(t *testing.T) {
	_, err := avroConfigSpec().ParseYAML(`
schema_path: "file://path_does_not_exist"
`, nil)
	require.Error(t, err)
}
