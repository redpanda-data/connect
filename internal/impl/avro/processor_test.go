package avro_test

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestAvroBasic(t *testing.T) {
	type testCase struct {
		name     string
		operator string
		encoding string
		input    []string
		output   []string
	}

	tests := []testCase{
		{
			name:     "textual to json 1",
			operator: "to_json",
			encoding: "textual",
			input: []string{
				`{"Name":"foo","Address":{"my.namespace.com.address":{"City":"foo","State":"bar"}}}`,
			},
			output: []string{
				`{"Address":{"my.namespace.com.address":{"City":"foo","State":"bar"}},"Name":"foo"}`,
			},
		},
		{
			name:     "binary to json 1",
			operator: "to_json",
			encoding: "binary",
			input: []string{
				"\x06foo\x02\x06foo\x06bar",
			},
			output: []string{
				`{"Address":{"my.namespace.com.address":{"City":"foo","State":"bar"}},"Name":"foo"}`,
			},
		},
		/*
			{
				name:     "single to json 1",
				operator: "to_json",
				encoding: "single",
				input: []string{
					"\xc3\x01\x84>\xe0\xee\xbb\xf1ǋ\x06foo\x02\x06foo\x06bar",
				},
				output: []string{
					`{"Address":{"my.namespace.com.address":{"City":"foo","State":"bar"}},"Name":"foo"}`,
				},
			},
		*/
		/*
			// TODO: Unfortunately this serialisation is non-deterministic
			{
				name:     "json to textual 1",
				operator: "from_json",
				encoding: "textual",
				input: []string{
					`{"Name":"foo","Address":{"my.namespace.com.address":{"City":"foo","State":"bar"}}}`,
				},
				output: []string{
					`{"Name":"foo","Address":{"my.namespace.com.address":{"City":"foo","State":"bar"}}}`,
				},
			},
		*/
		{
			name:     "json to binary 1",
			operator: "from_json",
			encoding: "binary",
			input: []string{
				`{"Name":"foo","Address":{"my.namespace.com.address":{"City":"foo","State":"bar"}}}`,
			},
			output: []string{
				"\x06foo\x02\x06foo\x06bar",
			},
		},
		/*
			{
				name:     "json to single 1",
				operator: "from_json",
				encoding: "single",
				input: []string{
					`{"Name":"foo","Address":{"my.namespace.com.address":{"City":"foo","State":"bar"}}}`,
				},
				output: []string{
					"\xc3\x01\x84>\xe0\xee\xbb\xf1ǋ\x06foo\x02\x06foo\x06bar",
				},
			},
		*/
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			conf := processor.NewConfig()
			require.NoError(t, yaml.Unmarshal(fmt.Appendf(nil, `
avro:
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
`, test.operator, test.encoding), &conf))

			proc, err := mock.NewManager().NewProcessor(conf)
			if err != nil {
				tt.Fatal(err)
			}

			input := message.QuickBatch(nil)
			for _, p := range test.input {
				input = append(input, message.NewPart([]byte(p)))
			}

			exp := make([][]byte, len(test.output))
			for i, p := range test.output {
				exp[i] = []byte(p)
			}

			msgs, res := proc.ProcessBatch(context.Background(), input)
			if res != nil {
				tt.Fatal(res)
			}

			if len(msgs) != 1 {
				tt.Fatalf("Expected one message, received: %v", len(msgs))
			}
			if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(act, exp) {
				tt.Errorf("Unexpected output: %s != %s", exp, act)
				tt.Logf("Part 0: %v", strconv.Quote(string(act[0])))
			}
			_ = msgs[0].Iter(func(i int, part *message.Part) error {
				assert.NoError(t, part.ErrorGet())
				return nil
			})
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
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpSchemaFile.Name())

	// write schema definition to tmpfile
	if _, err := tmpSchemaFile.WriteString(schema); err != nil {
		t.Fatal(err)
	}

	type testCase struct {
		name     string
		operator string
		encoding string
		input    []string
		output   []string
	}

	tests := []testCase{
		{
			name:     "textual to json 1",
			operator: "to_json",
			encoding: "textual",
			input: []string{
				`{"Name":"foo","Address":{"my.namespace.com.address":{"City":"foo","State":"bar"}}}`,
			},
			output: []string{
				`{"Address":{"my.namespace.com.address":{"City":"foo","State":"bar"}},"Name":"foo"}`,
			},
		},
		{
			name:     "binary to json 1",
			operator: "to_json",
			encoding: "binary",
			input: []string{
				"\x06foo\x02\x06foo\x06bar",
			},
			output: []string{
				`{"Address":{"my.namespace.com.address":{"City":"foo","State":"bar"}},"Name":"foo"}`,
			},
		},
		/*
			{
				name:     "single to json 1",
				operator: "to_json",
				encoding: "single",
				input: []string{
					"\xc3\x01\x84>\xe0\xee\xbb\xf1ǋ\x06foo\x02\x06foo\x06bar",
				},
				output: []string{
					`{"Address":{"my.namespace.com.address":{"City":"foo","State":"bar"}},"Name":"foo"}`,
				},
			},
		*/
		/*
			// TODO: Unfortunately this serialisation is non-deterministic
			{
				name:     "json to textual 1",
				operator: "from_json",
				encoding: "textual",
				input: []string{
					`{"Name":"foo","Address":{"my.namespace.com.address":{"City":"foo","State":"bar"}}}`,
				},
				output: []string{
					`{"Name":"foo","Address":{"my.namespace.com.address":{"City":"foo","State":"bar"}}}`,
				},
			},
		*/
		{
			name:     "json to binary 1",
			operator: "from_json",
			encoding: "binary",
			input: []string{
				`{"Name":"foo","Address":{"my.namespace.com.address":{"City":"foo","State":"bar"}}}`,
			},
			output: []string{
				"\x06foo\x02\x06foo\x06bar",
			},
		},
		/*
			{
				name:     "json to single 1",
				operator: "from_json",
				encoding: "single",
				input: []string{
					`{"Name":"foo","Address":{"my.namespace.com.address":{"City":"foo","State":"bar"}}}`,
				},
				output: []string{
					"\xc3\x01\x84>\xe0\xee\xbb\xf1ǋ\x06foo\x02\x06foo\x06bar",
				},
			},
		*/
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			conf := processor.NewConfig()
			require.NoError(t, yaml.Unmarshal(fmt.Appendf(nil, `
avro:
  operator: %v
  encoding: %v
  schema_path: %v
`, test.operator, test.encoding, fmt.Sprintf("file://%s", tmpSchemaFile.Name())), &conf))

			proc, err := mock.NewManager().NewProcessor(conf)
			if err != nil {
				tt.Fatal(err)
			}

			input := message.QuickBatch(nil)
			for _, p := range test.input {
				input = append(input, message.NewPart([]byte(p)))
			}

			exp := make([][]byte, len(test.output))
			for i, p := range test.output {
				exp[i] = []byte(p)
			}

			msgs, res := proc.ProcessBatch(context.Background(), input)
			if res != nil {
				tt.Fatal(res)
			}

			if len(msgs) != 1 {
				tt.Fatalf("Expected one message, received: %v", len(msgs))
			}
			if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(act, exp) {
				tt.Errorf("Unexpected output: %s != %s", exp, act)
				tt.Logf("Part 0: %v", strconv.Quote(string(act[0])))
			}
			_ = msgs[0].Iter(func(i int, part *message.Part) error {
				assert.NoError(t, part.ErrorGet())
				return nil
			})
		})
	}
}

func TestAvroSchemaPathNotExist(t *testing.T) {
	conf := processor.NewConfig()
	require.NoError(t, yaml.Unmarshal([]byte(`
avro:
  schema_path: "file://path_does_not_exist"
`), &conf))

	_, err := mock.NewManager().NewProcessor(conf)
	if err == nil {
		t.Error("expected error from loading non existent schema file")
	}
}
