package docs_test

import (
	"testing"

	"github.com/Jeffail/gabs/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/docs"
)

var configSpec = docs.FieldSpecs{
	docs.FieldInput("input", ""),
	docs.FieldBuffer("buffer", ""),
	docs.FieldObject("pipeline", "").WithChildren(
		docs.FieldProcessor("processors", "").Array(),
	),
	docs.FieldOutput("output", ""),
	docs.FieldCache("cache_resources", "").Array(),
}

func TestSetYAMLPath(t *testing.T) {
	mockProv := docs.NewMappedDocsProvider()
	mockProv.RegisterDocs(docs.ComponentSpec{
		Name: "kafka",
		Type: docs.TypeInput,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("addresses", "").Array(),
			docs.FieldString("topics", "").Array(),
		),
	})
	mockProv.RegisterDocs(docs.ComponentSpec{
		Name: "generate",
		Type: docs.TypeInput,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("mapping", ""),
		),
	})
	mockProv.RegisterDocs(docs.ComponentSpec{
		Name: "dynamic",
		Type: docs.TypeInput,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldInput("inputs", "").Map(),
		),
	})
	mockProv.RegisterDocs(docs.ComponentSpec{
		Name: "nats",
		Type: docs.TypeOutput,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("urls", "").Array(),
			docs.FieldString("subject", ""),
			docs.FieldInt("max_in_flight", ""),
		),
	})
	mockProv.RegisterDocs(docs.ComponentSpec{
		Name: "compress",
		Type: docs.TypeProcessor,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("algorithm", ""),
		),
	})
	mockProv.RegisterDocs(docs.ComponentSpec{
		Name: "workflow",
		Type: docs.TypeProcessor,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("order", "").ArrayOfArrays(),
		),
	})
	mockProv.RegisterDocs(docs.ComponentSpec{
		Name: "switch",
		Type: docs.TypeProcessor,
		Config: docs.FieldComponent().Array().WithChildren(
			docs.FieldString("check", ""),
			docs.FieldProcessor("processors", "").Array(),
		),
	})

	tests := []struct {
		name        string
		input       string
		path        string
		value       string
		output      string
		errContains string
	}{
		{
			name: "set input",
			input: `
input:
  kafka:
    addresses: [ "foo", "bar" ]
    topics: [ "baz" ]

output:
  nats:
    urls: [ nats://127.0.0.1:4222 ]
    subject: benthos_messages
    max_in_flight: 1
`,
			path: "/input",
			value: `
generate:
  mapping: 'root = {"foo":"bar"}'`,
			output: `
input:
  generate:
    mapping: 'root = {"foo":"bar"}'
output:
  nats:
    urls: [ nats://127.0.0.1:4222 ]
    subject: benthos_messages
    max_in_flight: 1
`,
		},
		{
			name: "set input addresses total",
			input: `
input:
  kafka:
    addresses: [ "foo", "bar" ]
    topics: [ "baz" ]
`,
			path:  "/input/kafka/addresses",
			value: `"foobar"`,
			output: `
input:
  kafka:
    addresses: [ "foobar" ]
    topics: [ "baz" ]
`,
		},
		{
			name: "set mapping value",
			input: `
input:
  kafka:
    addresses: [ "foo", "bar" ]
    topics: [ "baz" ]
`,
			path:  "/input/dynamic/inputs/foo/type",
			value: `"foobar"`,
			output: `
input:
  dynamic:
    inputs:
      foo:
        type: "foobar"
  kafka:
    addresses: [ "foo", "bar" ]
    topics: [ "baz" ]
`,
		},
		{
			name:  "set value to object",
			input: `input: "hello world"`,
			path:  "/input/kafka/addresses",
			value: `"foobar"`,
			output: `
input:
  kafka:
    addresses: ["foobar"]
`,
		},
		{
			name: "set array index",
			input: `
input:
  kafka:
    addresses: [ "foo", "bar" ]
    topics: [ "baz" ]
`,
			path:  "/input/kafka/addresses/0",
			value: `"baz"`,
			output: `
input:
  kafka:
    addresses: [ "baz", "bar" ]
    topics: [ "baz" ]
`,
		},
		{
			name: "set array index child",
			input: `
input:
  kafka:
    addresses: [ "foo", "bar" ]
    topics: [ "baz" ]
  processors:
    - compress:
        algorithm: gzip
`,
			path:  "/input/processors/0/compress/algorithm",
			value: `"baz"`,
			output: `
input:
  kafka:
    addresses: [ "foo", "bar" ]
    topics: [ "baz" ]
  processors:
    - compress:
        algorithm: baz
`,
		},
		{
			name: "set array append",
			input: `
input:
  kafka:
    addresses: [ "foo", "bar" ]
    topics: [ "baz" ]
`,
			path:  "/input/kafka/addresses/-",
			value: `"baz"`,
			output: `
input:
  kafka:
    addresses: [ "foo", "bar", "baz" ]
    topics: [ "baz" ]
`,
		},
		{
			name: "set array NaN",
			input: `
input:
  kafka:
    addresses: [ "foo", "bar" ]
`,
			path:        "/input/kafka/addresses/nope",
			value:       `"baz"`,
			errContains: "input.kafka.addresses.nope: failed to parse path segment as array index",
		},
		{
			name: "set array big index",
			input: `
input:
  kafka:
    addresses: [ "foo", "bar" ]
`,
			path:        "/input/kafka/addresses/2",
			value:       `"baz"`,
			errContains: "input.kafka.addresses.2: target index greater than",
		},
		{
			name: "set nested array big index",
			input: `
input:
  kafka:
    addresses: [ [ "foo", "bar" ] ]
`,
			path:        "/input/kafka/addresses/0/2",
			value:       `"baz"`,
			errContains: "input.kafka.addresses.0.2: field not recognised",
		},
		{
			name: "set 2D array value abs",
			input: `
pipeline:
  processors:
    - workflow:
        order: []
`,
			path:  "/pipeline/processors/0/workflow/order",
			value: `"baz"`,
			output: `
pipeline:
  processors:
    - workflow:
        order: [["baz"]]
`,
		},
		{
			name: "set 2D array value outer index",
			input: `
pipeline:
  processors:
    - workflow:
        order: []
`,
			path:  "/pipeline/processors/0/workflow/order/-",
			value: `"baz"`,
			output: `
pipeline:
  processors:
    - workflow:
        order: [["baz"]]
`,
		},
		{
			name: "set 2D array value inner index",
			input: `
pipeline:
  processors:
    - workflow:
        order: []
`,
			path:  "/pipeline/processors/0/workflow/order/-/-",
			value: `"baz"`,
			output: `
pipeline:
  processors:
    - workflow:
        order: [["baz"]]
`,
		},
		{
			name: "delete field",
			input: `
input:
  kafka:
    addresses: [ "foo", "bar" ]
    topics: [ "baz" ]
`,
			path: "/input/kafka/addresses",
			output: `
input:
  kafka:
    topics: [ "baz" ]
`,
		},
		{
			name: "delete element",
			input: `
input:
  kafka:
    addresses: [ "foo", "bar" ]
    topics: [ "baz" ]
`,
			path: "/input/kafka/addresses/1",
			output: `
input:
  kafka:
    addresses: [ "foo" ]
    topics: [ "baz" ]
`,
		},
		{
			name: "delete input",
			input: `
input:
  kafka:
    addresses: [ "foo", "bar" ]
    topics: [ "baz" ]
`,
			path: "/input/kafka",
			output: `
input: {}
`,
		},
		{
			name: "set switch case check",
			input: `
pipeline:
  processors:
  - switch:
      - check: 'root = "foobar"'
        processors:
          - compress:
              algorithm: meow1
      - check: 'root = "foobar2"'
        processors:
          - compress:
              algorithm: meow2
          - compress:
              algorithm: meow3
`,
			path:  "/pipeline/processors/0/switch/0/check",
			value: "foobar3",
			output: `
pipeline:
  processors:
  - switch:
      - check: 'foobar3'
        processors:
          - compress:
              algorithm: meow1
      - check: 'root = "foobar2"'
        processors:
          - compress:
              algorithm: meow2
          - compress:
              algorithm: meow3
`,
		},
		{
			name: "set switch case processors",
			input: `
pipeline:
  processors:
  - switch:
      - check: 'root = "foobar"'
        processors:
          - compress:
              algorithm: meow1
`,
			path: "/pipeline/processors/0/switch/0/processors",
			value: `
- compress:
    algorithm: meow2
- compress:
    algorithm: meow3
`,
			output: `
pipeline:
  processors:
  - switch:
      - check: 'root = "foobar"'
        processors:
          - compress:
              algorithm: meow2
          - compress:
              algorithm: meow3
`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input, value := &yaml.Node{}, &yaml.Node{}

			require.NoError(t, yaml.Unmarshal([]byte(test.input), input))
			if test.value != "" {
				require.NoError(t, yaml.Unmarshal([]byte(test.value), value))
			} else {
				value = nil
			}

			path, err := gabs.JSONPointerToSlice(test.path)
			require.NoError(t, err)

			err = configSpec.SetYAMLPath(mockProv, input, value, path...)
			if len(test.errContains) > 0 {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			} else {
				require.NoError(t, err)

				var iinput, ioutput any
				require.NoError(t, input.Decode(&iinput))
				require.NoError(t, yaml.Unmarshal([]byte(test.output), &ioutput))
				assert.Equal(t, ioutput, iinput)
			}
		})
	}
}

func TestGetPathDocs(t *testing.T) {
	mockProv := docs.NewMappedDocsProvider()
	mockProv.RegisterDocs(docs.ComponentSpec{
		Name: "kafka",
		Type: docs.TypeInput,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("addresses", "").Array(),
			docs.FieldString("topics", "").Array(),
		),
	})
	mockProv.RegisterDocs(docs.ComponentSpec{
		Name: "dynamic",
		Type: docs.TypeInput,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldInput("inputs", "").Map(),
		),
	})
	mockProv.RegisterDocs(docs.ComponentSpec{
		Name: "workflow",
		Type: docs.TypeProcessor,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("order", "").ArrayOfArrays(),
		),
	})
	mockProv.RegisterDocs(docs.ComponentSpec{
		Name:   "try",
		Type:   docs.TypeProcessor,
		Config: docs.FieldComponent().Array().HasType(docs.FieldTypeProcessor),
	})

	tests := []struct {
		name        string
		path        string
		resName     string
		resType     string
		resKind     string
		resChildren int
		errContains string
	}{
		{
			name:    "root input",
			path:    "/input",
			resName: "input",
			resType: "input",
			resKind: "scalar",
		},
		{
			name:        "kafka input",
			path:        "/input/kafka",
			resName:     "",
			resType:     "object",
			resKind:     "scalar",
			resChildren: 2,
		},
		{
			name:    "kafka input addresses",
			path:    "/input/kafka/addresses",
			resName: "addresses",
			resType: "string",
			resKind: "array",
		},
		{
			name:    "dynamic input inputs",
			path:    "/input/dynamic/inputs",
			resName: "inputs",
			resType: "input",
			resKind: "map",
		},
		{
			name:    "dynamic input named child",
			path:    "/input/dynamic/inputs/foo",
			resName: "inputs",
			resType: "input",
			resKind: "scalar",
		},
		{
			name:    "workflow 2D array outer",
			path:    "/pipeline/processors/0/workflow/order",
			resName: "order",
			resType: "string",
			resKind: "2darray",
		},
		{
			name:    "workflow 2D array inner",
			path:    "/pipeline/processors/0/workflow/order/1",
			resName: "order",
			resType: "string",
			resKind: "array",
		},
		{
			name:    "workflow 2D array element",
			path:    "/pipeline/processors/0/workflow/order/1/3",
			resName: "order",
			resType: "string",
			resKind: "scalar",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path, err := gabs.JSONPointerToSlice(test.path)
			require.NoError(t, err)

			docs, err := configSpec.GetDocsForPath(mockProv, path...)
			if len(test.errContains) > 0 {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			} else {
				require.NoError(t, err)

				assert.Equal(t, test.resName, docs.Name)
				assert.Equal(t, test.resType, string(docs.Type))
				assert.Equal(t, test.resKind, string(docs.Kind))
				assert.Equal(t, test.resChildren, len(docs.Children))
			}
		})
	}
}

func TestGetYAMLPath(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		path        string
		output      string
		errContains string
	}{
		{
			name: "all of input",
			input: `
input:
  kafka:
    addresses: [ "foo" ]
`,
			path: "/input",
			output: `
kafka:
  addresses: [ "foo" ]
`,
		},
		{
			name: "first address of input",
			input: `
input:
  kafka:
    addresses: [ "foo" ]
`,
			path:   "/input/kafka/addresses/0",
			output: `"foo"`,
		},
		{
			name: "unknown field",
			input: `
input:
  kafka:
    addresses: [ "foo" ]
`,
			path:        "/input/meow",
			errContains: "input.meow: key not found in mapping",
		},
		{
			name: "bad index",
			input: `
input:
  kafka:
    addresses: [ "foo" ]
`,
			path:        "/input/kafka/addresses/10",
			errContains: "input.kafka.addresses.10: target index greater",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var input yaml.Node
			require.NoError(t, yaml.Unmarshal([]byte(test.input), &input))

			path, err := gabs.JSONPointerToSlice(test.path)
			require.NoError(t, err)

			output, err := docs.GetYAMLPath(&input, path...)
			if len(test.errContains) > 0 {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			} else {
				require.NoError(t, err)

				var expected, actual any
				require.NoError(t, output.Decode(&actual))
				require.NoError(t, yaml.Unmarshal([]byte(test.output), &expected))
				assert.Equal(t, expected, actual)
			}
		})
	}
}

func TestYAMLLabelsToPath(t *testing.T) {
	mockProv := docs.NewMappedDocsProvider()
	mockProv.RegisterDocs(docs.ComponentSpec{
		Name: "kafka",
		Type: docs.TypeInput,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("addresses", "").Array(),
			docs.FieldString("topics", "").Array(),
		),
	})
	mockProv.RegisterDocs(docs.ComponentSpec{
		Name: "dynamic",
		Type: docs.TypeInput,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldInput("inputs", "").Map(),
		),
	})
	mockProv.RegisterDocs(docs.ComponentSpec{
		Name: "nats",
		Type: docs.TypeOutput,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("urls", "").Array(),
			docs.FieldString("subject", ""),
			docs.FieldInt("max_in_flight", ""),
		),
	})
	mockProv.RegisterDocs(docs.ComponentSpec{
		Name: "compress",
		Type: docs.TypeProcessor,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("algorithm", ""),
		),
	})
	mockProv.RegisterDocs(docs.ComponentSpec{
		Name: "for_each",
		Type: docs.TypeProcessor,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldProcessor("things", "").Array(),
		),
	})
	mockProv.RegisterDocs(docs.ComponentSpec{
		Name: "mega_for_each",
		Type: docs.TypeProcessor,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldProcessor("things", "").ArrayOfArrays(),
		),
	})
	mockProv.RegisterDocs(docs.ComponentSpec{
		Name: "workflow",
		Type: docs.TypeProcessor,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldProcessor("things", "").Map(),
		),
	})

	tests := []struct {
		name   string
		input  string
		output map[string][]string
	}{
		{
			name: "no labels",
			input: `
input:
  kafka:
    addresses: [ "foo", "bar" ]
    topics: [ "baz" ]

output:
  nats:
    urls: [ nats://127.0.0.1:4222 ]
    subject: benthos_messages
    max_in_flight: 1
`,
			output: map[string][]string{},
		},
		{
			name: "basic components all with labels",
			input: `
input:
  label: fooinput
  kafka:
    addresses: [ "foo", "bar" ]
    topics: [ "baz" ]

pipeline:
  processors:
    - label: fooproc1
      compress:
        algorithm: nahm8

output:
  label: foooutput
  nats:
    urls: [ nats://127.0.0.1:4222 ]
    subject: benthos_messages
    max_in_flight: 1
`,
			output: map[string][]string{
				"fooinput":  {"input"},
				"fooproc1":  {"pipeline", "processors", "0"},
				"foooutput": {"output"},
			},
		},
		{
			name: "Array of procs",
			input: `
pipeline:
  processors:
    - label: fooproc1
      for_each:
        things:
        - label: fooproc2
          compress:
            algorithm: nahm8
        - label: fooproc3
          compress:
            algorithm: nahm8
`,
			output: map[string][]string{
				"fooproc1": {"pipeline", "processors", "0"},
				"fooproc2": {"pipeline", "processors", "0", "for_each", "things", "0"},
				"fooproc3": {"pipeline", "processors", "0", "for_each", "things", "1"},
			},
		},
		{
			name: "array of array of procs",
			input: `
pipeline:
  processors:
    - label: fooproc1
      mega_for_each:
        things:
        -
          - label: fooproc2
            compress:
              algorithm: nahm8
          - label: fooproc3
            compress:
              algorithm: nahm8
        -
          - label: fooproc4
            compress:
              algorithm: nahm8
`,
			output: map[string][]string{
				"fooproc1": {"pipeline", "processors", "0"},
				"fooproc2": {"pipeline", "processors", "0", "mega_for_each", "things", "0", "0"},
				"fooproc3": {"pipeline", "processors", "0", "mega_for_each", "things", "0", "1"},
				"fooproc4": {"pipeline", "processors", "0", "mega_for_each", "things", "1", "0"},
			},
		},
		{
			name: "map of procs",
			input: `
pipeline:
  processors:
    - label: fooproc1
      workflow:
        things:
          first:
            label: fooproc2
            compress:
              algorithm: nahm8
          second:
            label: fooproc3
            compress:
              algorithm: nahm8
          third:
            label: fooproc4
            compress:
              algorithm: nahm8
`,
			output: map[string][]string{
				"fooproc1": {"pipeline", "processors", "0"},
				"fooproc2": {"pipeline", "processors", "0", "workflow", "things", "first"},
				"fooproc3": {"pipeline", "processors", "0", "workflow", "things", "second"},
				"fooproc4": {"pipeline", "processors", "0", "workflow", "things", "third"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var input yaml.Node
			require.NoError(t, yaml.Unmarshal([]byte(test.input), &input))

			paths := map[string][]string{}

			configSpec.YAMLLabelsToPaths(mockProv, &input, paths, nil)
			assert.Equal(t, test.output, paths)
		})
	}
}
