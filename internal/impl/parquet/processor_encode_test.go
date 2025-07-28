// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package parquet

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/schema"
	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestParquetEncodePanic(t *testing.T) {
	encodeConf, err := parquetEncodeProcessorConfig().ParseYAML(`
schema:
  - { name: id, type: FLOAT }
  - { name: name, type: UTF8 }
`, nil)
	require.NoError(t, err)

	encodeProc, err := newParquetEncodeProcessorFromConfig(encodeConf, nil)
	require.NoError(t, err)

	tctx := t.Context()
	_, err = encodeProc.ProcessBatch(tctx, service.MessageBatch{
		service.NewMessage([]byte(`{"id":12,"name":"foo"}`)),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot create parquet value of type FLOAT from go value of type int64")
}

func TestParquetEncodeDecodeRoundTrip(t *testing.T) {
	encodeConf, err := parquetEncodeProcessorConfig().ParseYAML(`
schema:
  - { name: id, type: INT64 }
  - { name: as, type: DOUBLE, repeated: true }
  - { name: b, type: BYTE_ARRAY }
  - { name: c, type: DOUBLE }
  - { name: d, type: BOOLEAN }
  - { name: e, type: INT64, optional: true }
  - { name: f, type: INT64 }
  - { name: g, type: UTF8 }
  - { name: ts, type: TIMESTAMP, optional: true }
  - { name: bson, type: BSON, optional: true }
  - { name: enum, type: ENUM, optional: true }
  - { name: uuid, type: UUID, optional: true }
  - { name: json, type: JSON, optional: true }
  - name: nested_stuff
    optional: true
    fields:
      - { name: a_stuff, type: BYTE_ARRAY }
      - { name: b_stuff, type: BYTE_ARRAY }
`, nil)
	require.NoError(t, err)

	encodeProc, err := newParquetEncodeProcessorFromConfig(encodeConf, nil)
	require.NoError(t, err)

	decodeConf, err := parquetDecodeProcessorConfig().ParseYAML(`
byte_array_as_string: true
handle_logical_types: v2
`, nil)
	require.NoError(t, err)

	decodeProc, err := newParquetDecodeProcessorFromConfig(decodeConf, nil)
	require.NoError(t, err)

	testParquetEncodeDecodeRoundTrip(t, encodeProc, decodeProc)
}

func TestParquetEncodeDecodeRoundTripPlainEncoding(t *testing.T) {
	encodeConf, err := parquetEncodeProcessorConfig().ParseYAML(`
default_encoding: PLAIN
schema:
  - { name: id, type: INT64 }
  - { name: as, type: DOUBLE, repeated: true }
  - { name: b, type: BYTE_ARRAY }
  - { name: c, type: DOUBLE }
  - { name: d, type: BOOLEAN }
  - { name: e, type: INT64, optional: true }
  - { name: f, type: INT64 }
  - { name: g, type: UTF8 }
  - { name: ts, type: TIMESTAMP, optional: true }
  - { name: bson, type: BSON, optional: true }
  - { name: enum, type: ENUM, optional: true }
  - { name: uuid, type: UUID, optional: true }
  - { name: json, type: JSON, optional: true }
  - name: nested_stuff
    optional: true
    fields:
      - { name: a_stuff, type: BYTE_ARRAY }
      - { name: b_stuff, type: BYTE_ARRAY }
`, nil)
	require.NoError(t, err)

	encodeProc, err := newParquetEncodeProcessorFromConfig(encodeConf, nil)
	require.NoError(t, err)

	decodeConf, err := parquetDecodeProcessorConfig().ParseYAML(`
byte_array_as_string: true
handle_logical_types: v2
`, nil)
	require.NoError(t, err)

	decodeProc, err := newParquetDecodeProcessorFromConfig(decodeConf, nil)
	require.NoError(t, err)

	testParquetEncodeDecodeRoundTrip(t, encodeProc, decodeProc)
}

func testParquetEncodeDecodeRoundTrip(t *testing.T, encodeProc *parquetEncodeProcessor, decodeProc *parquetDecodeProcessor) {
	tctx := t.Context()

	for _, test := range []struct {
		name      string
		input     string
		encodeErr string
		output    string
		decodeErr string
	}{
		{
			name: "basic values",
			input: `{
  "id": 3,
  "as": [ 0.1, 0.2, 0.3, 0.4 ],
  "b": "hello world basic values",
  "c": 0.5,
  "d": true,
  "e": 6,
  "f": 7,
  "g": "logical string represent",
  "ts": "1996-12-19T16:39:57Z",
  "bson": "bson-data",
  "enum": "enum",
  "uuid": "4a701342-4e27-4d08-bef9-e2f74fb79418",
  "json": {"foo":" bar"},
  "nested_stuff": {
    "a_stuff": "a value",
    "b_stuff": "b value"
  },
  "canary":"not in schema"
}`,
			output: `{
  "id": 3,
  "as": [ 0.1, 0.2, 0.3, 0.4 ],
  "b": "hello world basic values",
  "c": 0.5,
  "d": true,
  "e": 6,
  "f": 7,
  "g": "logical string represent",
  "ts": "1996-12-19T16:39:57Z",
  "bson": "bson-data",
  "enum": "enum",
  "uuid": "4a701342-4e27-4d08-bef9-e2f74fb79418",
  "json": {"foo":" bar"},
  "nested_stuff": {
    "a_stuff": "a value",
    "b_stuff": "b value"
  }
}`,
		},
		{
			name: "miss all optionals",
			input: `{
  "id": 3,
  "b": "hello world basic values",
  "c": 0.5,
  "d": true,
  "f": 7,
  "g": "logical string represent",
  "canary":"not in schema"
}`,
			output: `{
  "id": 3,
  "as": [],
  "b": "hello world basic values",
  "c": 0.5,
  "d": true,
  "e": null,
  "f": 7,
  "g": "logical string represent",
  "ts": null,
  "bson": null,
  "enum": null,
  "uuid": null,
  "json": null,
  "nested_stuff": null
}`,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			inBatch := service.MessageBatch{
				service.NewMessage([]byte(test.input)),
			}

			encodedBatches, err := encodeProc.ProcessBatch(tctx, inBatch)
			if test.encodeErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.encodeErr)
				return
			}
			require.NoError(t, err)
			require.Len(t, encodedBatches, 1)
			require.Len(t, encodedBatches[0], 1)

			encodedBytes, err := encodedBatches[0][0].AsBytes()
			require.NoError(t, err)

			decodedBatch, err := decodeProc.Process(tctx, service.NewMessage(encodedBytes))
			if test.encodeErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.encodeErr)
				return
			}
			require.NoError(t, err)
			require.Len(t, decodedBatch, 1)

			decodedBytes, err := decodedBatch[0].AsBytes()
			require.NoError(t, err)

			assert.JSONEq(t, test.output, string(decodedBytes))
		})
	}
}

func TestParquetEncodeEmptyBatch(t *testing.T) {
	tctx := t.Context()

	encodeConf, err := parquetEncodeProcessorConfig().ParseYAML(`
default_encoding: PLAIN
schema:
  - { name: id, type: INT64 }
`, nil)
	require.NoError(t, err)

	encodeProc, err := newParquetEncodeProcessorFromConfig(encodeConf, nil)
	require.NoError(t, err)

	inBatch := service.MessageBatch{}
	_, err = encodeProc.ProcessBatch(tctx, inBatch)
	require.NoError(t, err)
}

func TestParquetEncodeProcessor(t *testing.T) {
	type obj map[string]any
	type arr []any

	tests := []struct {
		name  string
		input any
	}{
		{
			name: "Empty values",
			input: obj{
				"ID": 0,
				"A":  0,
				"Foo": obj{
					"First":  nil,
					"Second": nil,
					"Third":  nil,
				},
				"Bar": obj{
					"Meows":      arr{},
					"NestedFoos": arr{},
				},
			},
		},
		{
			name: "Basic values",
			input: obj{
				"ID": 1,
				"Foo": obj{
					"First":  21,
					"Second": nil,
					"Third":  22,
				},
				"A": 2,
				"Bar": obj{
					"Meows": arr{41, 42},
					"NestedFoos": arr{
						obj{"First": 27, "Second": nil, "Third": nil},
						obj{"First": nil, "Second": 28, "Third": 29},
					},
				},
			},
		},
		{
			name: "Empty array trickery",
			input: obj{
				"ID": 0,
				"A":  0,
				"Foo": obj{
					"First":  nil,
					"Second": nil,
					"Third":  nil,
				},
				"Bar": obj{
					"Meows": arr{},
					"NestedFoos": arr{
						obj{"First": nil, "Second": nil, "Third": nil},
						obj{"First": nil, "Second": 28, "Third": 29},
					},
				},
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			expectedDataBytes, err := json.Marshal(test.input)
			require.NoError(t, err)

			reader, err := newParquetEncodeProcessor(nil, testPMSchema(), "", &parquet.Uncompressed)
			require.NoError(t, err)

			readerResBatches, err := reader.ProcessBatch(t.Context(), service.MessageBatch{
				service.NewMessage(expectedDataBytes),
			})
			require.NoError(t, err)

			require.Len(t, readerResBatches, 1)
			require.Len(t, readerResBatches[0], 1)

			pqDataBytes, err := readerResBatches[0][0].AsBytes()
			require.NoError(t, err)

			pRdr := parquet.NewGenericReader[any](bytes.NewReader(pqDataBytes), testPMSchema())
			require.NoError(t, err)

			outRows := make([]any, 1)
			_, err = pRdr.Read(outRows)
			// Read returns EOF when finished
			if errors.Is(err, io.EOF) {
				err = nil
			}
			require.NoError(t, err)

			require.NoError(t, pRdr.Close())

			actualDataBytes, err := json.Marshal(outRows[0])
			require.NoError(t, err)

			assert.JSONEq(t, string(expectedDataBytes), string(actualDataBytes))
		})
	}

	t.Run("all together", func(t *testing.T) {
		var expected []any

		var inBatch service.MessageBatch
		for _, test := range tests {
			expected = append(expected, test.input)

			dataBytes, err := json.Marshal(test.input)
			require.NoError(t, err)

			inBatch = append(inBatch, service.NewMessage(dataBytes))
		}

		reader, err := newParquetEncodeProcessor(nil, testPMSchema(), "", &parquet.Uncompressed)
		require.NoError(t, err)

		readerResBatches, err := reader.ProcessBatch(t.Context(), inBatch)
		require.NoError(t, err)

		require.Len(t, readerResBatches, 1)
		require.Len(t, readerResBatches[0], 1)

		pqDataBytes, err := readerResBatches[0][0].AsBytes()
		require.NoError(t, err)

		pRdr := parquet.NewGenericReader[any](bytes.NewReader(pqDataBytes), testPMSchema())
		require.NoError(t, err)

		var outRows []any
		for {
			outRowsTmp := make([]any, 1)
			n, err := pRdr.Read(outRowsTmp)
			if !errors.Is(err, io.EOF) {
				require.NoError(t, err)
			}
			if n == 0 {
				if err != nil {
					require.ErrorIs(t, err, io.EOF)
				}
				break
			}
			outRows = append(outRows, outRowsTmp[0])
		}
		require.NoError(t, pRdr.Close())

		expectedBytes, err := json.Marshal(expected)
		require.NoError(t, err)
		actualBytes, err := json.Marshal(outRows)
		require.NoError(t, err)

		assert.JSONEq(t, string(expectedBytes), string(actualBytes))
	})
}

func TestParquetEncodeParallel(t *testing.T) {
	encodeConf, err := parquetEncodeProcessorConfig().ParseYAML(`
schema:
  - { name: id, type: INT64 }
  - { name: as, type: DOUBLE, repeated: true }
  - { name: b, type: BYTE_ARRAY }
  - { name: c, type: DOUBLE }
  - { name: d, type: BOOLEAN }
  - { name: e, type: INT64, optional: true }
  - { name: f, type: INT64 }
  - { name: g, type: UTF8 }
  - name: nested_stuff
    optional: true
    fields:
      - { name: a_stuff, type: BYTE_ARRAY }
      - { name: b_stuff, type: BYTE_ARRAY }
`, nil)
	require.NoError(t, err)

	encodeProc, err := newParquetEncodeProcessorFromConfig(encodeConf, nil)
	require.NoError(t, err)

	inBatch := service.MessageBatch{
		service.NewMessage([]byte(`{
	"id": 3,
	"as": [ 0.1, 0.2, 0.3, 0.4 ],
	"b": "hello world basic values",
	"c": 0.5,
	"d": true,
	"e": 6,
	"f": 7,
	"g": "logical string represent",
	"nested_stuff": {
		"a_stuff": "a value",
		"b_stuff": "b value"
	},
	"canary":"not in schema"
}`)),
	}

	wg := sync.WaitGroup{}
	for i := range 10 {
		wg.Add(1)
		t.Run(fmt.Sprintf("iteration %d", i), func(t *testing.T) {
			defer wg.Done()

			encodedBatches, err := encodeProc.ProcessBatch(t.Context(), inBatch)
			require.NoError(t, err)
			require.Len(t, encodedBatches, 1)
			require.Len(t, encodedBatches[0], 1)
		})
	}
	wg.Wait()
}

func TestParquetEncodeDynamicSchemaProcessor(t *testing.T) {
	type obj map[string]any
	type arr []any

	var expected []any

	var inBatch service.MessageBatch
	for _, inObj := range []any{
		obj{
			"foo": "hello world",
			"bar": obj{"a": 23, "b": true, "c": 0.5},
			"baz": arr{
				obj{"nested": arr{1, 2, 3}},
				obj{"nested": arr{4, 5, 6}},
			},
		},
		obj{
			"foo": "this is",
			"bar": obj{"a": nil, "b": true, "c": nil},
			"baz": arr{
				obj{"nested": arr{7}},
				obj{"nested": arr{8, 9}},
			},
		},
		obj{
			"foo": "my data",
			"bar": obj{"a": nil, "b": nil, "c": nil},
			"baz": arr{},
		},
	} {
		expected = append(expected, inObj)

		dataBytes, err := json.Marshal(inObj)
		require.NoError(t, err)

		inBatch = append(inBatch, service.NewMessage(dataBytes))
	}

	commonSchema := &schema.Common{
		Type: schema.Object,
		Children: []schema.Common{
			{
				Name: "foo",
				Type: schema.String,
			},
			{
				Name: "bar",
				Type: schema.Object,
				Children: []schema.Common{
					{
						Name:     "a",
						Type:     schema.Int64,
						Optional: true,
					},
					{
						Name:     "b",
						Type:     schema.Boolean,
						Optional: true,
					},
					{
						Name:     "c",
						Type:     schema.Float64,
						Optional: true,
					},
				},
			},
			{
				Name: "baz",
				Type: schema.Array,
				Children: []schema.Common{
					{
						Type: schema.Object,
						Children: []schema.Common{
							{
								Name: "nested",
								Type: schema.Array,
								Children: []schema.Common{
									{
										Type: schema.Int64,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	parquetSchema := parquet.NewSchema("test", parquet.Group{
		"foo": parquet.String(),
		"bar": parquet.Group{
			"a": parquet.Optional(parquet.Int(64)),
			"b": parquet.Optional(parquet.Leaf(parquet.BooleanType)),
			"c": parquet.Optional(parquet.Leaf(parquet.DoubleType)),
		},
		"baz": parquet.Repeated(parquet.Group{
			"nested": parquet.Repeated(parquet.Int(64)),
		}),
	})

	inBatch[0].MetaSetMut("foobar", commonSchema.ToAny())

	reader, err := newParquetEncodeProcessor(nil, nil, "foobar", &parquet.Uncompressed)
	require.NoError(t, err)

	readerResBatches, err := reader.ProcessBatch(t.Context(), inBatch)
	require.NoError(t, err)

	require.Len(t, readerResBatches, 1)
	require.Len(t, readerResBatches[0], 1)

	pqDataBytes, err := readerResBatches[0][0].AsBytes()
	require.NoError(t, err)

	pRdr := parquet.NewGenericReader[any](bytes.NewReader(pqDataBytes), parquetSchema)
	require.NoError(t, err)

	var outRows []any
	for {
		outRowsTmp := make([]any, 1)
		n, err := pRdr.Read(outRowsTmp)
		if !errors.Is(err, io.EOF) {
			require.NoError(t, err)
		}
		if n == 0 {
			if err != nil {
				require.ErrorIs(t, err, io.EOF)
			}
			break
		}
		outRows = append(outRows, outRowsTmp[0])
	}
	require.NoError(t, pRdr.Close())

	expectedBytes, err := json.Marshal(expected)
	require.NoError(t, err)
	actualBytes, err := json.Marshal(outRows)
	require.NoError(t, err)

	assert.JSONEq(t, string(expectedBytes), string(actualBytes))
}

func TestParquetEncodeProcessorConfigLinting(t *testing.T) {
	configTests := []struct {
		name        string
		config      string
		errContains string
	}{
		{
			name: "no schema or schema metadata",
			config: `
parquet_encode: {}
`,
			errContains: "either a schema or schema_metadata must be specified",
		},
		{
			name: "no schema",
			config: `
parquet_encode:
  schema_metadata: foo
`,
		},
		{
			name: "no schema_metadata",
			config: `
parquet_encode:
  schema:
    - name: foo
      type: INT64
`,
		},
	}

	env := service.NewEnvironment()
	for _, test := range configTests {
		t.Run(test.name, func(t *testing.T) {
			strm := env.NewStreamBuilder()
			err := strm.AddProcessorYAML(test.config)
			if test.errContains == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			}
		})
	}
}
