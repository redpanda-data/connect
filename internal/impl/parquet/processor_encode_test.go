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
		service.NewMessage([]byte(`{"id":"bar","name":"foo"}`)),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "encoding panic")
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
		t.Run(test.name, func(t *testing.T) {
			expectedDataBytes, err := json.Marshal(test.input)
			require.NoError(t, err)

			reader, err := newParquetEncodeProcessor(nil, testPMSchema(), "", &parquet.Uncompressed, parquet.Nanosecond)
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

		reader, err := newParquetEncodeProcessor(nil, testPMSchema(), "", &parquet.Uncompressed, parquet.Nanosecond)
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

	reader, err := newParquetEncodeProcessor(nil, nil, "foobar", &parquet.Uncompressed, parquet.Nanosecond)
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

func TestParquetEncodeDynamicSchemaAnyFieldError(t *testing.T) {
	commonSchema := &schema.Common{
		Type: schema.Object,
		Children: []schema.Common{
			{
				Name: "id",
				Type: schema.Int64,
			},
			{
				Name: "payload",
				Type: schema.Any,
			},
		},
	}

	inBatch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":1,"payload":{"key":"value"}}`)),
	}
	inBatch[0].MetaSetMut("schema", commonSchema.ToAny())

	proc, err := newParquetEncodeProcessor(nil, nil, "schema", &parquet.Uncompressed, parquet.Nanosecond)
	require.NoError(t, err)

	_, err = proc.ProcessBatch(t.Context(), inBatch)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "payload")
	assert.Contains(t, err.Error(), "ANY")
}

func TestParquetEncodeTimestampUnit(t *testing.T) {
	tests := []struct {
		name           string
		unitConfig     string
		expectedUnit   parquet.TimeUnit
		expectedSchema string
	}{
		{name: "default is nanosecond", unitConfig: "", expectedUnit: parquet.Nanosecond, expectedSchema: "unit=NANOS"},
		{name: "microsecond", unitConfig: "default_timestamp_unit: MICROSECOND", expectedUnit: parquet.Microsecond, expectedSchema: "unit=MICROS"},
		{name: "millisecond", unitConfig: "default_timestamp_unit: MILLISECOND", expectedUnit: parquet.Millisecond, expectedSchema: "unit=MILLIS"},
		{name: "explicit nanosecond", unitConfig: "default_timestamp_unit: NANOSECOND", expectedUnit: parquet.Nanosecond, expectedSchema: "unit=NANOS"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			configYAML := fmt.Sprintf(`
schema:
  - { name: id, type: INT64 }
  - { name: ts, type: TIMESTAMP }
%s
`, test.unitConfig)
			encodeConf, err := parquetEncodeProcessorConfig().ParseYAML(configYAML, nil)
			require.NoError(t, err)

			encodeProc, err := newParquetEncodeProcessorFromConfig(encodeConf, nil)
			require.NoError(t, err)
			require.Equal(t, test.expectedUnit, encodeProc.timestampUnit)

			batches, err := encodeProc.ProcessBatch(t.Context(), service.MessageBatch{
				service.NewMessage([]byte(`{"id":1,"ts":"2026-04-17T12:00:00Z"}`)),
			})
			require.NoError(t, err)
			require.Len(t, batches, 1)
			require.Len(t, batches[0], 1)

			pqBytes, err := batches[0][0].AsBytes()
			require.NoError(t, err)

			pqFile, err := parquet.OpenFile(bytes.NewReader(pqBytes), int64(len(pqBytes)))
			require.NoError(t, err)
			assert.Contains(t, pqFile.Schema().String(), test.expectedSchema)
		})
	}
}

func TestParquetEncodeTimestampUnitDynamicSchema(t *testing.T) {
	encodeConf, err := parquetEncodeProcessorConfig().ParseYAML(`
schema_metadata: benthos_schema
default_timestamp_unit: MICROSECOND
`, nil)
	require.NoError(t, err)

	encodeProc, err := newParquetEncodeProcessorFromConfig(encodeConf, nil)
	require.NoError(t, err)

	commonSchema := &schema.Common{
		Type: schema.Object,
		Children: []schema.Common{
			{Name: "id", Type: schema.Int64},
			{Name: "ts", Type: schema.Timestamp},
		},
	}

	msg := service.NewMessage([]byte(`{"id":1,"ts":"2026-04-17T12:00:00Z"}`))
	msg.MetaSetMut("benthos_schema", commonSchema.ToAny())

	batches, err := encodeProc.ProcessBatch(t.Context(), service.MessageBatch{msg})
	require.NoError(t, err)
	require.Len(t, batches, 1)
	require.Len(t, batches[0], 1)

	pqBytes, err := batches[0][0].AsBytes()
	require.NoError(t, err)

	pqFile, err := parquet.OpenFile(bytes.NewReader(pqBytes), int64(len(pqBytes)))
	require.NoError(t, err)
	assert.Contains(t, pqFile.Schema().String(), "unit=MICROS")
}

func TestParquetEncodeTimestampUnitInvalid(t *testing.T) {
	env := service.NewEnvironment()
	err := env.NewStreamBuilder().AddProcessorYAML(`
parquet_encode:
  schema:
    - { name: id, type: INT64 }
  default_timestamp_unit: PICOSECOND
`)
	require.Error(t, err)
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

// TestParquetEncodeDecimalSmoke exercises the metadata-driven encoding path
// for Decimal columns covering all three precision buckets (Int32, Int64,
// FixedLenByteArray) plus negative values, padding, and BigDecimal
// rejection.
func TestParquetEncodeDecimalSmoke(t *testing.T) {
	commonSchema := &schema.Common{
		Type: schema.Object,
		Children: []schema.Common{
			{
				Name:    "small",
				Type:    schema.Decimal,
				Logical: &schema.LogicalParams{Decimal: &schema.DecimalParams{Precision: 9, Scale: 2}},
			},
			{
				Name:    "medium",
				Type:    schema.Decimal,
				Logical: &schema.LogicalParams{Decimal: &schema.DecimalParams{Precision: 18, Scale: 4}},
			},
			{
				Name:    "large",
				Type:    schema.Decimal,
				Logical: &schema.LogicalParams{Decimal: &schema.DecimalParams{Precision: 38, Scale: 8}},
			},
		},
	}

	row := map[string]any{
		"small":  "1234567.89",
		"medium": "-12345678901234.5678",
		"large":  "1.50000000",
	}
	rowBytes, err := json.Marshal(row)
	require.NoError(t, err)

	msg := service.NewMessage(rowBytes)
	msg.MetaSetMut("schema_meta", commonSchema.ToAny())

	proc, err := newParquetEncodeProcessor(nil, nil, "schema_meta", &parquet.Uncompressed, parquet.Nanosecond)
	require.NoError(t, err)

	out, err := proc.ProcessBatch(t.Context(), service.MessageBatch{msg})
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Len(t, out[0], 1)

	encoded, err := out[0][0].AsBytes()
	require.NoError(t, err)

	pf, err := parquet.OpenFile(bytes.NewReader(encoded), int64(len(encoded)))
	require.NoError(t, err)

	// Confirm each column carries a decimal logical type with the declared
	// precision and scale, and that the physical type matches the bucket.
	cols := pf.Schema().Columns()
	require.Len(t, cols, 3)
	for _, want := range []struct {
		name      string
		precision int32
		scale     int32
		kind      parquet.Kind
	}{
		{"small", 9, 2, parquet.Int32},
		{"medium", 18, 4, parquet.Int64},
		{"large", 38, 8, parquet.FixedLenByteArray},
	} {
		t.Run(want.name, func(t *testing.T) {
			node, err := pf.Schema().Lookup(want.name)
			require.True(t, err)
			lt := node.Node.Type().LogicalType()
			require.NotNil(t, lt)
			require.NotNil(t, lt.Decimal)
			assert.Equal(t, want.precision, lt.Decimal.Precision, "precision")
			assert.Equal(t, want.scale, lt.Decimal.Scale, "scale")
			assert.Equal(t, want.kind, node.Node.Type().Kind(), "physical kind")
		})
	}
}

// TestParquetEncodeBigDecimalRejected confirms that BigDecimal columns
// produce a clear error rather than silently dropping precision.
func TestParquetEncodeBigDecimalRejected(t *testing.T) {
	commonSchema := &schema.Common{
		Type: schema.Object,
		Children: []schema.Common{
			{Name: "amount", Type: schema.BigDecimal},
		},
	}

	msg := service.NewMessage([]byte(`{"amount":"1.5"}`))
	msg.MetaSetMut("schema_meta", commonSchema.ToAny())

	proc, err := newParquetEncodeProcessor(nil, nil, "schema_meta", &parquet.Uncompressed, parquet.Nanosecond)
	require.NoError(t, err)

	_, err = proc.ProcessBatch(t.Context(), service.MessageBatch{msg})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "BigDecimal")
}
