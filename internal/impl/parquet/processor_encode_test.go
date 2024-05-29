package parquet

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

	tctx := context.Background()
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
`, nil)
	require.NoError(t, err)

	decodeProc, err := newParquetDecodeProcessorFromConfig(decodeConf, nil)
	require.NoError(t, err)

	testParquetEncodeDecodeRoundTrip(t, encodeProc, decodeProc)
}

func testParquetEncodeDecodeRoundTrip(t *testing.T, encodeProc *parquetEncodeProcessor, decodeProc *parquetDecodeProcessor) {
	tctx := context.Background()

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
	tctx := context.Background()

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

			reader, err := newParquetEncodeProcessor(nil, testPMSchema(), &parquet.Uncompressed)
			require.NoError(t, err)

			readerResBatches, err := reader.ProcessBatch(context.Background(), service.MessageBatch{
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
			require.NoError(t, err)
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

		reader, err := newParquetEncodeProcessor(nil, testPMSchema(), &parquet.Uncompressed)
		require.NoError(t, err)

		readerResBatches, err := reader.ProcessBatch(context.Background(), inBatch)
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
			_, err := pRdr.Read(outRowsTmp)
			if err != nil {
				require.ErrorIs(t, err, io.EOF)
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
