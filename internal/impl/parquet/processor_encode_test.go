package parquet

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
)

func TestParquetEncodeDecodeRoundTrip(t *testing.T) {
	encodeConf, err := parquetEncodeProcessorConfig().ParseYAML(`
schema:
  - { name: id, type: INT64 }
  - { name: as, type: DOUBLE, repeated: true }
  - { name: b, type: BYTE_ARRAY }
  - { name: c, type: FLOAT }
  - { name: d, type: BOOLEAN }
  - { name: e, type: INT32, optional: true }
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
  - { name: c, type: FLOAT }
  - { name: d, type: BOOLEAN }
  - { name: e, type: INT32, optional: true }
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
  "as": null,
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

// Designed to contain all manner of structured data nasties, separated from the
// decode struct so that we can modify them separately.
type testMP struct {
	ID       int64
	Foo      testMPFoo
	A        int64
	Bar      testMPBar
	B        int64
	Foos     []testMPFoo
	C        int64
	MaybeFoo *testMPFoo
	D        int64
	E        int64
	Fs       []int64
	OA       *int64
	Bars     []testMPBar
	OB       *int64
	OFoo     *testMPFoo
	OC       *int64
	OD       *int64
	TailEs   []int64
}

type testMPFoo struct {
	First  *int64
	Second *int64
	Third  *int64
}

type testMPBar struct {
	Meows      []int64
	NestedFoos []testMPFoo
}

func TestParquetEncodeProcessor(t *testing.T) {
	tests := []struct {
		name  string
		input testMP
	}{
		{
			name: "Empty values",
			input: testMP{
				// Comparing in tests is bad as we fail on [] versus null here
				Foos: []testMPFoo{},
				Bar: testMPBar{
					Meows:      []int64{},
					NestedFoos: []testMPFoo{},
				},
				Fs:     []int64{},
				Bars:   []testMPBar{},
				TailEs: []int64{},
			},
		},
		{
			name: "Basic values",
			input: testMP{
				ID: 1,
				Foo: testMPFoo{
					First: iPtr(21),
					Third: iPtr(22),
				},
				A: 2,
				Bar: testMPBar{
					Meows: []int64{41, 42},
					NestedFoos: []testMPFoo{
						{First: iPtr(27)},
						{Second: iPtr(28), Third: iPtr(29)},
					},
				},
				B: 3,
				Foos: []testMPFoo{
					{Second: iPtr(23)},
					{Third: iPtr(24)},
				},
				C:  4,
				D:  5,
				E:  6,
				Fs: []int64{31, 33},
				Bars: []testMPBar{
					{
						Meows: []int64{43, 44},
						NestedFoos: []testMPFoo{
							{First: iPtr(60)},
							{Second: iPtr(61), Third: iPtr(62)},
						},
					},
					{
						Meows: []int64{45},
						NestedFoos: []testMPFoo{
							{First: iPtr(63)},
							{Second: iPtr(64), Third: iPtr(65)},
						},
					},
				},
				OB: iPtr(7),
				OFoo: &testMPFoo{
					Second: iPtr(26),
				},
				OD:     iPtr(8),
				TailEs: []int64{34, 35, 36},
			},
		},
		{
			name: "Empty array trickery",
			input: testMP{
				Bar: testMPBar{
					Meows: []int64{},
					NestedFoos: []testMPFoo{
						{},
						{Second: iPtr(28), Third: iPtr(29)},
					},
				},
				B: 3,
				Foos: []testMPFoo{
					{},
					{Third: iPtr(24)},
					{},
				},
				E:  6,
				Fs: []int64{},
				Bars: []testMPBar{
					{
						Meows: []int64{},
						NestedFoos: []testMPFoo{
							{First: iPtr(60)},
							{Second: iPtr(61), Third: iPtr(62)},
						},
					},
					{
						Meows: []int64{},
						NestedFoos: []testMPFoo{
							{First: iPtr(63)},
							{Second: iPtr(64), Third: iPtr(65)},
						},
					},
				},
				TailEs: []int64{34, 35, 36},
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			expectedDataBytes, err := json.Marshal(test.input)
			require.NoError(t, err)

			schema := parquet.SchemaOf(test.input)

			reader, err := newParquetEncodeProcessor(nil, schema, &parquet.Uncompressed)
			require.NoError(t, err)

			readerResBatches, err := reader.ProcessBatch(context.Background(), service.MessageBatch{
				service.NewMessage(expectedDataBytes),
			})
			require.NoError(t, err)

			require.Len(t, readerResBatches, 1)
			require.Len(t, readerResBatches[0], 1)

			pqDataBytes, err := readerResBatches[0][0].AsBytes()
			require.NoError(t, err)

			pRdr := parquet.NewGenericReader[testMP](bytes.NewReader(pqDataBytes))
			require.NoError(t, err)

			outRows := make([]testMP, 1)
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

		schema := parquet.SchemaOf(tests[0].input)

		reader, err := newParquetEncodeProcessor(nil, schema, &parquet.Uncompressed)
		require.NoError(t, err)

		readerResBatches, err := reader.ProcessBatch(context.Background(), inBatch)
		require.NoError(t, err)

		require.Len(t, readerResBatches, 1)
		require.Len(t, readerResBatches[0], 1)

		pqDataBytes, err := readerResBatches[0][0].AsBytes()
		require.NoError(t, err)

		pRdr := parquet.NewGenericReader[testMP](bytes.NewReader(pqDataBytes))
		require.NoError(t, err)

		var outRows []testMP
		for {
			outRowsTmp := make([]testMP, 1)
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
