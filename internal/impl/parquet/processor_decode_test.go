package parquet

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
)

// Designed to contain all manner of structured data nasties.
type testPM struct {
	ID       int64
	Foo      testPMFoo
	A        int64
	Bar      testPMBar
	B        int64
	Foos     []testPMFoo
	C        int64
	MaybeFoo *testPMFoo
	D        int64
	E        int64
	Fs       []int64
	OA       *int64
	Bars     []testPMBar
	OB       *int64
	OFoo     *testPMFoo
	OC       *int64
	OD       *int64
	TailEs   []int64
}

type testPMFoo struct {
	First  *int64
	Second *int64
	Third  *int64
}

type testPMBar struct {
	Meows      []int64
	NestedFoos []testPMFoo
}

func iPtr(i int64) *int64 {
	return &i
}

func TestParquetDecodeProcessor(t *testing.T) {
	tests := []struct {
		name  string
		input testPM
	}{
		{
			name:  "Empty values",
			input: testPM{},
		},
		{
			name: "Basic values",
			input: testPM{
				ID: 1,
				A:  2,
				B:  3,
				C:  4,
				D:  5,
				E:  6,
			},
		},
		{
			name: "Non-nil basic values",
			input: testPM{
				ID: 1,
				Foo: testPMFoo{
					First: iPtr(9),
					Third: iPtr(10),
				},
				A:  2,
				B:  3,
				C:  4,
				D:  5,
				E:  6,
				Fs: []int64{21, 22, 23},
				OA: iPtr(11),
				OC: iPtr(13),
			},
		},
		{
			name: "Non-nil nested basic values",
			input: testPM{
				ID: 1,
				Foo: testPMFoo{
					First: iPtr(9),
					Third: iPtr(10),
				},
				A:  2,
				B:  3,
				C:  4,
				D:  5,
				E:  6,
				OA: iPtr(11),
				OFoo: &testPMFoo{
					Second: iPtr(12),
				},
				OC: iPtr(13),
			},
		},
		{
			name: "Array stuff",
			input: testPM{
				ID: 1,
				A:  2,
				B:  3,
				Foos: []testPMFoo{
					{
						Second: iPtr(10),
					},
					{
						Second: iPtr(11),
					},
				},
				C:  4,
				D:  5,
				E:  6,
				OA: iPtr(12),
				Bars: []testPMBar{
					{
						Meows: []int64{17},
						NestedFoos: []testPMFoo{
							{Second: iPtr(13)},
							{First: iPtr(14)},
						},
					},
					{
						Meows: []int64{15, 16},
						NestedFoos: []testPMFoo{
							{Third: iPtr(17)},
						},
					},
				},
				OC:     iPtr(18),
				TailEs: []int64{19, 20},
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			buf := bytes.NewBuffer(nil)

			pWtr := parquet.NewGenericWriter[testPM](buf)
			_, err := pWtr.Write([]testPM{test.input})
			require.NoError(t, err)
			require.NoError(t, pWtr.Close())

			expectedDataBytes, err := json.Marshal(test.input)
			require.NoError(t, err)

			reader, err := newParquetDecodeProcessor(nil, &extractConfig{})
			require.NoError(t, err)

			readerResBatch, err := reader.Process(context.Background(), service.NewMessage(buf.Bytes()))
			require.NoError(t, err)

			require.Len(t, readerResBatch, 1)

			actualDataBytes, err := readerResBatch[0].AsBytes()
			require.NoError(t, err)

			assert.JSONEq(t, string(expectedDataBytes), string(actualDataBytes))
		})
	}

	t.Run("all together", func(t *testing.T) {
		var expected, actual []any

		buf := bytes.NewBuffer(nil)
		pWtr := parquet.NewGenericWriter[testPM](buf)

		for _, test := range tests {
			_, err := pWtr.Write([]testPM{test.input})
			require.NoError(t, err)
			require.NoError(t, pWtr.Close())

			expected = append(expected, test.input)
		}

		reader, err := newParquetDecodeProcessor(nil, &extractConfig{})
		require.NoError(t, err)

		readerResBatch, err := reader.Process(context.Background(), service.NewMessage(buf.Bytes()))
		require.NoError(t, err)
		require.Len(t, readerResBatch, len(expected))

		for _, m := range readerResBatch {
			actualData, err := m.AsStructured()
			require.NoError(t, err)
			actual = append(actual, actualData)
		}

		expectedBytes, err := json.Marshal(expected)
		require.NoError(t, err)
		actualBytes, err := json.Marshal(actual)
		require.NoError(t, err)

		assert.JSONEq(t, string(expectedBytes), string(actualBytes))
	})
}

type decodeCompressionTest struct {
	Foo string
	Bar int64
	Baz []byte
}

func TestDecodeCompressionStringParsing(t *testing.T) {
	input := decodeCompressionTest{
		Foo: "foo value",
		Bar: 2,
		Baz: []byte("baz value"),
	}

	buf := bytes.NewBuffer(nil)

	pWtr := parquet.NewGenericWriter[decodeCompressionTest](buf)

	_, err := pWtr.Write([]decodeCompressionTest{input})
	require.NoError(t, err)
	require.NoError(t, pWtr.Close())

	reader, err := newParquetDecodeProcessor(nil, &extractConfig{
		byteArrayAsStrings: true,
	})
	require.NoError(t, err)

	readerResBatch, err := reader.Process(context.Background(), service.NewMessage(buf.Bytes()))
	require.NoError(t, err)

	require.Len(t, readerResBatch, 1)

	actualDataBytes, err := readerResBatch[0].AsBytes()
	require.NoError(t, err)

	assert.JSONEq(t, `{"Foo":"foo value", "Bar":2, "Baz":"baz value"}`, string(actualDataBytes))

	// Without string extraction

	reader, err = newParquetDecodeProcessor(nil, &extractConfig{
		byteArrayAsStrings: false,
	})
	require.NoError(t, err)

	readerResBatch, err = reader.Process(context.Background(), service.NewMessage(buf.Bytes()))
	require.NoError(t, err)

	require.Len(t, readerResBatch, 1)

	actualDataBytes, err = readerResBatch[0].AsBytes()
	require.NoError(t, err)

	assert.JSONEq(t, `{"Foo":"foo value", "Bar":2, "Baz":"YmF6IHZhbHVl"}`, string(actualDataBytes))
}

func TestDecodeCompression(t *testing.T) {
	input := decodeCompressionTest{
		Foo: "foo value this is large enough aaaaaaaa bbbbbbbb cccccccccc that compression actually helps",
		Bar: 2,
		Baz: []byte("baz value this is large enough aaaaaaaa bbbbbbbb cccccccccc that compression actually helps"),
	}

	bufUncompressed := bytes.NewBuffer(nil)
	bufCompressed := bytes.NewBuffer(nil)

	pWtr := parquet.NewGenericWriter[decodeCompressionTest](bufCompressed, parquet.Compression(&parquet.Zstd))
	_, err := pWtr.Write([]decodeCompressionTest{input})
	require.NoError(t, err)
	require.NoError(t, pWtr.Close())

	pWtr = parquet.NewGenericWriter[decodeCompressionTest](bufUncompressed)
	_, err = pWtr.Write([]decodeCompressionTest{input})
	require.NoError(t, err)
	require.NoError(t, pWtr.Close())

	// Check that compression actually happened
	assert.NotEqual(t, bufCompressed.String(), bufUncompressed.String())
	assert.Less(t, bufCompressed.Len(), bufUncompressed.Len())

	reader, err := newParquetDecodeProcessor(nil, &extractConfig{
		byteArrayAsStrings: true,
	})
	require.NoError(t, err)

	readerResBatch, err := reader.Process(context.Background(), service.NewMessage(bufCompressed.Bytes()))
	require.NoError(t, err)

	require.Len(t, readerResBatch, 1)

	actualDataBytes, err := readerResBatch[0].AsBytes()
	require.NoError(t, err)

	assert.JSONEq(t, `{"Foo":"foo value this is large enough aaaaaaaa bbbbbbbb cccccccccc that compression actually helps", "Bar":2, "Baz":"baz value this is large enough aaaaaaaa bbbbbbbb cccccccccc that compression actually helps"}`, string(actualDataBytes))
}
