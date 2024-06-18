package parquet

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/Jeffail/gabs/v2"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func testPMSchema() *parquet.Schema {
	return parquet.NewSchema("test", parquet.Group{
		"ID": parquet.Int(64),
		"Foo": parquet.Group{
			"First":  parquet.Optional(parquet.Int(64)),
			"Second": parquet.Optional(parquet.Int(64)),
			"Third":  parquet.Optional(parquet.Int(64)),
		},
		"A": parquet.Int(64),
		"Bar": parquet.Group{
			"Meows": parquet.Repeated(parquet.Int(64)),
			"NestedFoos": parquet.Repeated(parquet.Group{
				"First":  parquet.Optional(parquet.Int(64)),
				"Second": parquet.Optional(parquet.Int(64)),
				"Third":  parquet.Optional(parquet.Int(64)),
			}),
		},
	})
}

func TestParquetDecodeProcessor(t *testing.T) {
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
			name: "Non-nil basic values",
			input: obj{
				"ID": 1,
				"Foo": obj{
					"First":  9,
					"Second": nil,
					"Third":  10,
				},
				"A": 2,
				"Bar": obj{
					"Meows":      arr{},
					"NestedFoos": arr{},
				},
			},
		},
		{
			name: "Non-nil nested basic values",
			input: obj{
				"ID": 1,
				"Foo": obj{
					"First":  9,
					"Second": nil,
					"Third":  10,
				},
				"A": 2,
				"Bar": obj{
					"Meows":      arr{},
					"NestedFoos": arr{},
				},
			},
		},
		{
			name: "Array stuff",
			input: obj{
				"ID": 1,
				"A":  2,
				"Foo": obj{
					"First":  nil,
					"Second": 10,
					"Third":  nil,
				},
				"Bar": obj{
					"Meows": arr{17},
					"NestedFoos": arr{
						obj{"First": 14, "Second": nil, "Third": nil},
						obj{"First": nil, "Second": 13, "Third": nil},
						obj{"First": nil, "Second": nil, "Third": nil},
					},
				},
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			buf := bytes.NewBuffer(nil)

			pWtr := parquet.NewGenericWriter[any](buf, testPMSchema())
			_, err := pWtr.Write([]any{test.input})
			require.NoError(t, err)
			require.NoError(t, pWtr.Close())

			reader := &parquetDecodeProcessor{}

			readerResBatch, err := reader.Process(context.Background(), service.NewMessage(buf.Bytes()))
			require.NoError(t, err)

			require.Len(t, readerResBatch, 1)

			actualRoot, err := readerResBatch[0].AsStructured()
			require.NoError(t, err)

			assert.Equal(t, gabs.Wrap(test.input).StringIndent("", "\t"), gabs.Wrap(actualRoot).StringIndent("", "\t"))
		})
	}

	t.Run("all together", func(t *testing.T) {
		var expected, actual []any

		buf := bytes.NewBuffer(nil)
		pWtr := parquet.NewGenericWriter[any](buf, testPMSchema())

		for _, test := range tests {
			_, err := pWtr.Write([]any{test.input})
			require.NoError(t, err)
			require.NoError(t, pWtr.Close())

			expected = append(expected, test.input)
		}

		reader := &parquetDecodeProcessor{}

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

	reader := &parquetDecodeProcessor{}

	readerResBatch, err := reader.Process(context.Background(), service.NewMessage(buf.Bytes()))
	require.NoError(t, err)

	require.Len(t, readerResBatch, 1)

	actualDataBytes, err := readerResBatch[0].AsBytes()
	require.NoError(t, err)

	assert.JSONEq(t, `{"Foo":"foo value", "Bar":2, "Baz":"baz value"}`, string(actualDataBytes))
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

	reader := &parquetDecodeProcessor{}

	readerResBatch, err := reader.Process(context.Background(), service.NewMessage(bufCompressed.Bytes()))
	require.NoError(t, err)

	require.Len(t, readerResBatch, 1)

	actualDataBytes, err := readerResBatch[0].AsBytes()
	require.NoError(t, err)

	assert.JSONEq(t, `{"Foo":"foo value this is large enough aaaaaaaa bbbbbbbb cccccccccc that compression actually helps", "Bar":2, "Baz":"baz value this is large enough aaaaaaaa bbbbbbbb cccccccccc that compression actually helps"}`, string(actualDataBytes))
}
