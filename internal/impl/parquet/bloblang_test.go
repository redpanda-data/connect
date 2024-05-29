package parquet

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
)

func TestParquetParseBloblangAsStrings(t *testing.T) {
	buf := bytes.NewBuffer(nil)

	pWtr := parquet.NewGenericWriter[any](buf, parquet.NewSchema("test", parquet.Group{
		"ID": parquet.Int(64),
		"A":  parquet.Int(64),
		"B":  parquet.Int(64),
		"C":  parquet.Int(64),
		"D":  parquet.String(),
		"E":  parquet.Leaf(parquet.ByteArrayType),
	}))

	type obj map[string]any

	_, err := pWtr.Write([]any{
		obj{"ID": 1, "A": 11, "B": 21, "C": 31, "D": "first", "E": []byte("first")},
		obj{"ID": 2, "A": 12, "B": 22, "C": 32, "D": "second", "E": []byte("second")},
		obj{"ID": 3, "A": 13, "B": 23, "C": 33, "D": "third", "E": []byte("third")},
		obj{"ID": 4, "A": 14, "B": 24, "C": 34, "D": "fourth", "E": []byte("fourth")},
	})
	require.NoError(t, err)

	require.NoError(t, pWtr.Close())

	exec, err := bloblang.Parse(`root = this.parse_parquet(byte_array_as_string: true)`)
	require.NoError(t, err)

	res, err := exec.Query(buf.Bytes())
	require.NoError(t, err)

	actualDataBytes, err := json.Marshal(res)
	require.NoError(t, err)

	assert.JSONEq(t, `[
  {"ID": 1, "A": 11, "B": 21, "C": 31, "D": "first", "E": "first"},
  {"ID": 2, "A": 12, "B": 22, "C": 32, "D": "second", "E": "second"},
  {"ID": 3, "A": 13, "B": 23, "C": 33, "D": "third", "E": "third"},
  {"ID": 4, "A": 14, "B": 24, "C": 34, "D": "fourth", "E": "fourth"}
]`, string(actualDataBytes))
}

func TestParquetParseBloblangPanicInit(t *testing.T) {
	exec, err := bloblang.Parse(`root = this.parse_parquet()`)
	require.NoError(t, err)

	_, err = exec.Query([]byte(`hello world lol`))
	require.Error(t, err)
}
