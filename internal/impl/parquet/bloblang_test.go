package parquet

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/bloblang"
)

type testPMBlobl struct {
	ID int64
	A  int64
	B  int64
	C  int64
	D  string
	E  []byte
}

func TestParquetParseBloblang(t *testing.T) {
	buf := bytes.NewBuffer(nil)

	pWtr := parquet.NewWriter(buf, parquet.SchemaOf(testPMBlobl{}))
	for _, in := range []testPMBlobl{
		{ID: 1, A: 11, B: 21, C: 31, D: "first", E: []byte("first")},
		{ID: 2, A: 12, B: 22, C: 32, D: "second", E: []byte("second")},
		{ID: 3, A: 13, B: 23, C: 33, D: "third", E: []byte("third")},
		{ID: 4, A: 14, B: 24, C: 34, D: "fourth", E: []byte("fourth")},
	} {
		require.NoError(t, pWtr.Write(in))
	}
	require.NoError(t, pWtr.Close())

	exec, err := bloblang.Parse(`root = this.parse_parquet()`)
	require.NoError(t, err)

	res, err := exec.Query(buf.Bytes())
	require.NoError(t, err)

	actualDataBytes, err := json.Marshal(res)
	require.NoError(t, err)

	assert.JSONEq(t, `[
  {"ID": 1, "A": 11, "B": 21, "C": 31, "D": "first", "E": "Zmlyc3Q="},
  {"ID": 2, "A": 12, "B": 22, "C": 32, "D": "second", "E": "c2Vjb25k"},
  {"ID": 3, "A": 13, "B": 23, "C": 33, "D": "third", "E": "dGhpcmQ="},
  {"ID": 4, "A": 14, "B": 24, "C": 34, "D": "fourth", "E": "Zm91cnRo"}
]`, string(actualDataBytes))
}

func TestParquetParseBloblangAsStrings(t *testing.T) {
	buf := bytes.NewBuffer(nil)

	pWtr := parquet.NewWriter(buf, parquet.SchemaOf(testPMBlobl{}))
	for _, in := range []testPMBlobl{
		{ID: 1, A: 11, B: 21, C: 31, D: "first", E: []byte("first")},
		{ID: 2, A: 12, B: 22, C: 32, D: "second", E: []byte("second")},
		{ID: 3, A: 13, B: 23, C: 33, D: "third", E: []byte("third")},
		{ID: 4, A: 14, B: 24, C: 34, D: "fourth", E: []byte("fourth")},
	} {
		require.NoError(t, pWtr.Write(in))
	}
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
