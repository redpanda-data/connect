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
}

func TestParquetParseBloblang(t *testing.T) {
	buf := bytes.NewBuffer(nil)

	pWtr := parquet.NewWriter(buf, parquet.SchemaOf(testPMBlobl{}))
	for _, in := range []testPMBlobl{
		{ID: 1, A: 11, B: 21, C: 31, D: "first"},
		{ID: 2, A: 12, B: 22, C: 32, D: "second"},
		{ID: 3, A: 13, B: 23, C: 33, D: "third"},
		{ID: 4, A: 14, B: 24, C: 34, D: "fourth"},
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
  {"ID": 1, "A": 11, "B": 21, "C": 31, "D": "Zmlyc3Q="},
  {"ID": 2, "A": 12, "B": 22, "C": 32, "D": "c2Vjb25k"},
  {"ID": 3, "A": 13, "B": 23, "C": 33, "D": "dGhpcmQ="},
  {"ID": 4, "A": 14, "B": 24, "C": 34, "D": "Zm91cnRo"}
]`, string(actualDataBytes))
}

func TestParquetParseBloblangAsStrings(t *testing.T) {
	buf := bytes.NewBuffer(nil)

	pWtr := parquet.NewWriter(buf, parquet.SchemaOf(testPMBlobl{}))
	for _, in := range []testPMBlobl{
		{ID: 1, A: 11, B: 21, C: 31, D: "first"},
		{ID: 2, A: 12, B: 22, C: 32, D: "second"},
		{ID: 3, A: 13, B: 23, C: 33, D: "third"},
		{ID: 4, A: 14, B: 24, C: 34, D: "fourth"},
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
  {"ID": 1, "A": 11, "B": 21, "C": 31, "D": "first"},
  {"ID": 2, "A": 12, "B": 22, "C": 32, "D": "second"},
  {"ID": 3, "A": 13, "B": 23, "C": 33, "D": "third"},
  {"ID": 4, "A": 14, "B": 24, "C": 34, "D": "fourth"}
]`, string(actualDataBytes))
}
