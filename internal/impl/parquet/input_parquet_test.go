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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type simpleData struct {
	ID    int64
	Value string
}

func TestParquetHappy(t *testing.T) {
	tmpDir := t.TempDir()

	for name, rows := range map[string][]simpleData{
		"1_first": {
			{ID: 1, Value: "foo 1"},
			{ID: 2, Value: "foo 2"},
			{ID: 3, Value: "foo 3"},
		},
		"2_second": {
			{ID: 4, Value: "bar 1"},
		},
		"3_third": {
			{ID: 5, Value: "baz 1"},
			{ID: 6, Value: "baz 2"},
			{ID: 7, Value: "baz 3"},
			{ID: 8, Value: "baz 4"},
		},
	} {
		buf := bytes.NewBuffer(nil)

		pWtr := parquet.NewWriter(buf, parquet.SchemaOf(simpleData{}))
		for _, r := range rows {
			require.NoError(t, pWtr.Write(r))
		}
		require.NoError(t, pWtr.Close())

		require.NoError(t, os.WriteFile(filepath.Join(tmpDir, name+".parquet"), buf.Bytes(), 0o655))
	}

	conf, err := parquetInputConfig().ParseYAML(fmt.Sprintf(`
paths: [ "%v/*.parquet" ]
batch_count: 2
`, tmpDir), nil)
	require.NoError(t, err)

	in, err := newParquetInputFromConfig(conf, service.MockResources())
	require.NoError(t, err)

	tCtx, done := context.WithTimeout(t.Context(), time.Minute)
	defer done()

	b, _, err := in.ReadBatch(tCtx)
	require.NoError(t, err)
	require.Len(t, b, 2)

	mBytes, err := b[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `{"ID":1,"Value":"foo 1"}`, string(mBytes))

	mBytes, err = b[1].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `{"ID":2,"Value":"foo 2"}`, string(mBytes))

	b, _, err = in.ReadBatch(tCtx)
	require.NoError(t, err)
	require.Len(t, b, 1)

	mBytes, err = b[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `{"ID":3,"Value":"foo 3"}`, string(mBytes))

	b, _, err = in.ReadBatch(tCtx)
	require.NoError(t, err)
	require.Len(t, b, 1)

	mBytes, err = b[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `{"ID":4,"Value":"bar 1"}`, string(mBytes))

	b, _, err = in.ReadBatch(tCtx)
	require.NoError(t, err)
	require.Len(t, b, 2)

	mBytes, err = b[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `{"ID":5,"Value":"baz 1"}`, string(mBytes))

	mBytes, err = b[1].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `{"ID":6,"Value":"baz 2"}`, string(mBytes))

	b, _, err = in.ReadBatch(tCtx)
	require.NoError(t, err)
	require.Len(t, b, 2)

	mBytes, err = b[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `{"ID":7,"Value":"baz 3"}`, string(mBytes))

	mBytes, err = b[1].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `{"ID":8,"Value":"baz 4"}`, string(mBytes))

	require.NoError(t, in.Close(tCtx))
}
