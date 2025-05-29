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

package sql_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"

	_ "github.com/redpanda-data/connect/v4/public/components/sql"
)

func TestConnSettingsInitStmt(t *testing.T) {
	tCtx, done := context.WithTimeout(t.Context(), time.Second*30)
	defer done()

	tmpDir := t.TempDir()

	outputConf := fmt.Sprintf(`
sql_insert:
  driver: sqlite
  dsn: file:%v/foo.db
  table: things
  columns: [ foo, bar, baz ]
  args_mapping: 'root = [ this.foo, this.bar, this.baz ]'
  init_statement: |
    CREATE TABLE IF NOT EXISTS things (
      foo varchar(50) not null,
      bar varchar(50) not null,
      baz varchar(50) not null,
      primary key (foo)
    ) WITHOUT ROWID;
`, tmpDir)

	streamInBuilder := service.NewStreamBuilder()
	require.NoError(t, streamInBuilder.SetLoggerYAML(`level: OFF`))
	require.NoError(t, streamInBuilder.AddOutputYAML(outputConf))

	inFn, err := streamInBuilder.AddBatchProducerFunc()
	require.NoError(t, err)

	streamIn, err := streamInBuilder.Build()
	require.NoError(t, err)

	go func() {
		assert.NoError(t, streamIn.Run(tCtx))
	}()

	require.NoError(t, inFn(tCtx, service.MessageBatch{
		service.NewMessage([]byte(`{"foo":"first","bar":"first bar","baz":"first baz"}`)),
		service.NewMessage([]byte(`{"foo":"second","bar":"second bar","baz":"second baz"}`)),
		service.NewMessage([]byte(`{"foo":"third","bar":"third bar","baz":"third baz"}`)),
	}))

	require.NoError(t, streamIn.Stop(tCtx))

	inputConf := fmt.Sprintf(`
sql_select:
  driver: sqlite
  dsn: file:%v/foo.db
  table: things
  columns: [ foo, bar, baz ]
`, tmpDir)

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: OFF`))
	require.NoError(t, streamOutBuilder.AddInputYAML(inputConf))

	var msgs []string
	require.NoError(t, streamOutBuilder.AddConsumerFunc(func(_ context.Context, m *service.Message) error {
		bMsg, err := m.AsBytes()
		require.NoError(t, err)
		msgs = append(msgs, string(bMsg))
		return nil
	}))
	require.NoError(t, err)

	streamOut, err := streamOutBuilder.Build()
	require.NoError(t, err)

	assert.NoError(t, streamOut.Run(tCtx))

	assert.Equal(t, []string{
		`{"bar":"first bar","baz":"first baz","foo":"first"}`,
		`{"bar":"second bar","baz":"second baz","foo":"second"}`,
		`{"bar":"third bar","baz":"third baz","foo":"third"}`,
	}, msgs)
}

func TestConnSettingsInitFiles(t *testing.T) {
	tCtx, done := context.WithTimeout(t.Context(), time.Second*30)
	defer done()

	tmpDir := t.TempDir()

	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "foo.sql"), []byte(`
CREATE TABLE IF NOT EXISTS things (
  foo varchar(50) not null,
  bar varchar(50) not null,
  primary key (foo)
) WITHOUT ROWID;
`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "bar.sql"), []byte(`
ALTER TABLE things
ADD COLUMN baz varchar(50);
`), 0o644))

	outputConf := fmt.Sprintf(`
sql_insert:
  driver: sqlite
  dsn: file:%v/foo.db
  table: things
  columns: [ foo, bar, baz ]
  args_mapping: 'root = [ this.foo, this.bar, this.baz ]'
  init_files: [ "%v/foo.sql", "%v/bar.sql" ]
`, tmpDir, tmpDir, tmpDir)

	streamInBuilder := service.NewStreamBuilder()
	require.NoError(t, streamInBuilder.SetLoggerYAML(`level: OFF`))
	require.NoError(t, streamInBuilder.AddOutputYAML(outputConf))

	inFn, err := streamInBuilder.AddBatchProducerFunc()
	require.NoError(t, err)

	streamIn, err := streamInBuilder.Build()
	require.NoError(t, err)

	go func() {
		assert.NoError(t, streamIn.Run(tCtx))
	}()

	require.NoError(t, inFn(tCtx, service.MessageBatch{
		service.NewMessage([]byte(`{"foo":"first","bar":"first bar","baz":"first baz"}`)),
		service.NewMessage([]byte(`{"foo":"second","bar":"second bar","baz":"second baz"}`)),
		service.NewMessage([]byte(`{"foo":"third","bar":"third bar","baz":"third baz"}`)),
	}))

	require.NoError(t, streamIn.Stop(tCtx))

	inputConf := fmt.Sprintf(`
sql_select:
  driver: sqlite
  dsn: file:%v/foo.db
  table: things
  columns: [ foo, bar, baz ]
`, tmpDir)

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: OFF`))
	require.NoError(t, streamOutBuilder.AddInputYAML(inputConf))

	var msgs []string
	require.NoError(t, streamOutBuilder.AddConsumerFunc(func(_ context.Context, m *service.Message) error {
		bMsg, err := m.AsBytes()
		require.NoError(t, err)
		msgs = append(msgs, string(bMsg))
		return nil
	}))
	require.NoError(t, err)

	streamOut, err := streamOutBuilder.Build()
	require.NoError(t, err)

	assert.NoError(t, streamOut.Run(tCtx))

	assert.Equal(t, []string{
		`{"bar":"first bar","baz":"first baz","foo":"first"}`,
		`{"bar":"second bar","baz":"second baz","foo":"second"}`,
		`{"bar":"third bar","baz":"third baz","foo":"third"}`,
	}, msgs)
}
