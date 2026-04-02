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
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	isql "github.com/redpanda-data/connect/v4/internal/impl/sql"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"

	_ "github.com/redpanda-data/connect/v4/public/components/sql"
)

type testFn func(t *testing.T, driver, dsn, table string)

func testProcessors(name string, fn func(t *testing.T, insertProc, selectProc service.BatchProcessor)) testFn {
	return func(t *testing.T, driver, dsn, table string) {
		colList := `[ "foo", "bar", "baz" ]`
		if driver == "oracle" {
			colList = `[ "\"foo\"", "\"bar\"", "\"baz\"" ]`
		}
		t.Run(name, func(t *testing.T) {
			insertConf := fmt.Sprintf(`
driver: %s
dsn: %s
table: %s
columns: %s
args_mapping: 'root = [ this.foo, this.bar.floor(), this.baz ]'
`, driver, dsn, table, colList)

			queryConf := fmt.Sprintf(`
driver: %s
dsn: %s
table: %s
columns: [ "*" ]
where: '"foo" = ?'
args_mapping: 'root = [ this.id ]'
`, driver, dsn, table)

			env := service.NewEnvironment()

			insertConfig, err := isql.InsertProcessorConfig().ParseYAML(insertConf, env)
			require.NoError(t, err)

			selectConfig, err := isql.SelectProcessorConfig().ParseYAML(queryConf, env)
			require.NoError(t, err)

			insertProc, err := isql.NewSQLInsertProcessorFromConfig(insertConfig, service.MockResources())
			require.NoError(t, err)
			t.Cleanup(func() { insertProc.Close(t.Context()) })

			selectProc, err := isql.NewSQLSelectProcessorFromConfig(selectConfig, service.MockResources())
			require.NoError(t, err)
			t.Cleanup(func() { selectProc.Close(t.Context()) })

			fn(t, insertProc, selectProc)
		})
	}
}

func testRawProcessors(name string, fn func(t *testing.T, insertProc, selectProc service.BatchProcessor)) testFn {
	return func(t *testing.T, driver, dsn, table string) {
		t.Run(name, func(t *testing.T) {
			valuesStr := `(?, ?, ?)`
			switch driver {
			case "postgres", "pgx", "clickhouse":
				valuesStr = `($1, $2, $3)`
			case "oracle":
				valuesStr = `(:1, :2, :3)`
			}
			insertConf := fmt.Sprintf(`
driver: %s
dsn: %s
query: insert into %s ( "foo", "bar", "baz" ) values `+valuesStr+`
args_mapping: 'root = [ this.foo, this.bar.floor(), this.baz ]'
exec_only: true
`, driver, dsn, table)

			placeholderStr := "?"
			switch driver {
			case "postgres", "pgx", "clickhouse":
				placeholderStr = "$1"
			case "oracle":
				placeholderStr = ":1"
			}
			queryConf := fmt.Sprintf(`
driver: %s
dsn: %s
query: select "foo", "bar", "baz" from %s where "foo" = `+placeholderStr+`
args_mapping: 'root = [ this.id ]'
`, driver, dsn, table)

			env := service.NewEnvironment()

			insertConfig, err := isql.RawProcessorConfig().ParseYAML(insertConf, env)
			require.NoError(t, err)

			selectConfig, err := isql.RawProcessorConfig().ParseYAML(queryConf, env)
			require.NoError(t, err)

			insertProc, err := isql.NewSQLRawProcessorFromConfig(insertConfig, service.MockResources())
			require.NoError(t, err)
			t.Cleanup(func() { insertProc.Close(t.Context()) })

			selectProc, err := isql.NewSQLRawProcessorFromConfig(selectConfig, service.MockResources())
			require.NoError(t, err)
			t.Cleanup(func() { selectProc.Close(t.Context()) })

			fn(t, insertProc, selectProc)
		})
	}
}

func testRawTransactionalProcessors(name string, fn func(t *testing.T, insertProc, selectProc service.BatchProcessor)) testFn {
	return func(t *testing.T, driver, dsn, table string) {
		t.Run(name, func(t *testing.T) {
			if driver == "trino" {
				t.Skip("transactions not supported")
			}
			placeholderStr := "?"
			valuesStr := `(?, ?, ?)`
			switch driver {
			case "postgres", "pgx", "clickhouse":
				valuesStr = `($1, $2, $3)`
				placeholderStr = "$1"
			case "oracle":
				valuesStr = `(:1, :2, :3)`
				placeholderStr = ":1"
			}
			updateStatement := fmt.Sprintf(`update %s set "bar" = "bar" + 1 WHERE "foo" = %s`, table, placeholderStr)
			if driver == "clickhouse" {
				updateStatement = fmt.Sprintf(`alter table %s update bar = bar + 1 where foo = %s`, table, placeholderStr)
			}
			insertConf := fmt.Sprintf(`
driver: %s
dsn: %s
query: insert into %s ( "foo", "bar", "baz" ) values `+valuesStr+`
args_mapping: 'root = [ this.foo, this.bar.floor(), this.baz ]'
exec_only: true
queries:
  - query: %s
    args_mapping: 'root = [ this.foo ]'
    exec_only: true
`, driver, dsn, table, updateStatement)

			updateStatement = strings.ReplaceAll(updateStatement, "+", "-")
			queryConf := fmt.Sprintf(`
driver: %s
dsn: %s
queries:
  - query: %s
    args_mapping: 'root = [ this.id ]'
  - query: select "foo", "bar", "baz" from %s where "foo" = `+placeholderStr+`
    args_mapping: 'root = [ this.id ]'
`, driver, dsn, updateStatement, table)

			env := service.NewEnvironment()

			insertConfig, err := isql.RawProcessorConfig().ParseYAML(insertConf, env)
			require.NoError(t, err)

			selectConfig, err := isql.RawProcessorConfig().ParseYAML(queryConf, env)
			require.NoError(t, err)

			insertProc, err := isql.NewSQLRawProcessorFromConfig(insertConfig, service.MockResources())
			require.NoError(t, err)
			t.Cleanup(func() { insertProc.Close(t.Context()) })

			selectProc, err := isql.NewSQLRawProcessorFromConfig(selectConfig, service.MockResources())
			require.NoError(t, err)
			t.Cleanup(func() { selectProc.Close(t.Context()) })

			fn(t, insertProc, selectProc)
		})
	}
}

func testRawDeprecatedProcessors(name string, fn func(t *testing.T, insertProc, selectProc service.BatchProcessor)) testFn {
	return func(t *testing.T, driver, dsn, table string) {
		t.Run(name, func(t *testing.T) {
			valuesStr := `(?, ?, ?)`
			switch driver {
			case "postgres", "pgx", "clickhouse":
				valuesStr = `($1, $2, $3)`
			case "oracle":
				valuesStr = `(:1, :2, :3)`
			}
			insertConf := fmt.Sprintf(`
driver: %s
data_source_name: %s
query: insert into %s ( "foo", "bar", "baz" ) values `+valuesStr+`
args_mapping: 'root = [ this.foo, this.bar.floor(), this.baz ]'
`, driver, dsn, table)

			placeholderStr := "?"
			switch driver {
			case "postgres", "pgx", "clickhouse":
				placeholderStr = "$1"
			case "oracle":
				placeholderStr = ":1"
			}
			queryConf := fmt.Sprintf(`
driver: %s
data_source_name: %s
query: select "foo", "bar", "baz" from %s where "foo" = `+placeholderStr+`
args_mapping: 'root = [ this.id ]'
result_codec: json_array
`, driver, dsn, table)

			env := service.NewEnvironment()

			insertConfig, err := isql.DeprecatedProcessorConfig().ParseYAML(insertConf, env)
			require.NoError(t, err)

			selectConfig, err := isql.DeprecatedProcessorConfig().ParseYAML(queryConf, env)
			require.NoError(t, err)

			insertProc, err := isql.NewSQLDeprecatedProcessorFromConfig(insertConfig, service.MockResources())
			require.NoError(t, err)
			t.Cleanup(func() { insertProc.Close(t.Context()) })

			selectProc, err := isql.NewSQLDeprecatedProcessorFromConfig(selectConfig, service.MockResources())
			require.NoError(t, err)
			t.Cleanup(func() { selectProc.Close(t.Context()) })

			fn(t, insertProc, selectProc)
		})
	}
}

var testBatchProcessorBasic = testProcessors("basic", func(t *testing.T, insertProc, selectProc service.BatchProcessor) {
	var insertBatch service.MessageBatch
	for i := range 10 {
		insertBatch = append(insertBatch, service.NewMessage(fmt.Appendf(nil, `{
  "foo": "doc-%d",
  "bar": %d,
  "baz": "and this"
}`, i, i)))
	}

	resBatches, err := insertProc.ProcessBatch(t.Context(), insertBatch)
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], len(insertBatch))
	for _, v := range resBatches[0] {
		require.NoError(t, v.GetError())
	}

	var queryBatch service.MessageBatch
	for i := range 10 {
		queryBatch = append(queryBatch, service.NewMessage(fmt.Appendf(nil, `{"id":"doc-%d"}`, i)))
	}

	resBatches, err = selectProc.ProcessBatch(t.Context(), queryBatch)
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], len(queryBatch))
	for i, v := range resBatches[0] {
		require.NoError(t, v.GetError())

		exp := fmt.Sprintf(`[{"bar":%d,"baz":"and this","foo":"doc-%d"}]`, i, i)
		actBytes, err := v.AsBytes()
		require.NoError(t, err)

		assert.Equal(t, exp, string(actBytes))
	}
})

var testBatchProcessorParallel = testProcessors("parallel", func(t *testing.T, insertProc, selectProc service.BatchProcessor) {
	nParallel, nLoops := 10, 50

	startChan := make(chan struct{})
	var wg sync.WaitGroup
	for i := range nParallel {
		var insertBatch service.MessageBatch
		for j := range nLoops {
			index := i*nLoops + j
			insertBatch = append(insertBatch, service.NewMessage(fmt.Appendf(nil, `{
  "foo": "doc-%d",
  "bar": %d,
  "baz": "and this"
}`, index, index)))
		}

		wg.Go(func() {
			<-startChan
			for _, msg := range insertBatch {
				_, err := insertProc.ProcessBatch(t.Context(), service.MessageBatch{msg})
				require.NoError(t, err)
			}
		})
	}

	close(startChan)
	wg.Wait()

	startChan = make(chan struct{})
	wg = sync.WaitGroup{}
	for i := range nParallel {
		var queryBatch service.MessageBatch

		for j := range nLoops {
			index := i*nLoops + j
			queryBatch = append(queryBatch, service.NewMessage(fmt.Appendf(nil, `{"id":"doc-%d"}`, index)))
		}

		wg.Go(func() {
			<-startChan
			for _, msg := range queryBatch {
				resBatches, err := selectProc.ProcessBatch(t.Context(), service.MessageBatch{msg})
				require.NoError(t, err)
				require.Len(t, resBatches, 1)
				require.Len(t, resBatches[0], 1)
				require.NoError(t, resBatches[0][0].GetError())
			}
		})
	}

	close(startChan)
	wg.Wait()
})

func rawProcessorTest(t *testing.T, insertProc, selectProc service.BatchProcessor) {
	var insertBatch service.MessageBatch
	for i := range 10 {
		insertBatch = append(insertBatch, service.NewMessage(fmt.Appendf(nil, `{
  "foo": "doc-%d",
  "bar": %d,
  "baz": "and this"
}`, i, i)))
	}

	resBatches, err := insertProc.ProcessBatch(t.Context(), insertBatch)
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], len(insertBatch))
	for _, v := range resBatches[0] {
		require.NoError(t, v.GetError())
	}

	var queryBatch service.MessageBatch
	for i := range 10 {
		queryBatch = append(queryBatch, service.NewMessage(fmt.Appendf(nil, `{"id":"doc-%d"}`, i)))
	}

	resBatches, err = selectProc.ProcessBatch(t.Context(), queryBatch)
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], len(queryBatch))
	for i, v := range resBatches[0] {
		require.NoError(t, v.GetError())

		exp := fmt.Sprintf(`[{"bar":%d,"baz":"and this","foo":"doc-%d"}]`, i, i)
		actBytes, err := v.AsBytes()
		require.NoError(t, err)

		assert.JSONEq(t, exp, string(actBytes))
	}
}

var testRawProcessorsBasic = testRawProcessors("raw", rawProcessorTest)

var testRawProcessorsTransactional = testRawTransactionalProcessors("raw_txn", rawProcessorTest)

var testDeprecatedProcessorsBasic = testRawDeprecatedProcessors("deprecated", rawProcessorTest)

func testBatchInputOutputBatch(t *testing.T, driver, dsn, table string) {
	colList := `[ "foo", "bar", "baz" ]`
	if driver == "oracle" {
		colList = `[ "\"foo\"", "\"bar\"", "\"baz\"" ]`
	}
	t.Run("batch_input_output", func(t *testing.T) {
		confReplacer := strings.NewReplacer(
			"$driver", driver,
			"$dsn", dsn,
			"$table", table,
			"$columnlist", colList,
		)

		outputConf := confReplacer.Replace(`
sql_insert:
  driver: $driver
  dsn: $dsn
  table: $table
  columns: $columnlist
  args_mapping: 'root = [ this.foo, this.bar.floor(), this.baz ]'
`)

		inputConf := confReplacer.Replace(`
sql_select:
  driver: $driver
  dsn: $dsn
  table: $table
  columns: [ "*" ]
  suffix: ' ORDER BY "bar" ASC'
processors:
  # For some reason MySQL driver doesn't resolve to integer by default.
  - bloblang: |
      root = this
      root.bar = this.bar.number()
`)

		streamInBuilder := service.NewStreamBuilder()
		require.NoError(t, streamInBuilder.SetLoggerYAML(`level: OFF`))
		require.NoError(t, streamInBuilder.AddOutputYAML(outputConf))

		inFn, err := streamInBuilder.AddBatchProducerFunc()
		require.NoError(t, err)

		streamIn, err := streamInBuilder.Build()
		require.NoError(t, err)

		go func() {
			assert.NoError(t, streamIn.Run(t.Context()))
		}()

		streamOutBuilder := service.NewStreamBuilder()
		require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: OFF`))
		require.NoError(t, streamOutBuilder.AddInputYAML(inputConf))

		var outBatches []string
		require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
			msgBytes, err := mb[0].AsBytes()
			require.NoError(t, err)
			outBatches = append(outBatches, string(msgBytes))
			return nil
		}))

		streamOut, err := streamOutBuilder.Build()
		require.NoError(t, err)

		var insertBatch service.MessageBatch
		for i := range 10 {
			insertBatch = append(insertBatch, service.NewMessage(fmt.Appendf(nil, `{
	"foo": "doc-%d",
	"bar": %d,
	"baz": "and this"
}`, i, i)))
		}
		require.NoError(t, inFn(t.Context(), insertBatch))
		require.NoError(t, streamIn.StopWithin(15*time.Second))

		require.NoError(t, streamOut.Run(t.Context()))

		assert.Equal(t, []string{
			"{\"bar\":0,\"baz\":\"and this\",\"foo\":\"doc-0\"}",
			"{\"bar\":1,\"baz\":\"and this\",\"foo\":\"doc-1\"}",
			"{\"bar\":2,\"baz\":\"and this\",\"foo\":\"doc-2\"}",
			"{\"bar\":3,\"baz\":\"and this\",\"foo\":\"doc-3\"}",
			"{\"bar\":4,\"baz\":\"and this\",\"foo\":\"doc-4\"}",
			"{\"bar\":5,\"baz\":\"and this\",\"foo\":\"doc-5\"}",
			"{\"bar\":6,\"baz\":\"and this\",\"foo\":\"doc-6\"}",
			"{\"bar\":7,\"baz\":\"and this\",\"foo\":\"doc-7\"}",
			"{\"bar\":8,\"baz\":\"and this\",\"foo\":\"doc-8\"}",
			"{\"bar\":9,\"baz\":\"and this\",\"foo\":\"doc-9\"}",
		}, outBatches)
	})
}

func testBatchInputOutputRaw(t *testing.T, driver, dsn, table string) {
	t.Run("raw_input_output", func(t *testing.T) {
		placeholderStr := "?"
		valuesStr := `(?, ?, ?)`
		switch driver {
		case "postgres", "pgx", "clickhouse":
			valuesStr = `($1, $2, $3)`
			placeholderStr = "$1"
		case "oracle":
			valuesStr = `(:1, :2, :3)`
			placeholderStr = ":1"
		}

		updateStr := "update"
		setStr := "set"
		if driver == "clickhouse" {
			updateStr = "alter table"
			setStr = "update"
		}

		confReplacer := strings.NewReplacer(
			"$driver", driver,
			"$dsn", dsn,
			"$table", table,
			"$update", updateStr,
			"$set", setStr,
		)

		updateStatement := confReplacer.Replace(`
    - query: $update $table $set "bar" = "bar" + 1 where "foo" = ` + placeholderStr + `
      args_mapping: 'root = [ this.foo ]'
`)

		// Trino doesn't support transactions, we make the test pass by doing this in blobl
		if driver == "trino" {
			updateStatement = `
processors:
  - mapping: |
      root = this
      root.bar = this.bar + 1
`
		}

		outputConf := confReplacer.Replace(`
sql_raw:
  driver: $driver
  dsn: $dsn
  queries:
    - query: insert into $table ("foo", "bar", "baz") values `+valuesStr+`
      args_mapping: 'root = [ this.foo, this.bar.floor(), this.baz ]'
`) + updateStatement

		inputConf := confReplacer.Replace(`
sql_raw:
  driver: $driver
  dsn: $dsn
  query: 'select "foo", "bar" - 1 as "bar", "baz" from $table ORDER BY "bar" ASC'
processors:
  # For some reason MySQL driver doesn't resolve to integer by default.
  - mapping: |
      root = this
      root.bar = this.bar.number()
`)

		streamInBuilder := service.NewStreamBuilder()
		require.NoError(t, streamInBuilder.SetLoggerYAML(`level: OFF`))
		require.NoError(t, streamInBuilder.AddOutputYAML(outputConf))

		inFn, err := streamInBuilder.AddBatchProducerFunc()
		require.NoError(t, err)

		streamIn, err := streamInBuilder.Build()
		require.NoError(t, err)

		go func() {
			assert.NoError(t, streamIn.Run(t.Context()))
		}()

		streamOutBuilder := service.NewStreamBuilder()
		require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: OFF`))
		require.NoError(t, streamOutBuilder.AddInputYAML(inputConf))

		var outBatches []string
		require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
			msgBytes, err := mb[0].AsBytes()
			require.NoError(t, err)
			outBatches = append(outBatches, string(msgBytes))
			return nil
		}))

		streamOut, err := streamOutBuilder.Build()
		require.NoError(t, err)

		var insertBatch service.MessageBatch
		for i := range 10 {
			insertBatch = append(insertBatch, service.NewMessage(fmt.Appendf(nil, `{
	"foo": "doc-%d",
	"bar": %d,
	"baz": "and this"
}`, i, i)))
		}
		require.NoError(t, inFn(t.Context(), insertBatch))
		require.NoError(t, streamIn.StopWithin(15*time.Second))

		require.NoError(t, streamOut.Run(t.Context()))

		assert.Equal(t, []string{
			"{\"bar\":0,\"baz\":\"and this\",\"foo\":\"doc-0\"}",
			"{\"bar\":1,\"baz\":\"and this\",\"foo\":\"doc-1\"}",
			"{\"bar\":2,\"baz\":\"and this\",\"foo\":\"doc-2\"}",
			"{\"bar\":3,\"baz\":\"and this\",\"foo\":\"doc-3\"}",
			"{\"bar\":4,\"baz\":\"and this\",\"foo\":\"doc-4\"}",
			"{\"bar\":5,\"baz\":\"and this\",\"foo\":\"doc-5\"}",
			"{\"bar\":6,\"baz\":\"and this\",\"foo\":\"doc-6\"}",
			"{\"bar\":7,\"baz\":\"and this\",\"foo\":\"doc-7\"}",
			"{\"bar\":8,\"baz\":\"and this\",\"foo\":\"doc-8\"}",
			"{\"bar\":9,\"baz\":\"and this\",\"foo\":\"doc-9\"}",
		}, outBatches)
	})
}

func testSuite(t *testing.T, driver, dsn string, createTableFn func(string) (string, error)) {
	for _, fn := range []testFn{
		testBatchProcessorBasic,
		testBatchProcessorParallel,
		testBatchInputOutputBatch,
		testBatchInputOutputRaw,
		testRawProcessorsBasic,
		testRawProcessorsTransactional,
		testDeprecatedProcessorsBasic,
	} {
		tableName, err := gonanoid.Generate("abcdefghijklmnopqrstuvwxyz", 40)
		require.NoError(t, err)

		tableName, err = createTableFn(tableName)
		require.NoError(t, err)

		fn(t, driver, dsn, tableName)
	}
}

func testClickhouseMapInsertOutput(t *testing.T, dsn string, db *sql.DB) {
	t.Run("sql_insert_map_types", func(t *testing.T) {
		tableName, err := gonanoid.Generate("abcdefghijklmnopqrstuvwxyz", 40)
		require.NoError(t, err)

		_, err = db.Exec(fmt.Sprintf(`create table %s (
  foo String,
  string_map Map(String, String),
  int_map Map(String, Int64),
  uint8_bool_map Map(UInt8, Bool),
  nested_map Map(String, Map(String, UInt16))
		) engine=Memory;`, tableName))
		require.NoError(t, err)

		outputConf := fmt.Sprintf(`
sql_insert:
  driver: clickhouse
  dsn: %s
  table: %s
  columns: [ foo, string_map, int_map, uint8_bool_map, nested_map ]
  args_mapping: |
    root = [
      this.foo,
      {
        "env": this.string_map.env,
        "region": this.string_map.region,
      },
      {
        "attempts": this.int_map.attempts.number().floor(),
        "retries": this.int_map.retries.number().floor(),
      },
      this.uint8_bool_map,
      this.nested_map,
    ]
`, dsn, tableName)

		streamBuilder := service.NewStreamBuilder()
		require.NoError(t, streamBuilder.SetLoggerYAML(`level: OFF`))
		require.NoError(t, streamBuilder.AddOutputYAML(outputConf))

		inFn, err := streamBuilder.AddBatchProducerFunc()
		require.NoError(t, err)

		stream, err := streamBuilder.Build()
		require.NoError(t, err)

		runErrChan := make(chan error, 1)
		go func() {
			runErrChan <- stream.Run(t.Context())
		}()

		batch := service.MessageBatch{service.NewMessage([]byte(`{
  "foo": "doc-1",
  "string_map": {
    "env": "prod",
    "region": "us-east-1"
  },
  "int_map": {
    "attempts": 7,
    "retries": 3
  },
  "uint8_bool_map": {
    "1": true,
    "2": false
  },
  "nested_map": {
    "outer": {
      "inner": 9
    }
  }
}`))}

		require.NoError(t, inFn(t.Context(), batch))
		require.NoError(t, stream.StopWithin(15*time.Second))
		require.NoError(t, <-runErrChan)

		var (
			foo, env, region  string
			attempts, retries int64
			one, two          bool
			inner             uint16
		)
		err = db.QueryRow(fmt.Sprintf(
			`select foo, string_map['env'], string_map['region'], int_map['attempts'], int_map['retries'], uint8_bool_map[1], uint8_bool_map[2], nested_map['outer']['inner'] from %s`,
			tableName,
		)).Scan(&foo, &env, &region, &attempts, &retries, &one, &two, &inner)
		require.NoError(t, err)

		assert.Equal(t, "doc-1", foo)
		assert.Equal(t, "prod", env)
		assert.Equal(t, "us-east-1", region)
		assert.Equal(t, int64(7), attempts)
		assert.Equal(t, int64(3), retries)
		assert.True(t, one)
		assert.False(t, two)
		assert.Equal(t, uint16(9), inner)
	})
}

func runClickhouseTest(t *testing.T, dsnScheme string) {
	t.Parallel()

	pwd, err := os.Getwd()
	require.NoError(t, err)

	ctr, err := testcontainers.Run(t.Context(), "clickhouse/clickhouse-server:latest",
		testcontainers.WithExposedPorts("9000/tcp"),
		testcontainers.WithEnv(map[string]string{
			"CLICKHOUSE_SKIP_USER_SETUP": "1",
		}),
		testcontainers.WithFiles(testcontainers.ContainerFile{
			HostFilePath:      pwd + "/resources/clickhouse/clickhouse.xml",
			ContainerFilePath: "/etc/clickhouse-server/users.d/clickhouse.xml",
			FileMode:          0o644,
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("9000/tcp").WithStartupTimeout(3*time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mappedPort, err := ctr.MappedPort(t.Context(), "9000/tcp")
	require.NoError(t, err)

	var db *sql.DB
	t.Cleanup(func() {
		if db != nil {
			db.Close()
		}
	})

	createTable := func(name string) (string, error) {
		_, err := db.Exec(fmt.Sprintf(`create table %s (
  "foo" String,
  "bar" Int64,
  "baz" String
		) engine=Memory;`, name))
		return name, err
	}

	dsn := fmt.Sprintf("%s://localhost:%s/", dsnScheme, mappedPort.Port())
	require.Eventually(t, func() bool {
		if db != nil {
			db.Close()
		}
		db, err = sql.Open("clickhouse", dsn)
		if err != nil {
			return false
		}
		if err = db.Ping(); err != nil {
			db.Close()
			db = nil
			return false
		}
		if _, err := createTable("footable"); err != nil {
			db.Close()
			db = nil
			return false
		}
		return true
	}, 3*time.Minute, time.Second)

	testSuite(t, "clickhouse", dsn, createTable)
	testClickhouseMapInsertOutput(t, dsn, db)
}

func TestIntegrationClickhouse(t *testing.T) {
	integration.CheckSkip(t)

	tests := []struct {
		name      string
		dsnScheme string
	}{
		{
			name:      "new DSN scheme",
			dsnScheme: "clickhouse",
		},
		{
			name:      "old DSN scheme",
			dsnScheme: "tcp",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runClickhouseTest(t, test.dsnScheme)
		})
	}
}

func TestIntegrationPostgres(t *testing.T) {
	integration.CheckSkip(t)

	ctr, err := testcontainers.Run(t.Context(), "postgres:latest",
		testcontainers.WithExposedPorts("5432/tcp"),
		testcontainers.WithEnv(map[string]string{
			"POSTGRES_USER":     "testuser",
			"POSTGRES_PASSWORD": "testpass",
			"POSTGRES_DB":       "testdb",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("5432/tcp").WithStartupTimeout(3*time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mp, err := ctr.MappedPort(t.Context(), "5432/tcp")
	require.NoError(t, err)
	dsn := fmt.Sprintf("postgres://testuser:testpass@localhost:%s/testdb?sslmode=disable", mp.Port())

	for _, driver := range []string{
		"postgres",
		"pgx",
	} {
		t.Run(fmt.Sprintf("driver %s", driver), func(t *testing.T) {
			var db *sql.DB
			t.Cleanup(func() {
				if db != nil {
					db.Close()
				}
			})

			createTable := func(name string) (string, error) {
				_, err := db.Exec(fmt.Sprintf(`create table %s (
	  "foo" varchar(50) not null,
	  "bar" integer not null,
	  "baz" varchar(50) not null,
	  primary key ("foo")
		)`, name))
				return name, err
			}

			require.Eventually(t, func() bool {
				conn, err := sql.Open(driver, dsn)
				if err != nil {
					return false
				}
				if err = conn.Ping(); err != nil {
					conn.Close()
					return false
				}
				db = conn
				tableName := fmt.Sprintf("footable_%s", driver)
				if _, err := createTable(tableName); err != nil {
					db.Close()
					db = nil
					return false
				}
				return true
			}, 3*time.Minute, time.Second)

			testSuite(t, driver, dsn, createTable)
		})
	}
}

func TestIntegrationPostgresVector(t *testing.T) {
	integration.CheckSkip(t)

	ctr, err := testcontainers.Run(t.Context(), "pgvector/pgvector:pg16",
		testcontainers.WithExposedPorts("5432/tcp"),
		testcontainers.WithEnv(map[string]string{
			"POSTGRES_USER":     "testuser",
			"POSTGRES_PASSWORD": "testpass",
			"POSTGRES_DB":       "testdb",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("5432/tcp").WithStartupTimeout(3*time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mp, err := ctr.MappedPort(t.Context(), "5432/tcp")
	require.NoError(t, err)
	dsn := fmt.Sprintf("postgres://testuser:testpass@localhost:%s/testdb?sslmode=disable", mp.Port())
	env := service.NewEnvironment()

	for _, driver := range []string{
		"postgres",
		"pgx",
	} {
		t.Run(fmt.Sprintf("driver %s", driver), func(t *testing.T) {
			var db *sql.DB
			t.Cleanup(func() {
				if db != nil {
					db.Close()
				}
			})

			require.Eventually(t, func() bool {
				conn, err := sql.Open(driver, dsn)
				if err != nil {
					return false
				}
				if err = conn.Ping(); err != nil {
					conn.Close()
					return false
				}
				if _, err := conn.Exec(`CREATE EXTENSION IF NOT EXISTS vector`); err != nil {
					conn.Close()
					return false
				}
				db = conn
				tableName := fmt.Sprintf("items_%s", driver)
				if _, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tableName)); err != nil {
					db.Close()
					db = nil
					return false
				}
				if _, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s (
	      foo text PRIMARY KEY,
	      embedding vector(3)
	    )`, tableName)); err != nil {
					db.Close()
					db = nil
					return false
				}
				return true
			}, 3*time.Minute, time.Second)

			tableName := fmt.Sprintf("items_%s", driver)
			insertConfig, err := isql.InsertProcessorConfig().ParseYAML(fmt.Sprintf(`
driver: %s
dsn: %s
table: %s
columns: ["foo", "embedding"]
args_mapping: 'root = [ this.foo, this.embedding.vector() ]'
`, driver, dsn, tableName), env)
			require.NoError(t, err)
			insertProc, err := isql.NewSQLInsertProcessorFromConfig(insertConfig, service.MockResources())
			require.NoError(t, err)
			t.Cleanup(func() { insertProc.Close(t.Context()) })

			insertBatch := service.MessageBatch{
				service.NewMessage([]byte(`{"foo": "blob","embedding": [4,5,6]}`)),
				service.NewMessage([]byte(`{"foo": "fish","embedding": [1,2,3]}`)),
			}

			resBatches, err := insertProc.ProcessBatch(t.Context(), insertBatch)
			require.NoError(t, err)
			require.Len(t, resBatches, 1)
			require.Len(t, resBatches[0], len(insertBatch))
			for _, v := range resBatches[0] {
				require.NoError(t, v.GetError())
			}

			queryConf := fmt.Sprintf(`
driver: %s
dsn: %s
table: %s
columns: [ "foo" ]
suffix: ORDER BY embedding <-> '[3,1,2]' LIMIT 1
`, driver, dsn, tableName)

			selectConfig, err := isql.SelectProcessorConfig().ParseYAML(queryConf, env)
			require.NoError(t, err)

			selectProc, err := isql.NewSQLSelectProcessorFromConfig(selectConfig, service.MockResources())
			require.NoError(t, err)
			t.Cleanup(func() { selectProc.Close(t.Context()) })

			queryBatch := service.MessageBatch{service.NewMessage([]byte(`{}`))}
			resBatches, err = selectProc.ProcessBatch(t.Context(), queryBatch)
			require.NoError(t, err)
			require.Len(t, resBatches, 1)
			require.Len(t, resBatches[0], 1)
			m := resBatches[0][0]
			require.NoError(t, m.GetError())
			actBytes, err := m.AsBytes()
			require.NoError(t, err)
			assert.JSONEq(t, `[{"foo":"fish"}]`, string(actBytes))
		})
	}
}

func TestIntegrationMySQL(t *testing.T) {
	integration.CheckSkip(t)

	ctr, err := testcontainers.Run(t.Context(), "mysql:latest",
		testcontainers.WithExposedPorts("3306/tcp"),
		testcontainers.WithCmd("--sql_mode=ANSI_QUOTES"),
		testcontainers.WithEnv(map[string]string{
			"MYSQL_USER":                 "testuser",
			"MYSQL_PASSWORD":             "testpass",
			"MYSQL_DATABASE":             "testdb",
			"MYSQL_RANDOM_ROOT_PASSWORD": "yes",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("3306/tcp").WithStartupTimeout(3*time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mp, err := ctr.MappedPort(t.Context(), "3306/tcp")
	require.NoError(t, err)

	var db *sql.DB
	t.Cleanup(func() {
		if db != nil {
			db.Close()
		}
	})

	createTable := func(name string) (string, error) {
		_, err := db.Exec(fmt.Sprintf(`create table %s (
  "foo" varchar(50) not null,
  "bar" integer not null,
  "baz" varchar(50) not null,
  primary key ("foo")
		)`, name))
		return name, err
	}

	dsn := fmt.Sprintf("testuser:testpass@tcp(localhost:%s)/testdb", mp.Port())
	require.Eventually(t, func() bool {
		if db != nil {
			db.Close()
		}
		if db, err = sql.Open("mysql", dsn); err != nil {
			return false
		}
		if err = db.Ping(); err != nil {
			db.Close()
			db = nil
			return false
		}
		if _, err := createTable("footable"); err != nil {
			db.Close()
			db = nil
			return false
		}
		return true
	}, 3*time.Minute, time.Second)

	testSuite(t, "mysql", dsn, createTable)
}

func TestIntegrationMSSQL(t *testing.T) {
	integration.CheckSkip(t)

	testPassword := "ins4n3lyStrongP4ssword"
	ctr, err := testcontainers.Run(t.Context(), "mcr.microsoft.com/mssql/server:2025-latest",
		testcontainers.WithImagePlatform("linux/amd64"),
		testcontainers.WithExposedPorts("1433/tcp"),
		testcontainers.WithEnv(map[string]string{
			"ACCEPT_EULA": "Y",
			"SA_PASSWORD": testPassword,
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("1433/tcp").WithStartupTimeout(3*time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mp, err := ctr.MappedPort(t.Context(), "1433/tcp")
	require.NoError(t, err)

	var db *sql.DB
	t.Cleanup(func() {
		if db != nil {
			db.Close()
		}
	})

	createTable := func(name string) (string, error) {
		_, err := db.Exec(fmt.Sprintf(`create table %s (
  "foo" varchar(50) not null,
  "bar" integer not null,
  "baz" varchar(50) not null,
  primary key ("foo")
		)`, name))
		return name, err
	}

	dsn := fmt.Sprintf("sqlserver://sa:"+testPassword+"@localhost:%s?database=master", mp.Port())
	require.Eventually(t, func() bool {
		if db != nil {
			db.Close()
		}
		db, err = sql.Open("mssql", dsn)
		if err != nil {
			return false
		}
		if err = db.Ping(); err != nil {
			db.Close()
			db = nil
			return false
		}
		if _, err := createTable("footable"); err != nil {
			db.Close()
			db = nil
			return false
		}
		return true
	}, 3*time.Minute, time.Second)

	testSuite(t, "mssql", dsn, createTable)
}

func TestIntegrationSQLite(t *testing.T) {
	integration.CheckSkip(t)

	var db *sql.DB
	var err error
	t.Cleanup(func() {
		if db != nil {
			db.Close()
		}
	})

	createTable := func(name string) (string, error) {
		_, err := db.Exec(fmt.Sprintf(`create table %s (
  "foo" varchar(50) not null,
  "bar" integer not null,
  "baz" varchar(50) not null,
  primary key ("foo")
		)`, name))
		return name, err
	}

	dsn := "file::memory:?cache=shared"

	require.NoError(t, func() error {
		db, err = sql.Open("sqlite", dsn)
		if err != nil {
			return err
		}
		if err = db.Ping(); err != nil {
			db.Close()
			db = nil
			return err
		}
		if _, err := createTable("footable"); err != nil {
			return err
		}
		return nil
	}())

	testSuite(t, "sqlite", dsn, createTable)
}

func TestIntegrationOracle(t *testing.T) {
	integration.CheckSkip(t)

	ctr, err := testcontainers.Run(t.Context(), "gvenzl/oracle-free:slim-faststart",
		testcontainers.WithExposedPorts("1521/tcp"),
		testcontainers.WithEnv(map[string]string{
			"ORACLE_PASSWORD": "testpass",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("1521/tcp").WithStartupTimeout(3*time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mp, err := ctr.MappedPort(t.Context(), "1521/tcp")
	require.NoError(t, err)

	var db *sql.DB
	t.Cleanup(func() {
		if db != nil {
			db.Close()
		}
	})

	createTable := func(name string) (string, error) {
		// We use a binary float column because the integer type in Oracle
		// can be larger than 64 bits so it is returned by the driver as a string.
		// Using a float type allows the type to be returned to be a number in blobl
		// which means the type is the same as other databases and the test passes.
		_, err := db.Exec(fmt.Sprintf(`create table %s (
  "foo" varchar(50) not null,
  "bar" binary_float not null,
  "baz" varchar(50) not null,
  primary key ("foo")
		)`, name))
		return name, err
	}

	dsn := fmt.Sprintf("oracle://system:testpass@localhost:%s/FREEPDB1", mp.Port())
	require.Eventually(t, func() bool {
		if db != nil {
			db.Close()
		}
		db, err = sql.Open("oracle", dsn)
		if err != nil {
			return false
		}

		if err = db.Ping(); err != nil {
			db.Close()
			db = nil
			return false
		}

		if _, err := createTable("footable"); err != nil {
			db.Close()
			db = nil
			return false
		}
		return true
	}, 3*time.Minute, time.Second)

	testSuite(t, "oracle", dsn, createTable)
}

func TestIntegrationTrino(t *testing.T) {
	integration.CheckSkip(t)

	testPassword := ""
	ctr, err := testcontainers.Run(t.Context(), "trinodb/trino:latest",
		testcontainers.WithExposedPorts("8080/tcp"),
		testcontainers.WithEnv(map[string]string{
			"PASSWORD": testPassword,
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("8080/tcp").WithStartupTimeout(3*time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mp, err := ctr.MappedPort(t.Context(), "8080/tcp")
	require.NoError(t, err)

	var db *sql.DB
	t.Cleanup(func() {
		if db != nil {
			db.Close()
		}
	})

	createTable := func(name string) (string, error) {
		name = "memory.default." + name
		_, err := db.Exec(fmt.Sprintf(`
create table %s (
  "foo" varchar,
  "bar" integer,
  "baz" varchar
)`, name))
		return name, err
	}

	dsn := fmt.Sprintf("http://trinouser:"+testPassword+"@localhost:%s", mp.Port())
	require.Eventually(t, func() bool {
		if db != nil {
			db.Close()
		}
		db, err = sql.Open("trino", dsn)
		if err != nil {
			return false
		}
		if err = db.Ping(); err != nil {
			db.Close()
			db = nil
			return false
		}
		if _, err := createTable("test"); err != nil {
			db.Close()
			db = nil
			return false
		}
		return true
	}, 3*time.Minute, time.Second)

	testSuite(t, "trino", dsn, createTable)
}

func TestIntegrationCosmosDB(t *testing.T) {
	integration.CheckSkip(t)

	ctr, err := testcontainers.Run(t.Context(), "mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:latest",
		testcontainers.WithImagePlatform("linux/amd64"),
		testcontainers.WithExposedPorts("8081/tcp"),
		testcontainers.WithEnv(map[string]string{
			// The bigger the value, the longer it takes for the container to start up.
			"AZURE_COSMOS_EMULATOR_PARTITION_COUNT":         "2",
			"AZURE_COSMOS_EMULATOR_ENABLE_DATA_PERSISTENCE": "false",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("8081/tcp").WithStartupTimeout(3*time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mp, err := ctr.MappedPort(t.Context(), "8081/tcp")
	require.NoError(t, err)

	var db *sql.DB
	t.Cleanup(func() {
		if db != nil {
			db.Close()
		}
	})

	createContainer := func(name string) (string, error) {
		_, err := db.Exec(fmt.Sprintf(`create collection %s with pk=/foo`, name))
		return name, err
	}

	dummyDatabase := "PacificOcean"
	dummyContainer := "ChallengerDeep"
	emulatorAccountKey := "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
	dsn := fmt.Sprintf(
		"AccountEndpoint=https://localhost:%s;AccountKey=%s;DefaultDb=%s;AutoId=true;InsecureSkipVerify=true",
		mp.Port(), emulatorAccountKey, dummyDatabase,
	)

	require.Eventually(t, func() bool {
		if db != nil {
			db.Close()
		}
		db, err = sql.Open("gocosmos", dsn)
		if err != nil {
			return false
		}
		if err = db.Ping(); err != nil {
			db.Close()
			db = nil
			return false
		}
		if _, err := db.Exec(fmt.Sprintf(`create database %s`, dummyDatabase)); err != nil {
			db.Close()
			db = nil
			return false
		}
		if _, err := createContainer(dummyContainer); err != nil {
			db.Close()
			db = nil
			return false
		}
		return true
	}, 3*time.Minute, time.Second)

	// TODO: Enable the full test suite once https://github.com/microsoft/gocosmos/issues/15 is addressed and increase
	// increase `AZURE_COSMOS_EMULATOR_PARTITION_COUNT` so the emulator can create all the required containers. Note
	// that select queries must prefix the column names with the container name (i.e `test.foo`) and, also `select *`
	// will return the autogenerated `id` column, which will break the naive diff when asserting the results.
	// testSuite(t, "gocosmos", dsn, createContainer)

	insertConf := fmt.Sprintf(`
driver: gocosmos
dsn: %s
table: %s
columns:
  - foo
  - bar
  - baz
args_mapping: 'root = [ this.foo, this.bar.uppercase(), this.baz ]'
`, dsn, dummyContainer)

	queryConf := fmt.Sprintf(`
driver: gocosmos
dsn: %s
table: %s
columns:
  - %s.foo
  - %s.bar
  - %s.baz
where: '%s.foo = ?'
args_mapping: 'root = [ this.foo ]'
`, dsn, dummyContainer, dummyContainer, dummyContainer, dummyContainer, dummyContainer)

	env := service.NewEnvironment()

	insertConfig, err := isql.InsertProcessorConfig().ParseYAML(insertConf, env)
	require.NoError(t, err)

	selectConfig, err := isql.SelectProcessorConfig().ParseYAML(queryConf, env)
	require.NoError(t, err)

	insertProc, err := isql.NewSQLInsertProcessorFromConfig(insertConfig, service.MockResources())
	require.NoError(t, err)
	t.Cleanup(func() { insertProc.Close(t.Context()) })

	selectProc, err := isql.NewSQLSelectProcessorFromConfig(selectConfig, service.MockResources())
	require.NoError(t, err)
	t.Cleanup(func() { selectProc.Close(t.Context()) })

	insertBatch := service.MessageBatch{service.NewMessage([]byte(`{
  "foo": "blobfish",
  "bar": "are really cool",
  "baz": 41
}`))}

	resBatches, err := insertProc.ProcessBatch(t.Context(), insertBatch)
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], len(insertBatch))
	for _, v := range resBatches[0] {
		require.NoError(t, v.GetError())
	}

	queryBatch := service.MessageBatch{service.NewMessage([]byte(`{"foo":"blobfish"}`))}

	resBatches, err = selectProc.ProcessBatch(t.Context(), queryBatch)
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], 1)
	m := resBatches[0][0]
	require.NoError(t, m.GetError())
	actBytes, err := m.AsBytes()
	require.NoError(t, err)
	assert.JSONEq(t, `[{"foo": "blobfish", "bar": "ARE REALLY COOL", "baz": 41}]`, string(actBytes))
}
