package sql_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	isql "github.com/benthosdev/benthos/v4/internal/impl/sql"
	"github.com/benthosdev/benthos/v4/internal/integration"
	"github.com/benthosdev/benthos/v4/public/service"

	_ "github.com/benthosdev/benthos/v4/public/components/pure"
	_ "github.com/benthosdev/benthos/v4/public/components/sql"
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
			t.Cleanup(func() { insertProc.Close(context.Background()) })

			selectProc, err := isql.NewSQLSelectProcessorFromConfig(selectConfig, service.MockResources())
			require.NoError(t, err)
			t.Cleanup(func() { selectProc.Close(context.Background()) })

			fn(t, insertProc, selectProc)
		})
	}
}

func testRawProcessors(name string, fn func(t *testing.T, insertProc, selectProc service.BatchProcessor)) testFn {
	return func(t *testing.T, driver, dsn, table string) {
		t.Run(name, func(t *testing.T) {
			valuesStr := `(?, ?, ?)`
			if driver == "postgres" || driver == "clickhouse" {
				valuesStr = `($1, $2, $3)`
			} else if driver == "oracle" {
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
			if driver == "postgres" || driver == "clickhouse" {
				placeholderStr = "$1"
			} else if driver == "oracle" {
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
			t.Cleanup(func() { insertProc.Close(context.Background()) })

			selectProc, err := isql.NewSQLRawProcessorFromConfig(selectConfig, service.MockResources())
			require.NoError(t, err)
			t.Cleanup(func() { selectProc.Close(context.Background()) })

			fn(t, insertProc, selectProc)
		})
	}
}

func testRawDeprecatedProcessors(name string, fn func(t *testing.T, insertProc, selectProc service.BatchProcessor)) testFn {
	return func(t *testing.T, driver, dsn, table string) {
		t.Run(name, func(t *testing.T) {
			valuesStr := `(?, ?, ?)`
			if driver == "postgres" || driver == "clickhouse" {
				valuesStr = `($1, $2, $3)`
			} else if driver == "oracle" {
				valuesStr = `(:1, :2, :3)`
			}
			insertConf := fmt.Sprintf(`
driver: %s
data_source_name: %s
query: insert into %s ( "foo", "bar", "baz" ) values `+valuesStr+`
args_mapping: 'root = [ this.foo, this.bar.floor(), this.baz ]'
`, driver, dsn, table)

			placeholderStr := "?"
			if driver == "postgres" || driver == "clickhouse" {
				placeholderStr = "$1"
			} else if driver == "oracle" {
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
			t.Cleanup(func() { insertProc.Close(context.Background()) })

			selectProc, err := isql.NewSQLDeprecatedProcessorFromConfig(selectConfig, service.MockResources())
			require.NoError(t, err)
			t.Cleanup(func() { selectProc.Close(context.Background()) })

			fn(t, insertProc, selectProc)
		})
	}
}

var testBatchProcessorBasic = testProcessors("basic", func(t *testing.T, insertProc, selectProc service.BatchProcessor) {
	var insertBatch service.MessageBatch
	for i := 0; i < 10; i++ {
		insertBatch = append(insertBatch, service.NewMessage([]byte(fmt.Sprintf(`{
  "foo": "doc-%d",
  "bar": %d,
  "baz": "and this"
}`, i, i))))
	}

	resBatches, err := insertProc.ProcessBatch(context.Background(), insertBatch)
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], len(insertBatch))
	for _, v := range resBatches[0] {
		require.NoError(t, v.GetError())
	}

	var queryBatch service.MessageBatch
	for i := 0; i < 10; i++ {
		queryBatch = append(queryBatch, service.NewMessage([]byte(fmt.Sprintf(`{"id":"doc-%d"}`, i))))
	}

	resBatches, err = selectProc.ProcessBatch(context.Background(), queryBatch)
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
	for i := 0; i < nParallel; i++ {
		var insertBatch service.MessageBatch
		for j := 0; j < nLoops; j++ {
			index := i*nLoops + j
			insertBatch = append(insertBatch, service.NewMessage([]byte(fmt.Sprintf(`{
  "foo": "doc-%d",
  "bar": %d,
  "baz": "and this"
}`, index, index))))
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startChan
			for _, msg := range insertBatch {
				_, err := insertProc.ProcessBatch(context.Background(), service.MessageBatch{msg})
				require.NoError(t, err)
			}
		}()
	}

	close(startChan)
	wg.Wait()

	startChan = make(chan struct{})
	wg = sync.WaitGroup{}
	for i := 0; i < nParallel; i++ {
		var queryBatch service.MessageBatch

		for j := 0; j < nLoops; j++ {
			index := i*nLoops + j
			queryBatch = append(queryBatch, service.NewMessage([]byte(fmt.Sprintf(`{"id":"doc-%d"}`, index))))
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startChan
			for _, msg := range queryBatch {
				resBatches, err := selectProc.ProcessBatch(context.Background(), service.MessageBatch{msg})
				require.NoError(t, err)
				require.Len(t, resBatches, 1)
				require.Len(t, resBatches[0], 1)
				require.NoError(t, resBatches[0][0].GetError())
			}
		}()
	}

	close(startChan)
	wg.Wait()
})

var testRawProcessorsBasic = testRawProcessors("raw", func(t *testing.T, insertProc, selectProc service.BatchProcessor) {
	var insertBatch service.MessageBatch
	for i := 0; i < 10; i++ {
		insertBatch = append(insertBatch, service.NewMessage([]byte(fmt.Sprintf(`{
  "foo": "doc-%d",
  "bar": %d,
  "baz": "and this"
}`, i, i))))
	}

	resBatches, err := insertProc.ProcessBatch(context.Background(), insertBatch)
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], len(insertBatch))
	for _, v := range resBatches[0] {
		require.NoError(t, v.GetError())
	}

	var queryBatch service.MessageBatch
	for i := 0; i < 10; i++ {
		queryBatch = append(queryBatch, service.NewMessage([]byte(fmt.Sprintf(`{"id":"doc-%d"}`, i))))
	}

	resBatches, err = selectProc.ProcessBatch(context.Background(), queryBatch)
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

var testDeprecatedProcessorsBasic = testRawDeprecatedProcessors("deprecated", func(t *testing.T, insertProc, selectProc service.BatchProcessor) {
	var insertBatch service.MessageBatch
	for i := 0; i < 10; i++ {
		insertBatch = append(insertBatch, service.NewMessage([]byte(fmt.Sprintf(`{
  "foo": "doc-%d",
  "bar": %d,
  "baz": "and this"
}`, i, i))))
	}

	resBatches, err := insertProc.ProcessBatch(context.Background(), insertBatch)
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], len(insertBatch))
	for _, v := range resBatches[0] {
		require.NoError(t, v.GetError())
	}

	var queryBatch service.MessageBatch
	for i := 0; i < 10; i++ {
		queryBatch = append(queryBatch, service.NewMessage([]byte(fmt.Sprintf(`{"id":"doc-%d"}`, i))))
	}

	resBatches, err = selectProc.ProcessBatch(context.Background(), queryBatch)
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
			assert.NoError(t, streamIn.Run(context.Background()))
		}()

		streamOutBuilder := service.NewStreamBuilder()
		require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: OFF`))
		require.NoError(t, streamOutBuilder.AddInputYAML(inputConf))

		var outBatches []string
		require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(c context.Context, mb service.MessageBatch) error {
			msgBytes, err := mb[0].AsBytes()
			require.NoError(t, err)
			outBatches = append(outBatches, string(msgBytes))
			return nil
		}))

		streamOut, err := streamOutBuilder.Build()
		require.NoError(t, err)

		var insertBatch service.MessageBatch
		for i := 0; i < 10; i++ {
			insertBatch = append(insertBatch, service.NewMessage([]byte(fmt.Sprintf(`{
	"foo": "doc-%d",
	"bar": %d,
	"baz": "and this"
}`, i, i))))
		}
		require.NoError(t, inFn(context.Background(), insertBatch))
		require.NoError(t, streamIn.StopWithin(15*time.Second))

		require.NoError(t, streamOut.Run(context.Background()))

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
		confReplacer := strings.NewReplacer(
			"$driver", driver,
			"$dsn", dsn,
			"$table", table,
		)

		valuesStr := `(?, ?, ?)`
		if driver == "postgres" || driver == "clickhouse" {
			valuesStr = `($1, $2, $3)`
		} else if driver == "oracle" {
			valuesStr = `(:1, :2, :3)`
		}

		outputConf := confReplacer.Replace(`
sql_raw:
  driver: $driver
  dsn: $dsn
  query: insert into $table ("foo", "bar", "baz") values ` + valuesStr + `
  args_mapping: 'root = [ this.foo, this.bar.floor(), this.baz ]'
`)

		inputConf := confReplacer.Replace(`
sql_raw:
  driver: $driver
  dsn: $dsn
  query: 'select * from $table ORDER BY "bar" ASC'
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
			assert.NoError(t, streamIn.Run(context.Background()))
		}()

		streamOutBuilder := service.NewStreamBuilder()
		require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: OFF`))
		require.NoError(t, streamOutBuilder.AddInputYAML(inputConf))

		var outBatches []string
		require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(c context.Context, mb service.MessageBatch) error {
			msgBytes, err := mb[0].AsBytes()
			require.NoError(t, err)
			outBatches = append(outBatches, string(msgBytes))
			return nil
		}))

		streamOut, err := streamOutBuilder.Build()
		require.NoError(t, err)

		var insertBatch service.MessageBatch
		for i := 0; i < 10; i++ {
			insertBatch = append(insertBatch, service.NewMessage([]byte(fmt.Sprintf(`{
	"foo": "doc-%d",
	"bar": %d,
	"baz": "and this"
}`, i, i))))
		}
		require.NoError(t, inFn(context.Background(), insertBatch))
		require.NoError(t, streamIn.StopWithin(15*time.Second))

		require.NoError(t, streamOut.Run(context.Background()))

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
		testDeprecatedProcessorsBasic,
	} {
		tableName, err := gonanoid.Generate("abcdefghijklmnopqrstuvwxyz", 40)
		require.NoError(t, err)

		tableName, err = createTableFn(tableName)
		require.NoError(t, err)

		fn(t, driver, dsn, tableName)
	}
}

func TestIntegrationClickhouse(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = 3 * time.Minute

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "clickhouse/clickhouse-server",
		ExposedPorts: []string{"9000/tcp"},
	})
	require.NoError(t, err)

	var db *sql.DB
	t.Cleanup(func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %s", err)
		}
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

	dsn := fmt.Sprintf("clickhouse://localhost:%s/", resource.GetPort("9000/tcp"))
	require.NoError(t, pool.Retry(func() error {
		db, err = sql.Open("clickhouse", dsn)
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
	}))

	testSuite(t, "clickhouse", dsn, createTable)
}

func TestIntegrationOldClickhouse(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = 3 * time.Minute

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "clickhouse/clickhouse-server",
		ExposedPorts: []string{"9000/tcp"},
	})
	require.NoError(t, err)

	var db *sql.DB
	t.Cleanup(func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %s", err)
		}
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

	dsn := fmt.Sprintf("tcp://localhost:%s/", resource.GetPort("9000/tcp"))
	require.NoError(t, pool.Retry(func() error {
		db, err = sql.Open("clickhouse", dsn)
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
	}))

	testSuite(t, "clickhouse", dsn, createTable)
}

func TestIntegrationPostgres(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = 3 * time.Minute

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "postgres",
		ExposedPorts: []string{"5432/tcp"},
		Env: []string{
			"POSTGRES_USER=testuser",
			"POSTGRES_PASSWORD=testpass",
			"POSTGRES_DB=testdb",
		},
	})
	require.NoError(t, err)

	var db *sql.DB
	t.Cleanup(func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %s", err)
		}
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

	dsn := fmt.Sprintf("postgres://testuser:testpass@localhost:%s/testdb?sslmode=disable", resource.GetPort("5432/tcp"))
	require.NoError(t, pool.Retry(func() error {
		db, err = sql.Open("postgres", dsn)
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
	}))

	testSuite(t, "postgres", dsn, createTable)
}

func TestIntegrationMySQL(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = 3 * time.Minute

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "mysql",
		ExposedPorts: []string{"3306/tcp"},
		Cmd: []string{
			"--sql_mode=ANSI_QUOTES",
		},
		Env: []string{
			"MYSQL_USER=testuser",
			"MYSQL_PASSWORD=testpass",
			"MYSQL_DATABASE=testdb",
			"MYSQL_RANDOM_ROOT_PASSWORD=yes",
		},
	})
	require.NoError(t, err)

	var db *sql.DB
	t.Cleanup(func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %s", err)
		}
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

	dsn := fmt.Sprintf("testuser:testpass@tcp(localhost:%s)/testdb", resource.GetPort("3306/tcp"))
	require.NoError(t, pool.Retry(func() error {
		if db, err = sql.Open("mysql", dsn); err != nil {
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
	}))

	testSuite(t, "mysql", dsn, createTable)
}

func TestIntegrationMSSQL(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = 3 * time.Minute

	testPassword := "ins4n3lyStrongP4ssword"
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "mcr.microsoft.com/mssql/server",
		ExposedPorts: []string{"1433/tcp"},
		Env: []string{
			"ACCEPT_EULA=Y",
			"SA_PASSWORD=" + testPassword,
		},
	})
	require.NoError(t, err)

	var db *sql.DB
	t.Cleanup(func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %s", err)
		}
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

	dsn := fmt.Sprintf("sqlserver://sa:"+testPassword+"@localhost:%s?database=master", resource.GetPort("1433/tcp"))
	require.NoError(t, pool.Retry(func() error {
		db, err = sql.Open("mssql", dsn)
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
	}))

	testSuite(t, "mssql", dsn, createTable)
}

func TestIntegrationSQLite(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

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
	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = 3 * time.Minute

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "gvenzl/oracle-free",
		Tag:          "slim-faststart",
		ExposedPorts: []string{"1521/tcp"},
		Env: []string{
			"ORACLE_PASSWORD=testpass",
		},
	})
	require.NoError(t, err)

	var db *sql.DB
	t.Cleanup(func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %s", err)
		}
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

	dsn := fmt.Sprintf("oracle://system:testpass@localhost:%s/FREEPDB1", resource.GetPort("1521/tcp"))
	require.NoError(t, pool.Retry(func() error {
		db, err = sql.Open("oracle", dsn)
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
	}))

	testSuite(t, "oracle", dsn, createTable)
}

func TestIntegrationTrino(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = 3 * time.Minute

	testPassword := ""
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "trinodb/trino",
		ExposedPorts: []string{"8080/tcp"},
		Env: []string{
			"PASSWORD=" + testPassword,
		},
	})
	require.NoError(t, err)

	var db *sql.DB
	t.Cleanup(func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %s", err)
		}
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

	dsn := fmt.Sprintf("http://trinouser:"+testPassword+"@localhost:%s", resource.GetPort("8080/tcp"))
	require.NoError(t, pool.Retry(func() error {
		db, err = sql.Open("trino", dsn)
		if err != nil {
			return err
		}
		if err = db.Ping(); err != nil {
			db.Close()
			db = nil
			return err
		}
		if _, err := createTable("test"); err != nil {
			return err
		}
		return nil
	}))

	testSuite(t, "trino", dsn, createTable)
}

func TestIntegrationCosmosDB(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = 3 * time.Minute

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator",
		Tag:        "latest",
		Env: []string{
			// The bigger the value, the longer it takes for the container to start up.
			"AZURE_COSMOS_EMULATOR_PARTITION_COUNT=2",
			"AZURE_COSMOS_EMULATOR_ENABLE_DATA_PERSISTENCE=false",
		},
		ExposedPorts: []string{"8081/tcp"},
	})
	require.NoError(t, err)

	_ = resource.Expire(900)

	var db *sql.DB
	t.Cleanup(func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %s", err)
		}
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
		resource.GetPort("8081/tcp"), emulatorAccountKey, dummyDatabase,
	)

	require.NoError(t, pool.Retry(func() error {
		db, err = sql.Open("gocosmos", dsn)
		if err != nil {
			return err
		}
		if err = db.Ping(); err != nil {
			db.Close()
			db = nil
			return err
		}
		if _, err := db.Exec(fmt.Sprintf(`create database %s`, dummyDatabase)); err != nil {
			return err
		}
		if _, err := createContainer(dummyContainer); err != nil {
			return err
		}
		return nil
	}))

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
	t.Cleanup(func() { insertProc.Close(context.Background()) })

	selectProc, err := isql.NewSQLSelectProcessorFromConfig(selectConfig, service.MockResources())
	require.NoError(t, err)
	t.Cleanup(func() { selectProc.Close(context.Background()) })

	insertBatch := service.MessageBatch{service.NewMessage([]byte(`{
  "foo": "blobfish",
  "bar": "are really cool",
  "baz": 41
}`))}

	resBatches, err := insertProc.ProcessBatch(context.Background(), insertBatch)
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], len(insertBatch))
	for _, v := range resBatches[0] {
		require.NoError(t, v.GetError())
	}

	queryBatch := service.MessageBatch{service.NewMessage([]byte(`{"foo":"blobfish"}`))}

	resBatches, err = selectProc.ProcessBatch(context.Background(), queryBatch)
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], 1)
	m := resBatches[0][0]
	require.NoError(t, m.GetError())
	actBytes, err := m.AsBytes()
	require.NoError(t, err)
	assert.JSONEq(t, `[{"foo": "blobfish", "bar": "ARE REALLY COOL", "baz": 41}]`, string(actBytes))
}
