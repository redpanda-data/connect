package sql_test

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	isql "github.com/Jeffail/benthos/v3/internal/impl/sql"
	"github.com/Jeffail/benthos/v3/public/service"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/Jeffail/benthos/v3/public/components/all"
)

type testFn func(t *testing.T, driver, dsn, table string)

func testProcessors(name string, fn func(t *testing.T, insertProc, selectProc service.BatchProcessor)) testFn {
	return func(t *testing.T, driver, dsn, table string) {
		t.Run(name, func(t *testing.T) {
			insertConf := fmt.Sprintf(`
driver: %v
dsn: %v
table: %v
columns: [ foo, bar, baz ]
args_mapping: 'root = [ this.foo, this.bar.floor(), this.baz ]'
`, driver, dsn, table)

			queryConf := fmt.Sprintf(`
driver: %v
dsn: %v
table: %v
columns: [ foo, bar, baz ]
where: foo = ?
args_mapping: 'root = [ this.id ]'
`, driver, dsn, table)

			env := service.NewEnvironment()

			insertConfig, err := isql.InsertProcessorConfig().ParseYAML(insertConf, env)
			require.NoError(t, err)

			selectConfig, err := isql.SelectProcessorConfig().ParseYAML(queryConf, env)
			require.NoError(t, err)

			insertProc, err := isql.NewSQLInsertProcessorFromConfig(insertConfig, nil)
			require.NoError(t, err)
			t.Cleanup(func() { insertProc.Close(context.Background()) })

			selectProc, err := isql.NewSQLSelectProcessorFromConfig(selectConfig, nil)
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
  "foo": "doc-%v",
  "bar": %v,
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
		queryBatch = append(queryBatch, service.NewMessage([]byte(fmt.Sprintf(`{"id":"doc-%v"}`, i))))
	}

	resBatches, err = selectProc.ProcessBatch(context.Background(), queryBatch)
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], len(queryBatch))
	for i, v := range resBatches[0] {
		require.NoError(t, v.GetError())

		exp := fmt.Sprintf(`[{"bar":%v,"baz":"and this","foo":"doc-%v"}]`, i, i)
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
  "foo": "doc-%v",
  "bar": %v,
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
			queryBatch = append(queryBatch, service.NewMessage([]byte(fmt.Sprintf(`{"id":"doc-%v"}`, index))))
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

func testBatchInputOutputBatch(t *testing.T, driver, dsn, table string) {
	t.Run("batch_input_output", func(t *testing.T) {
		confReplacer := strings.NewReplacer(
			"$driver", driver,
			"$dsn", dsn,
			"$table", table,
		)

		outputConf := confReplacer.Replace(`
sql_insert:
  driver: $driver
  dsn: $dsn
  table: $table
  columns: [ foo, bar, baz ]
  args_mapping: 'root = [ this.foo, this.bar.floor(), this.baz ]'
`)

		inputConf := confReplacer.Replace(`
sql_select:
  driver: $driver
  dsn: $dsn
  table: $table
  columns: [ "*" ]
  suffix: ' ORDER BY bar ASC'
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
	"foo": "doc-%v",
	"bar": %v,
	"baz": "and this"
}`, i, i))))
		}
		require.NoError(t, inFn(context.Background(), insertBatch))
		require.NoError(t, streamIn.StopWithin(time.Second))

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

func testSuite(t *testing.T, driver, dsn string, createTableFn func(string) error) {
	for _, fn := range []testFn{
		testBatchProcessorBasic,
		testBatchProcessorParallel,
		testBatchInputOutputBatch,
	} {
		tableName, err := gonanoid.Generate("abcdefghijklmnopqrstuvwxyz", 40)
		require.NoError(t, err)

		require.NoError(t, createTableFn(tableName), tableName)
		fn(t, driver, dsn, tableName)
	}
}

func TestIntegration(t *testing.T) {
	if m := flag.Lookup("test.run").Value.String(); m == "" || regexp.MustCompile(strings.Split(m, "/")[0]).FindString(t.Name()) == "" {
		t.Skip("Skipping as execution was not requested explicitly using go test -run ^TestIntegration$")
	}

	t.Run("clickhouse", clickhouseIntegration)
	t.Run("postgres", postgresIntegration)
	t.Run("mysql", mySQLIntegration)
	t.Run("mssql", msSQLIntegration)
}

func clickhouseIntegration(t *testing.T) {
	if runtime.GOOS == "darwin" {
		t.Skip("skipping test on macos")
	}

	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = 30 * time.Second

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "yandex/clickhouse-server",
		ExposedPorts: []string{"9000/tcp"},
	})
	require.NoError(t, err)

	var db *sql.DB
	t.Cleanup(func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
		if db != nil {
			db.Close()
		}
	})

	createTable := func(name string) error {
		_, err := db.Exec(fmt.Sprintf(`create table %v (
  foo String,
  bar Int64,
  baz String
) engine=Memory;`, name))
		return err
	}

	dsn := fmt.Sprintf("tcp://localhost:%v/", resource.GetPort("9000/tcp"))
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
		if err := createTable("footable"); err != nil {
			return err
		}
		return nil
	}))

	testSuite(t, "clickhouse", dsn, createTable)
}

func postgresIntegration(t *testing.T) {
	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = 30 * time.Second

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
			t.Logf("Failed to clean up docker resource: %v", err)
		}
		if db != nil {
			db.Close()
		}
	})

	createTable := func(name string) error {
		_, err := db.Exec(fmt.Sprintf(`create table %v (
  foo varchar(50) not null,
  bar integer not null,
  baz varchar(50) not null,
  primary key (foo)
);`, name))
		return err
	}

	dsn := fmt.Sprintf("postgres://testuser:testpass@localhost:%v/testdb?sslmode=disable", resource.GetPort("5432/tcp"))
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
		if err := createTable("footable"); err != nil {
			return err
		}
		return nil
	}))

	testSuite(t, "postgres", dsn, createTable)
}

func mySQLIntegration(t *testing.T) {
	if runtime.GOOS == "darwin" {
		t.Skip("skipping test on macos")
	}

	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = 30 * time.Second

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "mysql",
		ExposedPorts: []string{"3306/tcp"},
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
			t.Logf("Failed to clean up docker resource: %v", err)
		}
		if db != nil {
			db.Close()
		}
	})

	createTable := func(name string) error {
		_, err := db.Exec(fmt.Sprintf(`create table %v (
  foo varchar(50) not null,
  bar integer not null,
  baz varchar(50) not null,
  primary key (foo)
		);`, name))
		return err
	}

	dsn := fmt.Sprintf("testuser:testpass@tcp(localhost:%v)/testdb", resource.GetPort("3306/tcp"))
	require.NoError(t, pool.Retry(func() error {
		if db, err = sql.Open("mysql", dsn); err != nil {
			return err
		}
		if err = db.Ping(); err != nil {
			db.Close()
			db = nil
			return err
		}
		if err := createTable("footable"); err != nil {
			return err
		}
		return nil
	}))

	testSuite(t, "mysql", dsn, createTable)
}

func msSQLIntegration(t *testing.T) {
	if runtime.GOOS == "darwin" {
		t.Skip("skipping test on macos")
	}

	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = 30 * time.Second

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
			t.Logf("Failed to clean up docker resource: %v", err)
		}
		if db != nil {
			db.Close()
		}
	})

	createTable := func(name string) error {
		_, err := db.Exec(fmt.Sprintf(`create table %v (
  foo varchar(50) not null,
  bar integer not null,
  baz varchar(50) not null,
  primary key (foo)
		);`, name))
		return err
	}

	dsn := fmt.Sprintf("sqlserver://sa:"+testPassword+"@localhost:%v?database=master", resource.GetPort("1433/tcp"))
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
		if err := createTable("footable"); err != nil {
			return err
		}
		return nil
	}))

	testSuite(t, "mssql", dsn, createTable)
}
